// The Go server is a cubbyhole for the executor's state — persistence for
// resumption, not orchestration source of truth. The Zsh executor owns the
// logic: it builds DAGs, creates Kubernetes Jobs, watches completion, and
// propagates state. This server observes and records: metadata to PostgreSQL,
// logs to cloud buckets via Stow, events to the UI via WebSocket.
//
// The executor doesn't need this server to run a pipeline. This server
// doesn't orchestrate. Clean separation.
package main

import (
	"database/sql"
	"embed"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/gorilla/websocket"
	"github.com/graymeta/stow"
	"github.com/graymeta/stow/local"
	_ "github.com/lib/pq"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

var (
	clientset     *kubernetes.Clientset
	db            *sql.DB
	stowLocation  stow.Location
	stowContainer stow.Container
)

//go:embed migrations/*.sql
var migrations embed.FS

// WebSocket event types
type Event struct {
	Kind string      `json:"kind"` // "job_started", "job_completed", "job_failed", "log_line", "dag_updated"
	Data interface{} `json:"data"`
}

// Event hub for WebSocket broadcasting
type EventHub struct {
	mu          sync.RWMutex
	subscribers map[string]map[chan Event]bool // namespace/slug -> channels
}

var hub = &EventHub{
	subscribers: make(map[string]map[chan Event]bool),
}

func (h *EventHub) Subscribe(namespace, slug string) chan Event {
	h.mu.Lock()
	defer h.mu.Unlock()

	key := namespace + "/" + slug
	if h.subscribers[key] == nil {
		h.subscribers[key] = make(map[chan Event]bool)
	}

	ch := make(chan Event, 16)
	h.subscribers[key][ch] = true
	return ch
}

func (h *EventHub) Unsubscribe(namespace, slug string, ch chan Event) {
	h.mu.Lock()
	defer h.mu.Unlock()

	key := namespace + "/" + slug
	if h.subscribers[key] != nil {
		delete(h.subscribers[key], ch)
		close(ch)
	}
}

func (h *EventHub) Broadcast(namespace, slug string, event Event) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	key := namespace + "/" + slug
	for ch := range h.subscribers[key] {
		select {
		case ch <- event:
		default:
			// Drop if channel is full
		}
	}
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// API request/response types

type DAGRequest struct {
	DAG json.RawMessage `json:"dag"`
}

type RunRequest struct {
	Pipeline string `json:"pipeline"`
}

type RunUpdateRequest struct {
	Status string `json:"status"`
}

type JobStartRequest struct {
	StartedAt *time.Time `json:"started_at,omitempty"`
}

type JobUpdateRequest struct {
	Status   string `json:"status"`
	ExitCode *int   `json:"exit_code,omitempty"`
}

type ContainerLogRequest struct {
	StartedAt time.Time `json:"started_at"`
}

// Executor-facing handlers

func handlePostDAG(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("namespace")
	slug := r.PathValue("slug")

	var req DAGRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	// Versioned DAGs: atomically compare to the latest version and only
	// insert if different. The CTE grabs the latest, the INSERT fires only
	// when no match exists, and RETURNING tells us whether a new version
	// was created. One round trip, no race condition. Handles the data
	// science case where someone adds steps mid-run to a $7,000 pipeline.
	var createdAt time.Time
	err := db.QueryRow(`
		WITH latest AS (
			SELECT dag FROM dags
			WHERE namespace = $1 AND slug = $2
			ORDER BY created_at DESC LIMIT 1
		)
		INSERT INTO dags (namespace, slug, dag)
		SELECT $1, $2, $3::jsonb
		WHERE NOT EXISTS (SELECT 1 FROM latest WHERE dag = $3::jsonb)
		RETURNING created_at
	`, namespace, slug, req.DAG).Scan(&createdAt)

	switch {
	case err == sql.ErrNoRows:
		// DAG matches latest version, nothing to do
	case err != nil:
		slog.Error("Failed to insert DAG", "error", err)
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	default:
		slog.Info("DAG version created", "namespace", namespace, "slug", slug)
		hub.Broadcast(namespace, slug, Event{Kind: "dag_updated", Data: map[string]string{
			"namespace": namespace,
			"slug":      slug,
		}})
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func handlePostRun(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("namespace")
	slug := r.PathValue("slug")

	var req RunRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	// ON CONFLICT handles the resume case: a $12,000 data science pipeline
	// fails, you fix the bucket name and re-run with the same slug. We
	// preserve started_at so SOC 2 evidence shows the original start time,
	// not the resumption time. Only status, completed_at, and pipeline are
	// updated. For CI/CD, use unique slugs per invocation and this path
	// never fires.
	_, err := db.Exec(`
		INSERT INTO runs (namespace, slug, pipeline)
		VALUES ($1, $2, $3)
		ON CONFLICT (namespace, slug) DO UPDATE SET
			completed_at = NULL,
			status = 'running',
			pipeline = $3
	`, namespace, slug, req.Pipeline)

	if err != nil {
		slog.Error("Failed to create run", "error", err)
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}

	slog.Info("Run created", "namespace", namespace, "slug", slug, "pipeline", req.Pipeline)
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"status": "created"})
}

// handlePutRun marks a pipeline run as done. This is the SOC 2 money shot:
// "the scan ran and completed." Without this, the server can record individual
// jobs but can't say the pipeline finished.
func handlePutRun(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("namespace")
	slug := r.PathValue("slug")

	var req RunUpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if req.Status != "completed" && req.Status != "failed" && req.Status != "cancelled" {
		http.Error(w, "Status must be completed, failed, or cancelled", http.StatusBadRequest)
		return
	}

	// RETURNING gives us the actual completed_at timestamp and confirms
	// the row exists in one step, no RowsAffected check needed.
	var completedAt time.Time
	err := db.QueryRow(`
		UPDATE runs
		SET status = $1, completed_at = now()
		WHERE namespace = $2 AND slug = $3
		RETURNING completed_at
	`, req.Status, namespace, slug).Scan(&completedAt)

	if err == sql.ErrNoRows {
		http.Error(w, "Run not found", http.StatusNotFound)
		return
	} else if err != nil {
		slog.Error("Failed to update run", "error", err)
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}

	hub.Broadcast(namespace, slug, Event{Kind: "run_" + req.Status, Data: map[string]string{
		"namespace": namespace,
		"slug":      slug,
		"status":    req.Status,
	}})

	slog.Info("Run completed", "namespace", namespace, "slug", slug, "status", req.Status)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "updated"})
}

func handlePostJob(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("namespace")
	slug := r.PathValue("slug")
	job := r.PathValue("job")

	var req JobStartRequest
	json.NewDecoder(r.Body).Decode(&req) // Optional body

	// COALESCE lets PostgreSQL handle the default. If the executor doesn't
	// provide started_at, now() fills in. No Go-side defaulting.
	_, err := db.Exec(`
		INSERT INTO jobs (namespace, slug, job, started_at, status)
		VALUES ($1, $2, $3, COALESCE($4, now()), 'running')
	`, namespace, slug, job, req.StartedAt)

	if err != nil {
		slog.Error("Failed to create job", "error", err)
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}

	hub.Broadcast(namespace, slug, Event{Kind: "job_started", Data: map[string]string{
		"job": job,
	}})

	slog.Info("Job started", "namespace", namespace, "slug", slug, "job", job)
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"status": "created"})
}

func handlePutJob(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("namespace")
	slug := r.PathValue("slug")
	job := r.PathValue("job")

	var req JobUpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	// Go's *int passes as SQL NULL when nil, so one query handles both
	// cases — exit_code provided or not. No branching needed.
	_, err := db.Exec(`
		UPDATE jobs
		SET status = $1, exit_code = $2, completed_at = now()
		WHERE namespace = $3 AND slug = $4 AND job = $5
		AND completed_at IS NULL
	`, req.Status, req.ExitCode, namespace, slug, job)

	if err != nil {
		slog.Error("Failed to update job", "error", err)
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}

	eventKind := "job_completed"
	if req.Status == "failed" {
		eventKind = "job_failed"
	}
	hub.Broadcast(namespace, slug, Event{Kind: eventKind, Data: map[string]interface{}{
		"job":       job,
		"status":    req.Status,
		"exit_code": req.ExitCode,
	}})

	slog.Info("Job updated", "namespace", namespace, "slug", slug, "job", job, "status", req.Status)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "updated"})
}

func handlePostContainer(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("namespace")
	slug := r.PathValue("slug")
	job := r.PathValue("job")
	container := r.PathValue("container")

	// Get started_at from query param or use now
	startedAtStr := r.URL.Query().Get("started_at")
	var startedAt time.Time
	if startedAtStr != "" {
		var err error
		startedAt, err = time.Parse(time.RFC3339, startedAtStr)
		if err != nil {
			http.Error(w, "Invalid started_at format", http.StatusBadRequest)
			return
		}
	} else {
		startedAt = time.Now()
	}

	// Read log content from body
	logContent, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}

	// Logs go to cloud buckets via Stow, not PostgreSQL. Stow abstracts
	// GCS/S3/local with the same API — swap the dial string and config to
	// switch backends. The database just stores the path.
	logPath := fmt.Sprintf("%s/%s/%s/%s/%s.log",
		namespace, slug, job, startedAt.Format("20060102-150405"), container)

	_, err = stowContainer.Put(logPath, strings.NewReader(string(logContent)), int64(len(logContent)), nil)
	if err != nil {
		slog.Error("Failed to store log", "error", err, "path", logPath)
		http.Error(w, "Storage error", http.StatusInternalServerError)
		return
	}

	// Record in database
	_, err = db.Exec(`
		INSERT INTO containers (namespace, slug, job, started_at, container, log_path)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (namespace, slug, job, started_at, container) DO UPDATE SET
			log_path = $6
	`, namespace, slug, job, startedAt, container, logPath)

	if err != nil {
		slog.Error("Failed to record container", "error", err)
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}

	slog.Info("Container log stored", "namespace", namespace, "slug", slug, "job", job, "container", container)
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"status": "created", "log_path": logPath})
}

// UI-facing handlers

func handleGetRuns(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("namespace")

	rows, err := db.Query(`
		SELECT slug, pipeline, started_at, completed_at, status
		FROM runs
		WHERE namespace = $1
		ORDER BY started_at DESC
		LIMIT 100
	`, namespace)
	if err != nil {
		slog.Error("Failed to query runs", "error", err)
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	type Run struct {
		Slug        string     `json:"slug"`
		Pipeline    string     `json:"pipeline"`
		StartedAt   time.Time  `json:"started_at"`
		CompletedAt *time.Time `json:"completed_at,omitempty"`
		Status      string     `json:"status"`
	}

	var runs []Run
	for rows.Next() {
		var run Run
		if err := rows.Scan(&run.Slug, &run.Pipeline, &run.StartedAt, &run.CompletedAt, &run.Status); err != nil {
			continue
		}
		runs = append(runs, run)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(runs)
}

func handleGetRun(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("namespace")
	slug := r.PathValue("slug")

	type Job struct {
		Job         string     `json:"job"`
		StartedAt   time.Time  `json:"started_at"`
		CompletedAt *time.Time `json:"completed_at,omitempty"`
		Status      string     `json:"status"`
		ExitCode    *int       `json:"exit_code,omitempty"`
	}

	type RunDetail struct {
		Slug        string     `json:"slug"`
		Pipeline    string     `json:"pipeline"`
		StartedAt   time.Time  `json:"started_at"`
		CompletedAt *time.Time `json:"completed_at,omitempty"`
		Status      string     `json:"status"`
		Jobs        []Job      `json:"jobs"`
	}

	var run RunDetail
	err := db.QueryRow(`
		SELECT slug, pipeline, started_at, completed_at, status
		FROM runs
		WHERE namespace = $1 AND slug = $2
	`, namespace, slug).Scan(&run.Slug, &run.Pipeline, &run.StartedAt, &run.CompletedAt, &run.Status)

	if err == sql.ErrNoRows {
		http.Error(w, "Run not found", http.StatusNotFound)
		return
	} else if err != nil {
		slog.Error("Failed to query run", "error", err)
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}

	rows, err := db.Query(`
		SELECT job, started_at, completed_at, status, exit_code
		FROM jobs
		WHERE namespace = $1 AND slug = $2
		ORDER BY started_at
	`, namespace, slug)
	if err != nil {
		slog.Error("Failed to query jobs", "error", err)
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var job Job
		if err := rows.Scan(&job.Job, &job.StartedAt, &job.CompletedAt, &job.Status, &job.ExitCode); err != nil {
			continue
		}
		run.Jobs = append(run.Jobs, job)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(run)
}

func handleGetDAG(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("namespace")
	slug := r.PathValue("slug")

	var dag []byte
	var createdAt time.Time
	err := db.QueryRow(`
		SELECT dag, created_at FROM dags
		WHERE namespace = $1 AND slug = $2
		ORDER BY created_at DESC LIMIT 1
	`, namespace, slug).Scan(&dag, &createdAt)

	if err == sql.ErrNoRows {
		http.Error(w, "DAG not found", http.StatusNotFound)
		return
	} else if err != nil {
		slog.Error("Failed to query DAG", "error", err)
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"dag":        json.RawMessage(dag),
		"created_at": createdAt,
	})
}

func handleGetLogs(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("namespace")
	slug := r.PathValue("slug")
	job := r.PathValue("job")
	container := r.PathValue("container")

	// Find the log path
	var logPath string
	err := db.QueryRow(`
		SELECT log_path FROM containers
		WHERE namespace = $1 AND slug = $2 AND job = $3 AND container = $4
		ORDER BY started_at DESC LIMIT 1
	`, namespace, slug, job, container).Scan(&logPath)

	if err == sql.ErrNoRows {
		http.Error(w, "Log not found", http.StatusNotFound)
		return
	} else if err != nil {
		slog.Error("Failed to query log path", "error", err)
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}

	// Retrieve from Stow
	item, err := stowContainer.Item(logPath)
	if err != nil {
		slog.Error("Failed to get log item", "error", err, "path", logPath)
		http.Error(w, "Storage error", http.StatusInternalServerError)
		return
	}

	reader, err := item.Open()
	if err != nil {
		slog.Error("Failed to open log", "error", err)
		http.Error(w, "Storage error", http.StatusInternalServerError)
		return
	}
	defer reader.Close()

	w.Header().Set("Content-Type", "text/plain")
	io.Copy(w, reader)
}

func handleEvents(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("namespace")
	slug := r.PathValue("slug")

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("WebSocket upgrade failed", "error", err)
		return
	}
	defer conn.Close()

	ch := hub.Subscribe(namespace, slug)
	defer hub.Unsubscribe(namespace, slug, ch)

	// Send connected event
	conn.WriteJSON(Event{Kind: "connected", Data: map[string]string{
		"namespace": namespace,
		"slug":      slug,
	}})

	// Read events and forward to WebSocket
	for event := range ch {
		if err := conn.WriteJSON(event); err != nil {
			break
		}
	}
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
}

func main() {
	var kubeconfig *string
	var port int

	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.IntVar(&port, "port", 8080, "port to serve on")
	flag.Parse()

	// Build database connection string from environment variables
	dbURL := buildDatabaseURL()

	// Initialize database connection
	var err error
	db, err = sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Test database connection
	if err := testDatabaseConnection(); err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	// Run migrations
	if err := runMigrations(dbURL); err != nil {
		log.Fatalf("Failed to run migrations: %v", err)
	}

	// Initialize Stow for log storage
	if err := initStow(); err != nil {
		log.Fatalf("Failed to initialize storage: %v", err)
	}

	// Try in-cluster config first, fall back to kubeconfig
	config, err := rest.InClusterConfig()
	if err != nil {
		slog.Info("Not running in cluster, using kubeconfig", "path", *kubeconfig)
		config, err = clientcmd.BuildConfigFromFlags("", *kubeconfig)
		if err != nil {
			log.Fatalf("Error building config: %v", err)
		}
	} else {
		slog.Info("Using in-cluster configuration")
	}

	// Create clientset
	clientset, err = kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("Error creating client: %v", err)
	}

	// Set up routes
	mux := http.NewServeMux()

	// URL structure follows Kubernetes conventions: namespace scoping,
	// resource-oriented paths. The (namespace, slug) pair is the identity
	// model — it serves data science (resume with same slug) and CI/CD
	// (unique slug per invocation) without mode switching.
	//
	// Executor-facing: the Zsh executor curls these to record state.
	// UI-facing: the browser fetches these to display it.
	mux.HandleFunc("POST /api/{namespace}/dags/{slug}", handlePostDAG)
	mux.HandleFunc("POST /api/{namespace}/runs/{slug}", handlePostRun)
	mux.HandleFunc("PUT /api/{namespace}/runs/{slug}", handlePutRun)
	mux.HandleFunc("POST /api/{namespace}/jobs/{slug}/{job}", handlePostJob)
	mux.HandleFunc("PUT /api/{namespace}/jobs/{slug}/{job}", handlePutJob)
	mux.HandleFunc("POST /api/{namespace}/containers/{slug}/{job}/{container}", handlePostContainer)

	// UI-facing endpoints
	mux.HandleFunc("GET /api/{namespace}/runs", handleGetRuns)
	mux.HandleFunc("GET /api/{namespace}/runs/{slug}", handleGetRun)
	mux.HandleFunc("GET /api/{namespace}/dags/{slug}", handleGetDAG)
	mux.HandleFunc("GET /api/{namespace}/logs/{slug}/{job}/{container}", handleGetLogs)

	// WebSocket
	mux.HandleFunc("/events/{namespace}/{slug}", handleEvents)

	// Health
	mux.HandleFunc("GET /health", handleHealth)

	// Serve static files
	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("."))))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" || r.URL.Path == "/index.html" {
			http.ServeFile(w, r, "run.html")
		} else {
			http.NotFound(w, r)
		}
	})

	addr := fmt.Sprintf(":%d", port)
	slog.Info("Server starting", "addr", addr)

	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

func buildDatabaseURL() string {
	if url := os.Getenv("DATABASE_URL"); url != "" {
		return url
	}

	host := os.Getenv("DB_HOST")
	if host == "" {
		host = "localhost"
	}

	port := os.Getenv("DB_PORT")
	if port == "" {
		port = "5432"
	}

	user := os.Getenv("DB_USER")
	if user == "" {
		user = "postgres"
	}

	password := os.Getenv("DB_PASSWORD")
	if password == "" {
		password = "postgres"
	}

	dbname := os.Getenv("DB_NAME")
	if dbname == "" {
		dbname = "coalesce"
	}

	sslmode := os.Getenv("DB_SSLMODE")
	if sslmode == "" {
		sslmode = "disable"
	}

	url := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s",
		user, password, host, port, dbname, sslmode)

	sslcert := os.Getenv("DB_SSLCERT")
	sslkey := os.Getenv("DB_SSLKEY")
	sslrootcert := os.Getenv("DB_SSLROOTCERT")

	if sslcert != "" {
		url += "&sslcert=" + sslcert
	}
	if sslkey != "" {
		url += "&sslkey=" + sslkey
	}
	if sslrootcert != "" {
		url += "&sslrootcert=" + sslrootcert
	}

	slog.Info("Database connection configured",
		"host", host,
		"port", port,
		"user", user,
		"database", dbname,
		"sslmode", sslmode)

	return url
}

func initStow() error {
	kind := os.Getenv("STOW_KIND")
	if kind == "" {
		kind = "local"
	}

	var config stow.ConfigMap
	var err error

	switch kind {
	case "local":
		path := os.Getenv("STOW_PATH")
		if path == "" {
			path = "/tmp/coalesce-logs"
		}
		if err := os.MkdirAll(path, 0755); err != nil {
			return fmt.Errorf("failed to create log directory: %w", err)
		}
		config = stow.ConfigMap{
			local.ConfigKeyPath: path,
		}
	case "google":
		config = stow.ConfigMap{
			"json": os.Getenv("STOW_GOOGLE_JSON"),
		}
	default:
		return fmt.Errorf("unsupported stow kind: %s", kind)
	}

	stowLocation, err = stow.Dial(kind, config)
	if err != nil {
		return fmt.Errorf("failed to dial stow: %w", err)
	}

	bucket := os.Getenv("STOW_BUCKET")
	if bucket == "" {
		bucket = "logs"
	}

	stowContainer, err = stowLocation.Container(bucket)
	if err != nil {
		// Try to create it for local backend
		if kind == "local" {
			stowContainer, err = stowLocation.CreateContainer(bucket)
		}
		if err != nil {
			return fmt.Errorf("failed to get container: %w", err)
		}
	}

	slog.Info("Storage initialized", "kind", kind, "bucket", bucket)
	return nil
}

func runMigrations(dbURL string) error {
	slog.Info("Running database migrations")

	source, err := iofs.New(migrations, "migrations")
	if err != nil {
		return fmt.Errorf("failed to create migration source: %w", err)
	}

	m, err := migrate.NewWithSourceInstance("iofs", source, dbURL)
	if err != nil {
		return fmt.Errorf("failed to create migrate instance: %w", err)
	}

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("failed to run migrations: %w", err)
	}

	slog.Info("Database migrations completed")
	return nil
}

func testDatabaseConnection() error {
	var version string
	err := db.QueryRow("SELECT version()").Scan(&version)
	if err != nil {
		return fmt.Errorf("failed to query version: %w", err)
	}
	slog.Info("Database connection successful", "version", version)
	return nil
}
