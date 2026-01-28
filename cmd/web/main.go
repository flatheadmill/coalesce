package main

import (
	"bufio"
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
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/gorilla/websocket"
	_ "github.com/lib/pq"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

var (
	clientset *kubernetes.Clientset
	db        *sql.DB
)

//go:embed migrations/*.sql
var migrations embed.FS

type LogMessage struct {
	Kind     string `json:"kind"`               // "line", "error", "status", "exit"
	Text     string `json:"text,omitempty"`     // For line and error
	Phase    string `json:"phase,omitempty"`    // For status
	ExitCode int    `json:"exitCode,omitempty"` // For exit
	Reason   string `json:"reason,omitempty"`   // For exit and errors
}

type SSEWriter struct {
	w       http.ResponseWriter
	flusher http.Flusher
}

func NewSSEWriter(w http.ResponseWriter) (*SSEWriter, error) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return nil, fmt.Errorf("streaming not supported")
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	return &SSEWriter{w: w, flusher: flusher}, nil
}

func (sse *SSEWriter) Write(data string) {
	fmt.Fprintf(sse.w, "data: %s\n\n", strings.ReplaceAll(data, "\n", "\ndata: "))
	sse.flusher.Flush()
}

func (sse *SSEWriter) WriteJSON(msg LogMessage) {
	data, _ := json.Marshal(msg)
	sse.Write(string(data))
}

var upgrader = websocket.Upgrader{}

func handleEvents(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
  slug := strings.TrimPrefix(r.URL.Path, "/events/")
  conn.WriteJSON(LogMessage{Kind: "status", Phase: "connected", Reason: slug})
}

func handleLogs(w http.ResponseWriter, r *http.Request) {
	slog.Info("handleLogs called", "path", r.URL.Path, "method", r.Method)

	// Extract pod and container from URL: /logs/{namespace}/{pod}/{container}
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 5 {
		slog.Warn("Invalid path format", "path", r.URL.Path, "parts", len(parts))
		http.Error(w, "Invalid path format. Expected /logs/{namespace}/{pod}/{container}", http.StatusBadRequest)
		return
	}

	namespace := parts[2]
	podName := parts[3]
	containerName := parts[4]
	follow := r.URL.Query().Get("follow") != "false" // Default to true

	slog.Info("Log stream requested", "namespace", namespace, "pod", podName, "container", containerName, "follow", follow)

	// Get pod to verify it exists first
	pod, err := clientset.CoreV1().Pods(namespace).Get(r.Context(), podName, metav1.GetOptions{})
	if err != nil {
		slog.Error("Failed to get pod", "namespace", namespace, "pod", podName, "error", err)
		http.Error(w, fmt.Sprintf("Pod not found: %v", err), http.StatusNotFound)
		return
	}

	// Pod exists, create SSE writer
	sse, err := NewSSEWriter(w)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Wait for pod to be ready using watch
	if pod.Status.Phase == corev1.PodPending {
		watcher, err := clientset.CoreV1().Pods(namespace).Watch(r.Context(), metav1.ListOptions{
			FieldSelector: fmt.Sprintf("metadata.name=%s", podName),
		})
		if err != nil {
			slog.Error("Failed to watch pod", "namespace", namespace, "pod", podName, "error", err)
			sse.WriteJSON(LogMessage{
				Kind: "error",
				Text: "Failed to watch pod status",
			})
			return
		}
		defer watcher.Stop()

		sse.WriteJSON(LogMessage{
			Kind:  "status",
			Phase: "Pending",
		})
		for event := range watcher.ResultChan() {
			pod = event.Object.(*corev1.Pod)
			if pod.Status.Phase != corev1.PodPending {
				break
			}
		}
	}

	// Set up log options
	podLogOpts := &corev1.PodLogOptions{
		Follow:    follow,
		Container: containerName,
	}

	// Get log stream
	req := clientset.CoreV1().Pods(namespace).GetLogs(podName, podLogOpts)
	stream, err := req.Stream(r.Context())
	if err != nil {
		slog.Error("Failed to open log stream", "namespace", namespace, "pod", podName, "container", containerName, "error", err)
		sse.WriteJSON(LogMessage{
			Kind: "error",
			Text: "Failed to open log stream",
		})
		return
	}
	defer stream.Close()

	// Stream logs line by line
	scanner := bufio.NewScanner(stream)
	for scanner.Scan() {
		sse.WriteJSON(LogMessage{
			Kind: "line",
			Text: scanner.Text(),
		})
	}

	if err := scanner.Err(); err != nil && err != io.EOF {
		slog.Error("Scanner error", "namespace", namespace, "pod", podName, "container", containerName, "error", err)
		sse.WriteJSON(LogMessage{
			Kind: "error",
			Text: "Stream interrupted",
		})
	}

	// Get final pod status for exit code
	finalPod, err := clientset.CoreV1().Pods(namespace).Get(r.Context(), podName, metav1.GetOptions{})
	if err != nil {
		// Pod might be deleted already
		slog.Warn("Failed to get final pod status", "namespace", namespace, "pod", podName, "error", err)
		sse.WriteJSON(LogMessage{
			Kind: "error",
			Text: "Pod no longer exists",
		})
		return
	}

	// Find container status and send exit code
	for _, cs := range finalPod.Status.ContainerStatuses {
		if cs.Name == containerName {
			if cs.State.Terminated != nil {
				sse.WriteJSON(LogMessage{
					Kind:     "exit",
					ExitCode: int(cs.State.Terminated.ExitCode),
					Reason:   cs.State.Terminated.Reason,
				})
			}
			break
		}
	}
}

func handleDAG(w http.ResponseWriter, r *http.Request) {
	// For now, serve the static HTML
	// Later this can be dynamic based on job ID
	http.ServeFile(w, r, "run.html")
}

func handleGraph(w http.ResponseWriter, r *http.Request) {
	// Serve the graph.json file
	// Later this would be generated from the actual job
	w.Header().Set("Content-Type", "application/json")
	http.ServeFile(w, r, "graph.json")
}

func handleRun(w http.ResponseWriter, r *http.Request) {
	// Only accept POST
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract namespace and pipeline from URL: /run/{namespace}/{pipeline}
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 4 {
		http.Error(w, "Invalid path format. Expected /run/{namespace}/{pipeline}", http.StatusBadRequest)
		return
	}

	namespace := parts[2]
	pipelineName := parts[3]

	// Read POST body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error reading body: %v", err), http.StatusBadRequest)
		return
	}

	// For now, read pipeline from ConfigMap (later: Coalesce CRD)
	cm, err := clientset.CoreV1().ConfigMaps(namespace).Get(r.Context(), pipelineName, metav1.GetOptions{})
	if err != nil {
		http.Error(w, fmt.Sprintf("Pipeline not found: %v", err), http.StatusNotFound)
		return
	}

	source, ok := cm.Data["source"]
	if !ok {
		http.Error(w, "Pipeline source not found in ConfigMap", http.StatusBadRequest)
		return
	}

	// Generate unique job name
	jobName := fmt.Sprintf("%s-%d", pipelineName, time.Now().Unix())

	// Create Job
	ttl := int32(3600) // 1 hour TTL
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: namespace,
			Labels: map[string]string{
				"coalesce.io/pipeline": pipelineName,
			},
		},
		Spec: batchv1.JobSpec{
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy:         corev1.RestartPolicyNever,
					ShareProcessNamespace: &[]bool{true}[0],
					Containers: []corev1.Container{
						{
							Name:    "coalesce",
							Image:   "harbor.example.org/example/coalesce:latest",
							Command: []string{"/bin/sh", "-c"},
							Args:    []string{fmt.Sprintf("echo '%s' | bin/coalesce -", source)},
							Env: []corev1.EnvVar{
								{
									Name:  "POST_BODY",
									Value: string(body),
								},
								{
									Name:  "PIPELINE_NAME",
									Value: pipelineName,
								},
								{
									Name:  "JOB_NAME",
									Value: jobName,
								},
							},
						},
					},
				},
			},
		},
	}

	// Create the job
	createdJob, err := clientset.BatchV1().Jobs(namespace).Create(r.Context(), job, metav1.CreateOptions{})
	if err != nil {
		http.Error(w, fmt.Sprintf("Error creating job: %v", err), http.StatusInternalServerError)
		return
	}

	// Return success with job info
	w.Header().Set("Content-Type", "application/json")
	response := map[string]string{
		"job":       createdJob.Name,
		"namespace": namespace,
		"pipeline":  pipelineName,
		"logs_url":  fmt.Sprintf("/logs/%s/%s", namespace, createdJob.Name),
		"dag_url":   fmt.Sprintf("/dag/%s/%s", namespace, createdJob.Name),
	}
	json.NewEncoder(w).Encode(response)
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

	// Skip migrations for now - just testing mTLS
	slog.Info("Skipping migrations for mTLS testing")
	// if err := runMigrations(dbURL); err != nil {
	// 	log.Fatalf("Failed to run migrations: %v", err)
	// }

	// Try in-cluster config first, fall back to kubeconfig
	config, err := rest.InClusterConfig()
	if err != nil {
		// Not in cluster, try kubeconfig
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
	http.HandleFunc("/logs/", handleLogs)
	http.HandleFunc("/dag/", handleDAG)
	http.HandleFunc("/graph/", handleGraph)
	http.HandleFunc("/run/", handleRun)
	http.HandleFunc("/health", handleHealth)
	http.HandleFunc("/events/", handleEvents)

	// Serve static files from current directory
	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("."))))

	addr := fmt.Sprintf(":%d", port)
	slog.Info("Server starting", "addr", addr)

	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

func buildDatabaseURL() string {
	// Check if full DATABASE_URL is provided
	if url := os.Getenv("DATABASE_URL"); url != "" {
		return url
	}

	// Build from individual components
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

	// Build connection string
	url := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s",
		user, password, host, port, dbname, sslmode)

	// Add SSL cert parameters if provided
	sslcert := os.Getenv("DB_SSLCERT")
	sslkey := os.Getenv("DB_SSLKEY")
	sslrootcert := os.Getenv("DB_SSLROOTCERT")

	if sslcert != "" {
		url += "&sslcert=" + sslcert
		// Check if file exists
		if _, err := os.Stat(sslcert); err != nil {
			slog.Warn("SSL cert file not accessible", "path", sslcert, "error", err)
		}
	}
	if sslkey != "" {
		url += "&sslkey=" + sslkey
		// Check if file exists
		if _, err := os.Stat(sslkey); err != nil {
			slog.Warn("SSL key file not accessible", "path", sslkey, "error", err)
		}
	}
	if sslrootcert != "" {
		url += "&sslrootcert=" + sslrootcert
		// Check if file exists
		if _, err := os.Stat(sslrootcert); err != nil {
			slog.Warn("SSL root cert file not accessible", "path", sslrootcert, "error", err)
		}
	}

	slog.Info("Database connection configured",
		"host", host,
		"port", port,
		"user", user,
		"database", dbname,
		"sslmode", sslmode,
		"sslcert", sslcert,
		"sslkey", sslkey,
		"sslrootcert", sslrootcert)

	return url
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

// Database helper functions

func createRun(slug, pipeline string) error {
	_, err := db.Exec(`
		INSERT INTO runs (slug, pipeline)
		VALUES ($1, $2)
	`, slug, pipeline)
	return err
}

func createJob(slug, job, k8sName string) error {
	_, err := db.Exec(`
		INSERT INTO jobs (slug, job, k8s_name)
		VALUES ($1, $2, $3)
	`, slug, job, k8sName)
	return err
}

func updateJobStatus(slug, job, status string, exitCode *int) error {
	if exitCode != nil {
		_, err := db.Exec(`
			UPDATE jobs
			SET status = $1, exit_code = $2, completed_at = now()
			WHERE slug = $3 AND job = $4
		`, status, *exitCode, slug, job)
		return err
	}
	_, err := db.Exec(`
		UPDATE jobs
		SET status = $1, completed_at = now()
		WHERE slug = $2 AND job = $3
	`, status, slug, job)
	return err
}

func saveLogs(slug, job, content string) error {
	_, err := db.Exec(`
		INSERT INTO logs (slug, job, content)
		VALUES ($1, $2, $3)
		ON CONFLICT (slug, job)
		DO UPDATE SET content = $3
	`, slug, job, content)
	return err
}

func testDatabaseConnection() error {
	var version string
	var user string
	var ssl string

	err := db.QueryRow("SELECT version()").Scan(&version)
	if err != nil {
		return fmt.Errorf("failed to query version: %w", err)
	}

	err = db.QueryRow("SELECT current_user").Scan(&user)
	if err != nil {
		return fmt.Errorf("failed to query current user: %w", err)
	}

	err = db.QueryRow("SELECT current_setting('ssl_cert_file', true)").Scan(&ssl)
	if err != nil {
		// Not critical if this fails
		ssl = "unknown"
	}

	slog.Info("Database connection successful",
		"version", version,
		"current_user", user,
		"ssl_cert", ssl)

	return nil
}
