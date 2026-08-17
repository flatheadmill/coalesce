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
	"bufio"
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/gorilla/websocket"
	"github.com/graymeta/stow"
	"github.com/graymeta/stow/google"
	"github.com/graymeta/stow/local"
	_ "github.com/lib/pq"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

var (
	clientset     *kubernetes.Clientset
	db            *sql.DB
	stowLocation  stow.Location
	stowContainer stow.Container
	stowPrefix    string
)

//go:embed migrations/*.sql
var migrations embed.FS

// The UI rides in the binary — built output embedded and versioned with the
// server, per the settled asset contract. Built by the Dockerfile's node
// stage into cmd/web/dist; a bare `go build ./cmd/web` needs
// `(cd ui && npm run build)` first.
//
//go:embed all:dist
var dist embed.FS

// WebSocket event types
type Event struct {
	Kind string      `json:"kind"` // "job_started", "job_completed", "job_failed", "dag_updated"
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

	ch := make(chan Event, 4096)
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

	if req.Status != "completed" && req.Status != "failed" &&
		req.Status != "cancelled" && req.Status != "interrupted" {
		http.Error(w, "Status must be completed, failed, cancelled, or interrupted", http.StatusBadRequest)
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

// storeContainerLog writes log content to Stow and records the path in
// PostgreSQL. The transaction wraps both writes so they're atomic from the
// database's perspective — if Stow fails, the DB record rolls back and we
// don't have a row pointing at nothing. ON CONFLICT makes this idempotent:
// the pod watcher and the HTTP handler can both call it safely.
func storeContainerLog(namespace, slug, job string, startedAt time.Time, container string, logContent []byte) (string, error) {
	// STOW_PREFIX is an opaque string prepended to every stored path — no
	// semantics, empty by default. It exists so deployments sharing one
	// bucket can partition it however they partition things; the server
	// knows nothing about what the value means. The prefix is baked into
	// the path at write time and rides in the containers row, so retrieval
	// reads the stored path unchanged and rows written before a prefix
	// existed keep working.
	logPath := fmt.Sprintf("%s%s/%s/%s/%s/%s.log", stowPrefix,
		namespace, slug, job, startedAt.Format("20060102-150405"), container)

	tx, err := db.Begin()
	if err != nil {
		return "", fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.Exec(`
		INSERT INTO containers (namespace, slug, job, started_at, container, log_path)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (namespace, slug, job, started_at, container) DO UPDATE SET
			log_path = $6
	`, namespace, slug, job, startedAt, container, logPath)
	if err != nil {
		return "", fmt.Errorf("insert container: %w", err)
	}

	_, err = stowContainer.Put(logPath, strings.NewReader(string(logContent)), int64(len(logContent)), nil)
	if err != nil {
		return "", fmt.Errorf("store log: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("commit: %w", err)
	}

	return logPath, nil
}

func handlePostContainer(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("namespace")
	slug := r.PathValue("slug")
	job := r.PathValue("job")
	container := r.PathValue("container")

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

	logContent, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}

	logPath, err := storeContainerLog(namespace, slug, job, startedAt, container, logContent)
	if err != nil {
		slog.Error("Failed to store container log", "error", err)
		http.Error(w, "Storage error", http.StatusInternalServerError)
		return
	}

	slog.Info("Container log stored", "namespace", namespace, "slug", slug, "job", job, "container", container)
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"status": "created", "log_path": logPath})
}

// harvestPodLogs fetches logs for every container in a terminated pod and
// stores them. The job's started_at comes from PostgreSQL because the
// containers table has a FK to jobs on (namespace, slug, job, started_at).
// If the job record doesn't exist yet (executor hasn't curled it), we skip
// — the watcher will see the pod again on the next list cycle.
func harvestPodLogs(ctx context.Context, pod *corev1.Pod) {
	namespace := pod.Namespace
	slug := pod.Labels["coalesce.flatheadmill.com/slug"]
	job := pod.Labels["coalesce.flatheadmill.com/job"]

	if slug == "" || job == "" {
		return
	}

	var startedAt time.Time
	err := db.QueryRow(`
		SELECT started_at FROM jobs
		WHERE namespace = $1 AND slug = $2 AND job = $3
		ORDER BY started_at DESC LIMIT 1
	`, namespace, slug, job).Scan(&startedAt)
	if err != nil {
		slog.Warn("Job record not found for log harvest, skipping",
			"namespace", namespace, "slug", slug, "job", job)
		return
	}

	for _, c := range pod.Spec.Containers {
		logStream, err := clientset.CoreV1().Pods(namespace).GetLogs(pod.Name, &corev1.PodLogOptions{
			Container: c.Name,
		}).Stream(ctx)
		if err != nil {
			slog.Warn("Failed to fetch container logs",
				"pod", pod.Name, "container", c.Name, "error", err)
			continue
		}

		logContent, err := io.ReadAll(logStream)
		logStream.Close()
		if err != nil {
			slog.Warn("Failed to read container logs",
				"pod", pod.Name, "container", c.Name, "error", err)
			continue
		}

		logPath, err := storeContainerLog(namespace, slug, job, startedAt, c.Name, logContent)
		if err != nil {
			slog.Error("Failed to store harvested log",
				"pod", pod.Name, "container", c.Name, "error", err)
			continue
		}

		slog.Info("Harvested container log",
			"pod", pod.Name, "container", c.Name, "path", logPath)
	}
}

// watchPodCompletions watches for pods with our label that reach a terminal
// state and harvests their logs. The watch can disconnect — the outer loop
// re-lists and re-watches. On each cycle it first sweeps terminated pods
// from the list response (catching anything that completed while the watch
// was down or before the server started), then watches for new completions.
//
// The SIGTERM contract means containers exit cleanly and their logs are
// available. The ttlSecondsAfterFinished gives us five minutes to harvest.
// If the node disappears first, that's the accepted 5% — we don't contort
// the architecture to cover it.
func watchPodCompletions(ctx context.Context, namespace string) {
	for {
		// List first to catch pods that terminated while we weren't watching.
		pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
			LabelSelector: "coalesce.flatheadmill.com/slug",
		})
		if err != nil {
			slog.Error("Failed to list pods for log harvest", "error", err)
			time.Sleep(10 * time.Second)
			continue
		}

		for i := range pods.Items {
			pod := &pods.Items[i]
			if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
				harvestPodLogs(ctx, pod)
			}
		}

		// Watch from the list's resource version so we don't miss anything
		// between the list and the watch.
		watcher, err := clientset.CoreV1().Pods(namespace).Watch(ctx, metav1.ListOptions{
			LabelSelector:   "coalesce.flatheadmill.com/slug",
			ResourceVersion: pods.ResourceVersion,
		})
		if err != nil {
			slog.Error("Failed to start pod watch for log harvest", "error", err)
			time.Sleep(10 * time.Second)
			continue
		}

		for event := range watcher.ResultChan() {
			// DELETE events carry the pod's last state, which is terminal.
			// The pod is already gone — nothing to harvest.
			if event.Type == watch.Deleted {
				continue
			}
			pod, ok := event.Object.(*corev1.Pod)
			if !ok {
				continue
			}
			if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
				harvestPodLogs(ctx, pod)
			}
		}

		// Watch disconnected. Loop back to list + watch.
		slog.Info("Pod watch disconnected, restarting")
	}
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

	// Single query with LEFT JOIN gives a consistent snapshot of the run
	// and its jobs. Two separate queries leave a gap where a job could
	// complete between them.
	rows, err := db.Query(`
		SELECT r.slug, r.pipeline, r.started_at, r.completed_at, r.status,
			j.job, j.started_at, j.completed_at, j.status, j.exit_code
		FROM runs r
		LEFT JOIN jobs j ON j.namespace = r.namespace AND j.slug = r.slug
		WHERE r.namespace = $1 AND r.slug = $2
		ORDER BY j.started_at
	`, namespace, slug)
	if err != nil {
		slog.Error("Failed to query run", "error", err)
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var run RunDetail
	var found bool

	for rows.Next() {
		var jobName *string
		var jobStartedAt *time.Time
		var jobCompletedAt *time.Time
		var jobStatus *string
		var jobExitCode *int

		if err := rows.Scan(
			&run.Slug, &run.Pipeline, &run.StartedAt, &run.CompletedAt, &run.Status,
			&jobName, &jobStartedAt, &jobCompletedAt, &jobStatus, &jobExitCode,
		); err != nil {
			continue
		}
		found = true

		if jobName != nil {
			run.Jobs = append(run.Jobs, Job{
				Job:         *jobName,
				StartedAt:   *jobStartedAt,
				CompletedAt: jobCompletedAt,
				Status:      *jobStatus,
				ExitCode:    jobExitCode,
			})
		}
	}

	if !found {
		http.Error(w, "Run not found", http.StatusNotFound)
		return
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

// A log tail is one Kubernetes stream written directly to one WebSocket. The
// read and write stay in the same loop so a full socket stops the Kubernetes
// reader instead of turning log content into lossy broadcast events.
func handleTailLogs(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("namespace")
	slug := r.PathValue("slug")
	job := r.PathValue("job")
	container := r.URL.Query().Get("container")

	// Find the pod by the labels the executor sets when creating Jobs.
	pods, err := clientset.CoreV1().Pods(namespace).List(r.Context(), metav1.ListOptions{
		LabelSelector: fmt.Sprintf("coalesce.flatheadmill.com/slug=%s,coalesce.flatheadmill.com/job=%s", slug, job),
	})
	if err != nil {
		slog.Error("Failed to list pods", "error", err, "namespace", namespace, "slug", slug, "job", job)
		http.Error(w, "Failed to find pod", http.StatusInternalServerError)
		return
	}

	if len(pods.Items) == 0 {
		http.Error(w, "No pod found for job", http.StatusNotFound)
		return
	}

	pod := &pods.Items[0]
	podName := pod.Name

	// Default to first container if not specified.
	if container == "" && len(pod.Spec.Containers) > 0 {
		container = pod.Spec.Containers[0].Name
	}
	if container == "" {
		http.Error(w, "Pod has no containers", http.StatusBadRequest)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("Log WebSocket upgrade failed", "error", err)
		return
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	go func() {
		for {
			if _, _, err := conn.NextReader(); err != nil {
				cancel()
				return
			}
		}
	}()

	slog.Info("Starting log tail", "namespace", namespace, "slug", slug, "job", job, "pod", podName, "container", container)
	if err := tailPodLogs(ctx, conn, namespace, job, podName, container); err != nil {
		if !errors.Is(err, context.Canceled) {
			slog.Error("Log tail failed", "error", err, "namespace", namespace, "slug", slug, "job", job, "pod", podName)
			_ = conn.WriteJSON(Event{Kind: "log_error", Data: map[string]string{
				"job":   job,
				"error": "Log stream failed",
			}})
			_ = conn.WriteControl(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseInternalServerErr, "log stream failed"),
				time.Now().Add(time.Second))
		}
		return
	}
	_ = conn.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		time.Now().Add(time.Second))
	slog.Info("Log tail completed", "namespace", namespace, "slug", slug, "job", job, "pod", podName)
}

func tailPodLogs(ctx context.Context, conn *websocket.Conn, namespace, job, podName, container string) error {
	// If the pod is still pending, watch until it transitions.
	pod, err := clientset.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get pod for tailing: %w", err)
	}

	if pod.Status.Phase == corev1.PodPending {
		if err := conn.WriteJSON(Event{Kind: "log_status", Data: map[string]string{
			"job":   job,
			"phase": "Pending",
		}}); err != nil {
			return err
		}

		watcher, err := clientset.CoreV1().Pods(namespace).Watch(ctx, metav1.ListOptions{
			FieldSelector: fmt.Sprintf("metadata.name=%s", podName),
		})
		if err != nil {
			return fmt.Errorf("watch pod: %w", err)
		}
		defer watcher.Stop()

		for pod.Status.Phase == corev1.PodPending {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case event, ok := <-watcher.ResultChan():
				if !ok {
					return errors.New("pod watch closed while pending")
				}
				updated, ok := event.Object.(*corev1.Pod)
				if ok {
					pod = updated
				}
			}
		}
	}

	if err := conn.WriteJSON(Event{Kind: "log_status", Data: map[string]string{
		"job":   job,
		"phase": string(pod.Status.Phase),
	}}); err != nil {
		return err
	}

	stream, err := clientset.CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{
		Follow:    true,
		Container: container,
	}).Stream(ctx)
	if err != nil {
		return fmt.Errorf("open log stream: %w", err)
	}
	defer stream.Close()

	if err := streamLogLines(stream, conn, job); err != nil {
		return fmt.Errorf("stream log lines: %w", err)
	}

	// The log stream can close just before the terminated state reaches the
	// API. Wait for that state rather than turning the ordinary race into a
	// failed tail.
	exitContext, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	terminated, err := waitForContainerTermination(exitContext, namespace, podName, container)
	if err != nil {
		return err
	}
	if err := conn.WriteJSON(Event{Kind: "log_exit", Data: map[string]interface{}{
		"job":       job,
		"exit_code": int(terminated.ExitCode),
		"reason":    terminated.Reason,
	}}); err != nil {
		return err
	}
	return nil
}

func waitForContainerTermination(ctx context.Context, namespace, podName, container string) (*corev1.ContainerStateTerminated, error) {
	pod, err := clientset.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get final pod state: %w", err)
	}
	if terminated := containerTermination(pod, container); terminated != nil {
		return terminated, nil
	}

	watcher, err := clientset.CoreV1().Pods(namespace).Watch(ctx, metav1.ListOptions{
		FieldSelector:   fmt.Sprintf("metadata.name=%s", podName),
		ResourceVersion: pod.ResourceVersion,
	})
	if err != nil {
		return nil, fmt.Errorf("watch final pod state: %w", err)
	}
	defer watcher.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case event, ok := <-watcher.ResultChan():
			if !ok {
				return nil, errors.New("pod watch closed before container terminated")
			}
			if event.Type == watch.Error {
				return nil, errors.New("pod watch failed before container terminated")
			}
			pod, ok := event.Object.(*corev1.Pod)
			if !ok {
				continue
			}
			if terminated := containerTermination(pod, container); terminated != nil {
				return terminated, nil
			}
		}
	}
}

func containerTermination(pod *corev1.Pod, container string) *corev1.ContainerStateTerminated {
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == container {
			return status.State.Terminated
		}
	}
	return nil
}

type eventWriter interface {
	WriteJSON(any) error
}

func streamLogLines(stream io.Reader, writer eventWriter, job string) error {
	scanner := bufio.NewScanner(stream)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		if err := writer.WriteJSON(Event{Kind: "log_line", Data: map[string]interface{}{
			"job":  job,
			"line": scanner.Text(),
		}}); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil && err != io.EOF {
		return err
	}
	return nil
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
}

func main() {
	var port int
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

	// The server is a pod in the cluster it serves. No kubeconfig flag, no
	// fallback. If this isn't running in-cluster, it shouldn't be running.
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("Not running in cluster: %v", err)
	}
	slog.Info("Using in-cluster configuration")

	clientset, err = kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("Error creating Kubernetes client: %v", err)
	}

	// Start the pod log harvester. It watches for terminated pods and
	// collects their logs into Stow before the node drains and the logs
	// vanish. The namespace is scoped to match the RBAC Role.
	watchNamespace := os.Getenv("WATCH_NAMESPACE")
	if watchNamespace == "" {
		watchNamespace = "default"
	}
	go watchPodCompletions(context.Background(), watchNamespace)
	slog.Info("Pod log harvester started", "namespace", watchNamespace)

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

	// Live log tailing has its own WebSocket so log bytes never enter the
	// lossy state-event hub.
	mux.HandleFunc("GET /tail/{namespace}/{slug}/{job}", handleTailLogs)

	// WebSocket
	mux.HandleFunc("/events/{namespace}/{slug}", handleEvents)

	// Health
	mux.HandleFunc("GET /health", handleHealth)

	// The old prototype stays reachable at /run.html until its role is fully
	// retired. It carries everything it needs from CDNs; the /static/
	// directory-server it once rode beside is gone — http.Dir(".") from /app
	// served the binary itself to anyone who asked.
	mux.HandleFunc("/run.html", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "run.html")
	})

	// The embedded app: real files by name, index.html for everything else,
	// so a direct refresh on a deep route like /coalesce/runs/hello-1 lands
	// in the router instead of a 404. A clean clone compiles because the
	// tracked dist/.gitkeep satisfies the all: pattern; a binary built that
	// way says so plainly instead of serving a bare 404.
	ui, uiErr := fs.Sub(dist, "dist")
	if uiErr != nil {
		log.Fatalf("Embedded UI missing: %v", uiErr)
	}
	_, uiBuiltErr := fs.Stat(ui, "index.html")
	uiBuilt := uiBuiltErr == nil
	if !uiBuilt {
		slog.Warn("UI not embedded in this binary; / serves a notice",
			"fix", "(cd ui && npm run build) and rebuild, or use docker build")
	}
	uiFiles := http.FileServer(http.FS(ui))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if !uiBuilt {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			fmt.Fprintln(w, "This binary was built without the UI. Run (cd ui && npm run build) and rebuild, or use docker build, which builds both.")
			return
		}
		path := strings.TrimPrefix(r.URL.Path, "/")
		if path != "" && path != "index.html" {
			if _, err := fs.Stat(ui, path); err == nil {
				uiFiles.ServeHTTP(w, r)
				return
			}
		}
		http.ServeFileFS(w, r, ui, "index.html")
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
		// Credentials arrive the way every secret in this house arrives: a
		// file, at a path, named by GOOGLE_APPLICATION_CREDENTIALS. Stow's
		// google backend falls through to the SDK's default-credentials
		// chain when the json config value is empty, and that chain reads
		// the file — so an empty STOW_GOOGLE_JSON here is the normal,
		// intended state, not a missing setting. STOW_GOOGLE_JSON accepts
		// inline key contents for arrangements without a mount. Both keys
		// must be present in the map — the backend's validator checks
		// presence, not value — and the project id is only consulted for
		// bucket creation, which this server never does: the bucket is
		// infrastructure, declared where infrastructure is declared.
		config = stow.ConfigMap{
			google.ConfigJSON:      os.Getenv("STOW_GOOGLE_JSON"),
			google.ConfigProjectId: os.Getenv("STOW_GOOGLE_PROJECT"),
		}
	default:
		return fmt.Errorf("unsupported stow kind: %s", kind)
	}
	stowPrefix = os.Getenv("STOW_PREFIX")

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
