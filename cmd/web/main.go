package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

var clientset *kubernetes.Clientset

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

	// Serve static files from current directory
	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("."))))

	addr := fmt.Sprintf(":%d", port)
	slog.Info("Server starting", "addr", addr)

	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
