// The receiver is Coalesce's public doorman, not another orchestrator. It
// verifies the GitHub delivery, translates one permitted pull request into
// one deterministic runner Job, and waits for Kubernetes to accept that Job
// before answering. Everything after creation belongs to the runner.
//
// GitHub's delivery UI is part of the diagnostic surface. A 5xx means the
// Cloudflare/Traefik porch answered but the receiver failed. A timeout points
// at the porch itself: Cloudflare takes longer to conclude a 521 or 522 than
// GitHub waits. If the API create crosses GitHub's deadline, the Job may still
// exist; redelivery is safe because AlreadyExists is success.
package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	defaultAddress      = ":8080"
	defaultNamespace    = "coalesce"
	defaultRunnerImage  = "harbor.example.org/example/coalesce-runner:latest"
	defaultPipelineMap  = "coalesce-pipeline-secrets"
	defaultPipelineFile = "build.coalesce.zsh"
	defaultCoalesceURL  = "http://coalesce.coalesce.svc.cluster.local"
	defaultMaxBodyBytes = int64(1 << 20)
	defaultAPITimeout   = 8 * time.Second
)

func main() {
	config, address, err := configFromEnvironment()
	if err != nil {
		log.Fatal(err)
	}

	restConfig, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("load in-cluster Kubernetes configuration: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		log.Fatalf("create Kubernetes client: %v", err)
	}

	handler, err := newReceiver(clientset.BatchV1().Jobs(config.Namespace), config)
	if err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.Handle(webhookPath, handler)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	server := &http.Server{
		Addr:              address,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    64 << 10,
	}
	log.Printf("Coalesce receiver listening on %s", address)
	log.Fatal(server.ListenAndServe())
}

func configFromEnvironment() (receiverConfig, string, error) {
	secret := os.Getenv("WEBHOOK_SECRET")
	if secret == "" {
		return receiverConfig{}, "", fmt.Errorf("WEBHOOK_SECRET is required")
	}

	repositoryPattern := strings.TrimSpace(os.Getenv("REPOSITORY_PATTERN"))
	if repositoryPattern == "" {
		return receiverConfig{}, "", fmt.Errorf("REPOSITORY_PATTERN is required")
	}

	maxBodyBytes := defaultMaxBodyBytes
	if value := os.Getenv("MAX_BODY_BYTES"); value != "" {
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil || parsed <= 0 {
			return receiverConfig{}, "", fmt.Errorf("MAX_BODY_BYTES must be a positive integer")
		}
		maxBodyBytes = parsed
	}

	config := receiverConfig{
		Secret:            []byte(secret),
		RepositoryPattern: repositoryPattern,
		Namespace:         environment("JOB_NAMESPACE", defaultNamespace),
		RunnerImage:       environment("RUNNER_IMAGE", defaultRunnerImage),
		PipelineConfigMap: environment("PIPELINE_CONFIG_MAP", defaultPipelineMap),
		PipelineFile:      environment("PIPELINE_FILE", defaultPipelineFile),
		CoalesceURL:       environment("COALESCE_URL", defaultCoalesceURL),
		MaxBodyBytes:      maxBodyBytes,
		APITimeout:        defaultAPITimeout,
	}
	return config, environment("LISTEN_ADDRESS", defaultAddress), nil
}

func environment(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
