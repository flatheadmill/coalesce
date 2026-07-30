// The receiver is Coalesce's public doorman, not another orchestrator. It
// verifies GitHub deliveries and dispatches them through the pipeline
// ConfigMaps currently present in its namespace. The ConfigMaps are the
// allowlist and the routing table; their ./run programs own everything after
// Kubernetes accepts the Job.
package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	kcache "k8s.io/client-go/tools/cache"
)

const (
	defaultAddress     = ":8080"
	defaultNamespace   = "coalesce"
	defaultCoalesceURL = "http://coalesce.coalesce.svc.cluster.local"
	// The signed body becomes a pod annotation so an oversized webhook fails
	// while the receiver is creating the Job, before GitHub sees green. The API
	// server allows roughly 256 KiB across an object's annotations; 192 KiB
	// leaves 64 KiB for keys and other annotations while remaining generous for
	// pull-request payloads.
	defaultMaxBodyBytes = int64(192 << 10)
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

	pipelines := newPipelineCache(config.Organization)
	factory := informers.NewSharedInformerFactoryWithOptions(
		clientset,
		0,
		informers.WithNamespace(config.Namespace),
		informers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.LabelSelector = pipelineLabel
		}),
	)
	informer := factory.Core().V1().ConfigMaps().Informer()
	if _, err := informer.AddEventHandler(pipelines.handlers()); err != nil {
		log.Fatalf("watch pipeline ConfigMaps: %v", err)
	}
	stop := make(chan struct{})
	defer close(stop)
	factory.Start(stop)
	if !kcache.WaitForCacheSync(stop, informer.HasSynced) {
		log.Fatal("pipeline ConfigMap cache did not synchronize")
	}

	handler, err := newReceiver(
		clientset.BatchV1().Jobs(config.Namespace),
		pipelines,
		config,
	)
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
	organization := strings.TrimSpace(os.Getenv("GITHUB_ORGANIZATION"))
	if organization == "" {
		return receiverConfig{}, "", fmt.Errorf("GITHUB_ORGANIZATION is required")
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
		Secret:       []byte(secret),
		Organization: organization,
		Namespace:    environment("JOB_NAMESPACE", defaultNamespace),
		CoalesceURL:  environment("COALESCE_URL", defaultCoalesceURL),
		MaxBodyBytes: maxBodyBytes,
		APITimeout:   defaultAPITimeout,
	}
	return config, environment("LISTEN_ADDRESS", defaultAddress), nil
}

func environment(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
