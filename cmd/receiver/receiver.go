package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	batchclient "k8s.io/client-go/kubernetes/typed/batch/v1"
	kcache "k8s.io/client-go/tools/cache"
)

const (
	webhookPath = "/webhooks/github"

	pipelineLabel                = "flatheadmill.github.io/pipeline"
	pipelineRepositoryAnnotation = "flatheadmill.github.io/repository"
	pipelineEventsAnnotation     = "flatheadmill.github.io/events"
	pipelineImageAnnotation      = "flatheadmill.github.io/image"
	pipelineArchiveKey           = "pipeline.tar.gz"
	webhookPayloadAnnotation     = "flatheadmill.github.io/webhook-payload"
	webhookPayloadPath           = "/run/webhook/payload.json"
)

type receiverConfig struct {
	Secret       []byte
	Organization string
	Namespace    string
	CoalesceURL  string
	MaxBodyBytes int64
	APITimeout   time.Duration
}

type receiver struct {
	jobs      batchclient.JobInterface
	pipelines *pipelineCache
	config    receiverConfig
	now       func() time.Time
}

// webhookEnvelope is deliberately smaller than any GitHub event. The receiver
// reads only enough of the signed document to find its dispatch rules; ./run
// owns every interpretation of the payload after that.
type webhookEnvelope struct {
	Action     string `json:"action"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
}

type pipeline struct {
	Name       string
	Repository string
	Image      string
	Events     map[string]struct{}
}

type pipelineCache struct {
	mu           sync.RWMutex
	organization string
	pipelines    map[string]pipeline
}

func newPipelineCache(organization string) *pipelineCache {
	return &pipelineCache{
		organization: strings.ToLower(organization),
		pipelines:    make(map[string]pipeline),
	}
}

func (pipelines *pipelineCache) handlers() kcache.ResourceEventHandlerFuncs {
	return kcache.ResourceEventHandlerFuncs{
		AddFunc:    pipelines.upsert,
		UpdateFunc: func(_, current any) { pipelines.upsert(current) },
		DeleteFunc: pipelines.delete,
	}
}

func (pipelines *pipelineCache) upsert(object any) {
	configMap, ok := object.(*corev1.ConfigMap)
	if !ok {
		return
	}

	pipeline, err := pipelineFromConfigMap(configMap, pipelines.organization)
	key := configMap.Namespace + "/" + configMap.Name

	pipelines.mu.Lock()
	defer pipelines.mu.Unlock()
	if err != nil {
		delete(pipelines.pipelines, key)
		slog.Error("Ignoring invalid pipeline ConfigMap",
			"error", err,
			"config_map", key,
		)
		return
	}
	pipelines.pipelines[key] = pipeline
	slog.Info("Pipeline cached",
		"config_map", key,
		"repository", pipeline.Repository,
	)
}

func (pipelines *pipelineCache) delete(object any) {
	configMap, ok := object.(*corev1.ConfigMap)
	if !ok {
		tombstone, tombstoneOK := object.(kcache.DeletedFinalStateUnknown)
		if !tombstoneOK {
			return
		}
		configMap, ok = tombstone.Obj.(*corev1.ConfigMap)
		if !ok {
			return
		}
	}

	key := configMap.Namespace + "/" + configMap.Name
	pipelines.mu.Lock()
	delete(pipelines.pipelines, key)
	pipelines.mu.Unlock()
	slog.Info("Pipeline removed", "config_map", key)
}

func (pipelines *pipelineCache) matching(repository, event string) []pipeline {
	pipelines.mu.RLock()
	defer pipelines.mu.RUnlock()

	var matches []pipeline
	for _, pipeline := range pipelines.pipelines {
		if pipeline.Repository != repository {
			continue
		}
		if _, ok := pipeline.Events[event]; ok {
			matches = append(matches, pipeline)
		}
	}
	sort.Slice(matches, func(i, j int) bool {
		return matches[i].Name < matches[j].Name
	})
	return matches
}

func pipelineFromConfigMap(configMap *corev1.ConfigMap, organization string) (pipeline, error) {
	if _, ok := configMap.Labels[pipelineLabel]; !ok {
		return pipeline{}, fmt.Errorf("label %s is required", pipelineLabel)
	}

	repository := strings.ToLower(strings.TrimSpace(
		configMap.Annotations[pipelineRepositoryAnnotation],
	))
	owner, _, err := splitRepository(repository)
	if err != nil {
		return pipeline{}, fmt.Errorf("annotation %s: %w",
			pipelineRepositoryAnnotation, err)
	}
	if owner != organization {
		return pipeline{}, fmt.Errorf("repository organization %q does not match %q",
			owner, organization)
	}

	image := strings.TrimSpace(configMap.Annotations[pipelineImageAnnotation])
	if image == "" {
		return pipeline{}, fmt.Errorf("annotation %s is required", pipelineImageAnnotation)
	}
	events, err := parseEvents(configMap.Annotations[pipelineEventsAnnotation])
	if err != nil {
		return pipeline{}, fmt.Errorf("annotation %s: %w",
			pipelineEventsAnnotation, err)
	}
	if len(configMap.BinaryData[pipelineArchiveKey]) == 0 {
		return pipeline{}, fmt.Errorf("binaryData key %s is required", pipelineArchiveKey)
	}

	return pipeline{
		Name:       configMap.Name,
		Repository: repository,
		Image:      image,
		Events:     events,
	}, nil
}

func parseEvents(value string) (map[string]struct{}, error) {
	events := make(map[string]struct{})
	for _, value := range strings.Split(value, ",") {
		event := strings.ToLower(strings.TrimSpace(value))
		if event == "" {
			continue
		}
		parts := strings.Split(event, ".")
		if len(parts) > 2 {
			return nil, fmt.Errorf("%q is not an event or event.action", event)
		}
		for _, part := range parts {
			if !validEventPart(part) {
				return nil, fmt.Errorf("%q is invalid", event)
			}
		}
		events[event] = struct{}{}
	}
	if len(events) == 0 {
		return nil, errors.New("at least one event is required")
	}
	return events, nil
}

func validEventPart(value string) bool {
	if value == "" {
		return false
	}
	for _, char := range value {
		if char < 'a' || char > 'z' {
			if char < '0' || char > '9' {
				if char != '_' {
					return false
				}
			}
		}
	}
	return true
}

func newReceiver(
	jobs batchclient.JobInterface,
	pipelines *pipelineCache,
	config receiverConfig,
) (*receiver, error) {
	switch {
	case len(config.Secret) == 0:
		return nil, errors.New("webhook secret is required")
	case config.Organization == "":
		return nil, errors.New("organization is required")
	case !validRepositoryPart(config.Organization):
		return nil, errors.New("organization is invalid")
	case config.Namespace == "":
		return nil, errors.New("job namespace is required")
	case config.CoalesceURL == "":
		return nil, errors.New("Coalesce URL is required")
	case config.MaxBodyBytes <= 0:
		return nil, errors.New("maximum body size must be positive")
	case config.APITimeout <= 0:
		return nil, errors.New("API timeout must be positive")
	case pipelines == nil:
		return nil, errors.New("pipeline cache is required")
	}

	config.Organization = strings.ToLower(config.Organization)
	return &receiver{
		jobs:      jobs,
		pipelines: pipelines,
		config:    config,
		now:       time.Now,
	}, nil
}

func (receiver *receiver) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		rejectWebhook(w, request,
			http.StatusMethodNotAllowed,
			"Method not allowed",
			"method",
			"method", request.Method,
		)
		return
	}

	request.Body = http.MaxBytesReader(w, request.Body, receiver.config.MaxBodyBytes)
	body, err := io.ReadAll(request.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			rejectWebhook(w, request,
				http.StatusRequestEntityTooLarge,
				"Payload too large",
				"body_size",
				"content_length", request.ContentLength,
				"bytes_read", len(body),
				"maximum_bytes", receiver.config.MaxBodyBytes,
			)
			return
		}
		rejectWebhook(w, request,
			http.StatusBadRequest,
			"Unable to read payload",
			"body_read",
			"error", err,
		)
		return
	}

	// HTTP/2 lowercases field names on the wire. Header.Get canonicalizes its
	// lookup, so the signature survives the Cloudflare-to-Traefik porch.
	signature := request.Header.Get("X-Hub-Signature-256")
	if !validSignature(receiver.config.Secret, body, signature) {
		rejectWebhook(w, request,
			http.StatusUnauthorized,
			"Invalid signature",
			"signature",
		)
		return
	}

	event := strings.ToLower(strings.TrimSpace(request.Header.Get("X-GitHub-Event")))
	delivery := request.Header.Get("X-GitHub-Delivery")
	if delivery == "" {
		rejectWebhook(w, request,
			http.StatusBadRequest,
			"GitHub delivery ID is required",
			"delivery",
		)
		return
	}
	if event == "ping" {
		writeJSON(w, http.StatusOK, map[string]string{"status": "pong"})
		return
	}
	if !validEventPart(event) {
		rejectWebhook(w, request,
			http.StatusBadRequest,
			"GitHub event is invalid",
			"event",
			"event_value", event,
		)
		return
	}

	var envelope webhookEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		rejectWebhook(w, request,
			http.StatusBadRequest,
			"Invalid JSON",
			"json",
			"content_type", request.Header.Get("Content-Type"),
			"error", err,
		)
		return
	}
	owner, _, err := splitRepository(envelope.Repository.FullName)
	if err != nil {
		rejectWebhook(w, request,
			http.StatusUnprocessableEntity,
			"Repository full_name is invalid",
			"repository",
			"repository", envelope.Repository.FullName,
		)
		return
	}
	if !strings.EqualFold(owner, receiver.config.Organization) {
		rejectWebhook(w, request,
			http.StatusForbidden,
			"Repository organization not allowed",
			"organization",
			"repository", envelope.Repository.FullName,
			"organization", owner,
		)
		return
	}

	repository := strings.ToLower(envelope.Repository.FullName)
	qualifiedEvent := event
	if envelope.Action != "" {
		action := strings.ToLower(envelope.Action)
		if !validEventPart(action) {
			rejectWebhook(w, request,
				http.StatusUnprocessableEntity,
				"GitHub action is invalid",
				"action",
				"repository", repository,
				"action", action,
			)
			return
		}
		qualifiedEvent += "." + action
	}
	pipelines := receiver.pipelines.matching(repository, qualifiedEvent)
	if len(pipelines) == 0 {
		slog.Info("No pipeline claimed GitHub event",
			"repository", repository,
			"event", qualifiedEvent,
			"delivery", delivery,
		)
		writeJSON(w, http.StatusOK, map[string]string{
			"status": "ignored",
			"event":  qualifiedEvent,
		})
		return
	}

	slug := runSlug(repository, qualifiedEvent, receiver.now())
	var created []string
	var failed bool
	for _, pipeline := range pipelines {
		job := receiver.job(pipeline, slug, qualifiedEvent, delivery, body)
		ctx, cancel := context.WithTimeout(request.Context(), receiver.config.APITimeout)
		_, err := receiver.jobs.Create(ctx, job, metav1.CreateOptions{})
		cancel()
		if err != nil {
			failed = true
			slog.Error("Unable to create pipeline Job",
				"error", err,
				"delivery", delivery,
				"repository", repository,
				"pipeline", pipeline.Name,
				"job", job.Name,
			)
			continue
		}
		created = append(created, job.Name)
		slog.Info("Pipeline Job created",
			"delivery", delivery,
			"repository", repository,
			"pipeline", pipeline.Name,
			"job", job.Name,
		)
	}

	if failed {
		http.Error(w, "Unable to create every pipeline", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"status": "created",
		"jobs":   created,
	})
}

func rejectWebhook(
	w http.ResponseWriter,
	request *http.Request,
	status int,
	response, check string,
	details ...any,
) {
	attributes := []any{
		"check", check,
		"status", status,
	}
	if delivery := request.Header.Get("X-GitHub-Delivery"); delivery != "" {
		attributes = append(attributes, "delivery", delivery)
	}
	if event := request.Header.Get("X-GitHub-Event"); event != "" {
		attributes = append(attributes, "event", event)
	}
	attributes = append(attributes, details...)
	slog.Info("GitHub webhook rejected", attributes...)
	http.Error(w, response, status)
}

func validSignature(secret, body []byte, signature string) bool {
	const prefix = "sha256="
	if !strings.HasPrefix(signature, prefix) {
		return false
	}
	provided, err := hex.DecodeString(strings.TrimPrefix(signature, prefix))
	if err != nil || len(provided) != sha256.Size {
		return false
	}

	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(body)
	return hmac.Equal(provided, mac.Sum(nil))
}

func splitRepository(repository string) (string, string, error) {
	parts := strings.Split(repository, "/")
	if len(parts) != 2 || !validRepositoryPart(parts[0]) ||
		!validRepositoryPart(parts[1]) {
		return "", "", errors.New("must be an owner/name pair")
	}
	return strings.ToLower(parts[0]), strings.ToLower(parts[1]), nil
}

func validRepositoryPart(value string) bool {
	if value == "" {
		return false
	}
	for _, char := range value {
		valid := char >= 'A' && char <= 'Z' ||
			char >= 'a' && char <= 'z' ||
			char >= '0' && char <= '9' ||
			strings.ContainsRune("_.-", char)
		if !valid {
			return false
		}
	}
	return true
}

func (receiver *receiver) job(
	pipeline pipeline,
	slug, event, delivery string,
	payload []byte,
) *batchv1.Job {
	backoffLimit := int32(0)
	ttl := int32(3600)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      slug,
			Namespace: receiver.config.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":      "coalesce-dispatch",
				"flatheadmill.github.io/slug": slug,
			},
			Annotations: map[string]string{
				pipelineRepositoryAnnotation:      pipeline.Repository,
				"flatheadmill.github.io/event":    event,
				"flatheadmill.github.io/delivery": delivery,
				pipelineLabel:                     pipeline.Name,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app.kubernetes.io/name":      "coalesce-dispatch",
						"flatheadmill.github.io/slug": slug,
					},
					Annotations: map[string]string{
						webhookPayloadAnnotation: string(payload),
					},
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: "coalesce-runner",
					RestartPolicy:      corev1.RestartPolicyNever,
					ImagePullSecrets: []corev1.LocalObjectReference{
						{Name: "registry"},
					},
					Containers: []corev1.Container{{
						Name:            "pipeline",
						Image:           pipeline.Image,
						ImagePullPolicy: corev1.PullAlways,
						Command:         []string{"/bin/sh", "-c"},
						WorkingDir:      "/run/pipeline",
						Args: []string{
							"set -eu\n" +
								"tar -xzf /run/coalesce/pipeline.tar.gz -C .\n" +
								"exec ./run",
						},
						Env: []corev1.EnvVar{
							{Name: "COALESCE_NAMESPACE", Value: receiver.config.Namespace},
							{Name: "COALESCE_PIPELINE", Value: pipeline.Name},
							{Name: "COALESCE_SLUG", Value: slug},
							{Name: "COALESCE_URL", Value: receiver.config.CoalesceURL},
							{Name: "GITHUB_DELIVERY", Value: delivery},
							{Name: "GITHUB_EVENT", Value: event},
							{Name: "GITHUB_WEBHOOK_PAYLOAD_FILE", Value: webhookPayloadPath},
						},
						VolumeMounts: []corev1.VolumeMount{
							{
								Name:      "archive",
								MountPath: "/run/coalesce",
								ReadOnly:  true,
							},
							{
								Name:      "pipeline",
								MountPath: "/run/pipeline",
							},
							{
								Name:      "webhook",
								MountPath: "/run/webhook",
								ReadOnly:  true,
							},
						},
					}},
					Volumes: []corev1.Volume{
						{
							Name: "archive",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: pipeline.Name,
									},
									Items: []corev1.KeyToPath{{
										Key:  pipelineArchiveKey,
										Path: pipelineArchiveKey,
									}},
								},
							},
						},
						{
							Name: "pipeline",
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{},
							},
						},
						{
							Name: "webhook",
							VolumeSource: corev1.VolumeSource{
								DownwardAPI: &corev1.DownwardAPIVolumeSource{
									Items: []corev1.DownwardAPIVolumeFile{{
										Path: "payload.json",
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "metadata.annotations['" +
												webhookPayloadAnnotation + "']",
										},
									}},
								},
							},
						},
					},
				},
			},
		},
	}
}

// A slug declares what counts as the same work. CI builds choose a new identity
// for every invocation; an expensive resumable pipeline can instead choose a
// stable domain identity when it calls Coalesce itself.
func runSlug(repository, event string, at time.Time) string {
	_, name, err := splitRepository(repository)
	if err != nil {
		panic(err)
	}
	epoch := fmt.Sprintf("-%d", at.Unix())
	qualifiedEvent := sanitizeDNSLabel(event)
	// This receiver currently launches one step named build. Its executor Job
	// adds "-" + eight CRC32 hex digits + "-build" to this slug, so the slug is
	// not the last derived Kubernetes name and must leave that concrete room.
	// A future pipeline with a longer leaf is rejected where the executor knows
	// the actual name rather than making every repository pay for a guessed cap.
	const buildJobSuffixBudget = len("-ffffffff-build")
	maxSlug := 63 - buildJobSuffixBudget
	nameBudget := maxSlug - len(epoch) - len(qualifiedEvent) - 1
	if nameBudget < 1 {
		panic(fmt.Sprintf("qualified event %q leaves no room for a repository slug", event))
	}
	visibleName := sanitizeDNSLabel(name)
	if len(visibleName) > nameBudget {
		visibleName = strings.Trim(visibleName[:nameBudget], "-")
	}
	slug := visibleName + "-" + qualifiedEvent + epoch
	if problems := validation.IsDNS1123Label(slug); len(problems) != 0 {
		panic(fmt.Sprintf("generated invalid slug %q: %s",
			slug, strings.Join(problems, ", ")))
	}
	return slug
}

func sanitizeDNSLabel(value string) string {
	var builder strings.Builder
	lastDash := false
	for _, char := range strings.ToLower(value) {
		valid := char >= 'a' && char <= 'z' || char >= '0' && char <= '9'
		if valid {
			builder.WriteRune(char)
			lastDash = false
		} else if !lastDash {
			builder.WriteByte('-')
			lastDash = true
		}
	}
	return strings.Trim(builder.String(), "-")
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
