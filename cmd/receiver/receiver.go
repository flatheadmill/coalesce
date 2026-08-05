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
	"strconv"
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

	pipelineLabel                = "coalesce.flatheadmill.com/pipeline"
	pipelineRepositoryAnnotation = "coalesce.flatheadmill.com/repository"
	pipelineEventsAnnotation     = "coalesce.flatheadmill.com/events"
	pipelineImageAnnotation      = "coalesce.flatheadmill.com/image"
	pipelineSourcesAnnotation    = "coalesce.flatheadmill.com/sources"
	pipelineSecretsAnnotation    = "coalesce.flatheadmill.com/secrets"
	webhookPayloadAnnotation     = "coalesce.flatheadmill.com/webhook-payload"
	webhookPayloadPath           = "/run/webhook/payload.json"

	// Every source carries exactly one archive and it is named ball.tar.gz,
	// because coalesce make is the one renderer and stamps the one name. The
	// loop is closed — one producer, one consumer, both in this house — so the
	// layout below insists on the name rather than globbing for whatever might
	// be there. A hand-rolled ConfigMap with a different key fails loudly at
	// tar with the missing path in the message, and the answer to "it needs a
	// specific name" is: yes, and? Use coalesce make.
	archiveKey = "ball.tar.gz"

	benchRoot = "/run/coalesce"
)

// The layout program is a constant, identical in every Job the receiver has
// ever created, and the manifest shows all of it — no entry point, no helper
// baked into an image, nothing running that kubectl get job -o yaml does not
// display. The per-Job variance rides in the environment: COALESCE_SOURCES is
// the list to lay out, COALESCE_PIPELINE the tree whose run starts. Data
// never enters program text, so there is nothing to escape and no injection
// class to argue about — the environment cannot become syntax no matter what
// it contains. ${=COALESCE_SOURCES} is Zsh's explicit word split, visibly
// overriding its refusal to split unquoted parameters.
//
// The only tools the script needs are tini, zsh, tar — furniture present in
// every image this house has ever built — so the receiver and the runner
// image may deploy in any order.
// The loop body is indented with tabs because this is a Go file and the house
// checks Go files for space indentation, string literal or not. Zsh does not
// care, and the manifest reads the same.
const layoutScript = `setopt errexit
typeset name
for name in ${=COALESCE_SOURCES}; do
	mkdir -p ` + benchRoot + `/src/$name
	tar -xzf ` + benchRoot + `/mnt/$name/` + archiveKey + ` -C ` + benchRoot + `/src/$name
done
cd ` + benchRoot + `/src/$COALESCE_PIPELINE
exec ./run`

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
// owns every interpretation of the payload after that. Number and After are
// read as data for the run's name — the pull request number is the human
// identity of a PR event and the pushed SHA of a push — the same class of
// reading as Action for dispatch, not a step toward interpreting payloads.
type webhookEnvelope struct {
	Action string `json:"action"`
	Number int    `json:"number"`
	After  string `json:"after"`

	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
}

type pipeline struct {
	Name       string
	Repository string
	Image      string
	Events     map[string]struct{}
	// Sources is the layout list. The script unpacks every name in it and then
	// cds into COALESCE_PIPELINE, which is carried separately, so the order
	// here decides nothing — the pipeline is listed first because a list that
	// opens with the thing being run reads correctly, not because anything
	// depends on it. The annotation adds whatever libraries the pipeline
	// calls; a pipeline that needs nothing but itself names no sources at all.
	Sources []string
	Secrets []string
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
	if len(configMap.BinaryData[archiveKey]) == 0 {
		return pipeline{}, fmt.Errorf("binaryData key %s is required", archiveKey)
	}
	sources, err := parseSources(configMap.Name,
		configMap.Annotations[pipelineSourcesAnnotation])
	if err != nil {
		return pipeline{}, fmt.Errorf("annotation %s: %w",
			pipelineSourcesAnnotation, err)
	}
	secrets, err := parseSecrets(configMap.Annotations[pipelineSecretsAnnotation])
	if err != nil {
		return pipeline{}, fmt.Errorf("annotation %s: %w",
			pipelineSecretsAnnotation, err)
	}

	return pipeline{
		Name:       configMap.Name,
		Repository: repository,
		Image:      image,
		Events:     events,
		Sources:    sources,
		Secrets:    secrets,
	}, nil
}

// The pipeline is always in its own source list, because it has to be laid out
// like anything else, and it is put there first so the list reads as the thing
// being run followed by what it calls. A named source is a ConfigMap in this
// namespace carrying ball.tar.gz; the receiver never opens it, so a name that
// does not resolve is a Pod that will not start rather than an error here.
func parseSources(name, value string) ([]string, error) {
	return parseResourceNames("ConfigMap", value, name)
}

func parseSecrets(value string) ([]string, error) {
	return parseResourceNames("Secret", value)
}

func parseResourceNames(kind, value string, names ...string) ([]string, error) {
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		seen[name] = struct{}{}
	}
	for _, value := range strings.Split(value, ",") {
		name := strings.TrimSpace(value)
		if name == "" {
			continue
		}
		if errs := validation.IsDNS1123Subdomain(name); len(errs) > 0 {
			return nil, fmt.Errorf("%q is not a %s name: %s",
				name, kind, strings.Join(errs, "; "))
		}
		if _, duplicate := seen[name]; duplicate {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	return names, nil
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

	// The slug is minted per pipeline, not per delivery: it opens with the
	// pipeline's own name, so two pipelines claiming one event can never
	// fight over one Job name.
	var created []string
	var failed bool
	for _, pipeline := range pipelines {
		slug := runSlug(pipeline.Name, envelope, receiver.now())
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

	// One mount per source, at the name the layout script iterates. The volume
	// is named by position because a ConfigMap name may carry dots and a volume
	// name may not; nothing reads the volume name, while the mount path is the
	// whole interface.
	mounts := []corev1.VolumeMount{
		{Name: "src", MountPath: benchRoot + "/src"},
		{Name: "webhook", MountPath: "/run/webhook", ReadOnly: true},
	}
	volumes := []corev1.Volume{
		{
			Name: "src",
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
	}
	for index, source := range pipeline.Sources {
		name := fmt.Sprintf("source-%d", index)
		mounts = append(mounts, corev1.VolumeMount{
			Name:      name,
			MountPath: benchRoot + "/mnt/" + source,
			ReadOnly:  true,
		})
		volumes = append(volumes, corev1.Volume{
			Name: name,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: source},
				},
			},
		})
	}
	secretMode := int32(0444)
	for index, secret := range pipeline.Secrets {
		name := fmt.Sprintf("secret-%d", index)
		mounts = append(mounts, corev1.VolumeMount{
			Name:      name,
			MountPath: benchRoot + "/secrets/" + secret,
			ReadOnly:  true,
		})
		// Kubelet may refresh this volume while a Job is running. A caller that
		// needs one credential for the whole run must read the file once.
		volumes = append(volumes, corev1.Volume{
			Name: name,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  secret,
					DefaultMode: &secretMode,
				},
			},
		})
	}

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      slug,
			Namespace: receiver.config.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":         "coalesce-dispatch",
				"coalesce.flatheadmill.com/slug": slug,
			},
			Annotations: map[string]string{
				pipelineRepositoryAnnotation:         pipeline.Repository,
				"coalesce.flatheadmill.com/event":    event,
				"coalesce.flatheadmill.com/delivery": delivery,
				pipelineLabel:                        pipeline.Name,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app.kubernetes.io/name":         "coalesce-dispatch",
						"coalesce.flatheadmill.com/slug": slug,
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
						// The house shape: zsh -c and a program you can read in
						// the manifest — the layoutScript constant above, the
						// same text in every Job, with the variance in env.
						//
						// tini leads it because Kubernetes sends TERM to PID 1
						// alone. Without it a run blocked in a curl or kubectl
						// finishes that child before its trap runs, and a pod
						// killed mid-step records nothing.
						Command: []string{"tini", "-g", "--", "zsh", "-c"},
						Args:    []string{layoutScript},
						Env: []corev1.EnvVar{
							{Name: "COALESCE_NAMESPACE", Value: receiver.config.Namespace},
							{Name: "COALESCE_PIPELINE", Value: pipeline.Name},
							{Name: "COALESCE_SOURCES", Value: strings.Join(pipeline.Sources, " ")},
							{Name: "COALESCE_SLUG", Value: slug},
							{Name: "COALESCE_URL", Value: receiver.config.CoalesceURL},
							{Name: "GITHUB_DELIVERY", Value: delivery},
							{Name: "GITHUB_EVENT", Value: event},
							{Name: "GITHUB_WEBHOOK_PAYLOAD_FILE", Value: webhookPayloadPath},
						},
						VolumeMounts: mounts,
					}},
					Volumes: volumes,
				},
			},
		},
	}
}

// A slug declares what counts as the same work. CI builds choose a new identity
// for every invocation; an expensive resumable pipeline can instead choose a
// stable domain identity when it calls Coalesce itself.
//
// The slug spends its characters on what an operator scans for in a pod
// listing: the pipeline's own name — the author's chosen word from the
// descriptor, no derivation here and no pattern language ever — then the
// event's human identity, a pull request number or a pushed SHA shortened to
// seven, then the same epoch as always, spelled in base36 for a smaller
// column. The executor appends its checksum and step name, and the 63-byte
// total is the pipeline author's lookout: name a step too long and the
// executor refuses the create, loudly, where the arithmetic lives.
func runSlug(name string, envelope webhookEnvelope, at time.Time) string {
	slug := name
	if envelope.Number > 0 {
		slug += fmt.Sprintf("-%d", envelope.Number)
	} else if after := strings.ToLower(envelope.After); len(after) >= 7 &&
		validRepositoryPart(after) {
		slug += "-" + after[:7]
	}
	slug += "-" + strconv.FormatInt(at.Unix(), 36)
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
