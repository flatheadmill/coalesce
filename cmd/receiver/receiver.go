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
	"regexp"
	"strconv"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	batchclient "k8s.io/client-go/kubernetes/typed/batch/v1"
)

const webhookPath = "/webhooks/github"

var (
	repositoryPart = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)
	gitSHA         = regexp.MustCompile(`^[0-9a-f]{40}$`)
	buildActions   = map[string]struct{}{
		"opened":      {},
		"reopened":    {},
		"synchronize": {},
	}
)

type receiverConfig struct {
	Secret            []byte
	Allowed           map[string]struct{}
	Namespace         string
	RunnerImage       string
	PipelineConfigMap string
	PipelineFile      string
	CoalesceURL       string
	MaxBodyBytes      int64
	APITimeout        time.Duration
}

type receiver struct {
	jobs   batchclient.JobInterface
	config receiverConfig
}

type pullRequestEvent struct {
	Action     string `json:"action"`
	Number     int    `json:"number"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
	PullRequest struct {
		Number int `json:"number"`
		Base   struct {
			Ref string `json:"ref"`
			SHA string `json:"sha"`
		} `json:"base"`
		Head struct {
			Ref string `json:"ref"`
			SHA string `json:"sha"`
		} `json:"head"`
	} `json:"pull_request"`
}

func newReceiver(jobs batchclient.JobInterface, config receiverConfig) (*receiver, error) {
	switch {
	case len(config.Secret) == 0:
		return nil, errors.New("webhook secret is required")
	case len(config.Allowed) == 0:
		return nil, errors.New("at least one allowed repository is required")
	case config.Namespace == "":
		return nil, errors.New("job namespace is required")
	case config.RunnerImage == "":
		return nil, errors.New("runner image is required")
	case config.PipelineConfigMap == "":
		return nil, errors.New("pipeline ConfigMap is required")
	case config.PipelineFile == "":
		return nil, errors.New("pipeline file is required")
	case config.CoalesceURL == "":
		return nil, errors.New("Coalesce URL is required")
	case config.MaxBodyBytes <= 0:
		return nil, errors.New("maximum body size must be positive")
	case config.APITimeout <= 0:
		return nil, errors.New("API timeout must be positive")
	}

	allowed := make(map[string]struct{}, len(config.Allowed))
	for repository := range config.Allowed {
		repository = strings.ToLower(strings.TrimSpace(repository))
		if err := validateRepository(repository); err != nil {
			return nil, fmt.Errorf("allowed repository %q: %w", repository, err)
		}
		allowed[repository] = struct{}{}
	}
	config.Allowed = allowed

	return &receiver{jobs: jobs, config: config}, nil
}

func (receiver *receiver) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	request.Body = http.MaxBytesReader(w, request.Body, receiver.config.MaxBodyBytes)
	body, err := io.ReadAll(request.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			http.Error(w, "Payload too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "Unable to read payload", http.StatusBadRequest)
		return
	}

	// HTTP/2 lowercases field names on the wire. Header.Get canonicalizes its
	// lookup, so the signature survives the Cloudflare-to-Traefik porch.
	signature := request.Header.Get("X-Hub-Signature-256")
	if !validSignature(receiver.config.Secret, body, signature) {
		http.Error(w, "Invalid signature", http.StatusUnauthorized)
		return
	}

	event := request.Header.Get("X-GitHub-Event")
	delivery := request.Header.Get("X-GitHub-Delivery")
	if delivery == "" {
		http.Error(w, "GitHub delivery ID is required", http.StatusBadRequest)
		return
	}
	if event == "ping" {
		writeJSON(w, http.StatusOK, map[string]string{"status": "pong"})
		return
	}
	if event != "pull_request" {
		http.Error(w, "Unsupported GitHub event", http.StatusUnprocessableEntity)
		return
	}

	var payload pullRequestEvent
	if err := json.Unmarshal(body, &payload); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	if err := validatePullRequest(payload); err != nil {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}

	repository := strings.ToLower(payload.Repository.FullName)
	if _, ok := receiver.config.Allowed[repository]; !ok {
		http.Error(w, "Repository not allowed", http.StatusForbidden)
		return
	}
	if _, ok := buildActions[payload.Action]; !ok {
		writeJSON(w, http.StatusOK, map[string]string{
			"status": "ignored",
			"action": payload.Action,
		})
		return
	}

	job := receiver.job(payload, delivery)
	ctx, cancel := context.WithTimeout(request.Context(), receiver.config.APITimeout)
	defer cancel()

	_, err = receiver.jobs.Create(ctx, job, metav1.CreateOptions{})
	switch {
	case apierrors.IsAlreadyExists(err):
		// GitHub can time out while the API server completes the create. A
		// redelivery proves its work by finding the deterministic Job.
		writeJSON(w, http.StatusOK, map[string]string{
			"status": "exists",
			"job":    job.Name,
		})
	case err != nil:
		slog.Error("Unable to create pipeline Job",
			"error", err,
			"delivery", delivery,
			"repository", repository,
			"job", job.Name,
		)
		http.Error(w, "Unable to create pipeline", http.StatusInternalServerError)
	default:
		slog.Info("Pipeline Job created",
			"delivery", delivery,
			"repository", repository,
			"job", job.Name,
		)
		writeJSON(w, http.StatusCreated, map[string]string{
			"status": "created",
			"job":    job.Name,
		})
	}
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

func validatePullRequest(payload pullRequestEvent) error {
	switch {
	case payload.Action == "":
		return errors.New("pull request action is required")
	case payload.Number <= 0:
		return errors.New("pull request number must be positive")
	case payload.PullRequest.Number != payload.Number:
		return errors.New("pull request numbers do not match")
	case validateRepository(payload.Repository.FullName) != nil:
		return errors.New("repository full_name is invalid")
	case !validRef(payload.PullRequest.Base.Ref):
		return errors.New("base ref is invalid")
	case !gitSHA.MatchString(payload.PullRequest.Base.SHA):
		return errors.New("base SHA is invalid")
	case !validRef(payload.PullRequest.Head.Ref):
		return errors.New("head ref is invalid")
	case !gitSHA.MatchString(payload.PullRequest.Head.SHA):
		return errors.New("head SHA is invalid")
	default:
		return nil
	}
}

func validateRepository(repository string) error {
	parts := strings.Split(repository, "/")
	if len(parts) != 2 || !repositoryPart.MatchString(parts[0]) ||
		!repositoryPart.MatchString(parts[1]) {
		return errors.New("must be an owner/name pair")
	}
	return nil
}

func validRef(ref string) bool {
	if ref == "" || len(ref) > 255 || ref[0] == '-' ||
		ref == "@" || strings.HasPrefix(ref, "/") ||
		strings.HasSuffix(ref, "/") || strings.HasSuffix(ref, ".") ||
		strings.Contains(ref, "..") || strings.Contains(ref, "//") ||
		strings.Contains(ref, "@{") {
		return false
	}
	for _, char := range ref {
		if char <= '\x20' || char == '\x7f' ||
			strings.ContainsRune("~^:?*[\\", char) {
			return false
		}
	}
	for _, part := range strings.Split(ref, "/") {
		if strings.HasPrefix(part, ".") || strings.HasSuffix(part, ".lock") {
			return false
		}
	}
	return true
}

func (receiver *receiver) job(payload pullRequestEvent, delivery string) *batchv1.Job {
	slug := jobName(payload.Repository.FullName, payload.Number, payload.PullRequest.Head.SHA)
	backoffLimit := int32(0)
	ttl := int32(3600)
	runAsNonRoot := true
	runAsUser := int64(1000)
	allowPrivilegeEscalation := false

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      slug,
			Namespace: receiver.config.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":          "coalesce-runner",
				"flatheadmill.github.io/slug":     slug,
				"flatheadmill.github.io/pipeline": "build",
			},
			Annotations: map[string]string{
				"flatheadmill.github.io/repository":   payload.Repository.FullName,
				"flatheadmill.github.io/pull-request": strconv.Itoa(payload.Number),
				"flatheadmill.github.io/head-sha":     payload.PullRequest.Head.SHA,
				"flatheadmill.github.io/delivery":     delivery,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app.kubernetes.io/name":      "coalesce-runner",
						"flatheadmill.github.io/slug": slug,
					},
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: "coalesce-runner",
					RestartPolicy:      corev1.RestartPolicyNever,
					ImagePullSecrets: []corev1.LocalObjectReference{
						{Name: "registry"},
					},
					Containers: []corev1.Container{{
						Name:            "runner",
						Image:           receiver.config.RunnerImage,
						ImagePullPolicy: corev1.PullAlways,
						Args: []string{
							"run",
							"-N", receiver.config.Namespace,
							"-s", slug,
							"/run/coalesce/pipelines/" + receiver.config.PipelineFile,
						},
						Env: []corev1.EnvVar{
							{Name: "COALESCE_URL", Value: receiver.config.CoalesceURL},
							{Name: "GITHUB_ACTION", Value: payload.Action},
							{Name: "GITHUB_REPOSITORY", Value: payload.Repository.FullName},
							{Name: "GITHUB_PR_NUMBER", Value: strconv.Itoa(payload.Number)},
							{Name: "GITHUB_BASE_REF", Value: payload.PullRequest.Base.Ref},
							{Name: "GITHUB_BASE_SHA", Value: payload.PullRequest.Base.SHA},
							{Name: "GITHUB_HEAD_REF", Value: payload.PullRequest.Head.Ref},
							{Name: "GITHUB_HEAD_SHA", Value: payload.PullRequest.Head.SHA},
						},
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: &allowPrivilegeEscalation,
							Capabilities: &corev1.Capabilities{
								Drop: []corev1.Capability{"ALL"},
							},
							RunAsNonRoot: &runAsNonRoot,
							RunAsUser:    &runAsUser,
							SeccompProfile: &corev1.SeccompProfile{
								Type: corev1.SeccompProfileTypeRuntimeDefault,
							},
						},
						VolumeMounts: []corev1.VolumeMount{{
							Name:      "pipeline",
							MountPath: "/run/coalesce/pipelines",
							ReadOnly:  true,
						}},
					}},
					Volumes: []corev1.Volume{{
						Name: "pipeline",
						VolumeSource: corev1.VolumeSource{
							ConfigMap: &corev1.ConfigMapVolumeSource{
								LocalObjectReference: corev1.LocalObjectReference{
									Name: receiver.config.PipelineConfigMap,
								},
							},
						},
					}},
				},
			},
		},
	}
}

func jobName(repository string, number int, headSHA string) string {
	raw := strings.ToLower(repository) + "\x00" + strconv.Itoa(number) + "\x00" + headSHA
	sum := sha256.Sum256([]byte(raw))
	suffix := hex.EncodeToString(sum[:])[:10]

	visible := fmt.Sprintf("coalesce-%s-pr-%d-%s",
		strings.ReplaceAll(strings.ToLower(repository), "/", "-"),
		number,
		headSHA[:12],
	)
	visible = sanitizeDNSLabel(visible)
	maxVisible := 63 - 1 - len(suffix)
	if len(visible) > maxVisible {
		visible = strings.Trim(visible[:maxVisible], "-")
	}
	name := visible + "-" + suffix
	if problems := validation.IsDNS1123Label(name); len(problems) != 0 {
		panic(fmt.Sprintf("generated invalid Job name %q: %s", name, strings.Join(problems, ", ")))
	}
	return name
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
