package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

const (
	testSecret           = "correct horse battery staple"
	testPipelineTemplate = `{
"metadata": {
"labels": {
"author.example/label": "kept",
"coalesce.flatheadmill.com/slug": "author-value"
},
"annotations": {
"author.example/annotation": "kept",
"coalesce.flatheadmill.com/webhook-payload": "author-value",
"coalesce.flatheadmill.com/event": "author-value",
"coalesce.flatheadmill.com/delivery": "author-value"
}
},
"spec": {
"serviceAccountName": "author-runner",
"restartPolicy": "Never",
"imagePullSecrets": [{"name": "author-registry"}],
"containers": [{
"name": "pipeline",
"image": "example.invalid/author:test",
"imagePullPolicy": "IfNotPresent",
"command": ["author", "command"],
"args": ["author program"],
"env": [
{"name": "GITHUB_EVENT", "valueFrom": {"fieldRef": {"fieldPath": "metadata.annotations['coalesce.flatheadmill.com/event']"}}},
{"name": "GITHUB_DELIVERY", "valueFrom": {"fieldRef": {"fieldPath": "metadata.annotations['coalesce.flatheadmill.com/delivery']"}}}
],
"volumeMounts": [{"name": "webhook", "mountPath": "/author/webhook", "readOnly": true}]
}],
"volumes": [{
"name": "webhook",
"downwardAPI": {"items": [{
"path": "payload.json",
"fieldRef": {"fieldPath": "metadata.annotations['coalesce.flatheadmill.com/webhook-payload']"}
}]}
}]
}
}`
)

var testNow = time.Unix(1_754_000_000, 0)

func TestReceiverDispatchesRawPayload(t *testing.T) {
	handler, client, _ := testReceiver(t, 1<<20)
	body := validPayload("Example_Inc/Secrets.Site", "synchronize")
	request := signedRequest(t, "pull_request", body, testSecret)
	request.Header.Set("X-GitHub-Delivery", "delivery-123")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}
	result := decodeDispatchResponse(t, response)
	want := dispatchAddress{
		Pipeline:  "secrets-build",
		Namespace: "coalesce",
		Slug:      "secrets-build-511-t0ab28",
	}
	if result.Status != "created" || len(result.Runs) != 1 || result.Runs[0] != want {
		t.Fatalf("response = %+v, want one created run %+v", result, want)
	}
	if result.Failures == nil || len(result.Failures) != 0 {
		t.Fatalf("failures = %#v, want an empty array", result.Failures)
	}
	jobs, err := client.BatchV1().Jobs("coalesce").List(t.Context(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs.Items) != 1 {
		t.Fatalf("created %d Jobs, want 1", len(jobs.Items))
	}

	job := jobs.Items[0]
	if job.Name != "secrets-build-511-t0ab28" {
		t.Fatalf("Job name = %q", job.Name)
	}
	if problems := validation.IsDNS1123Label(job.Name); len(problems) != 0 {
		t.Fatalf("Job name %q is invalid: %v", job.Name, problems)
	}
	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 0 {
		t.Fatalf("backoff limit = %v", job.Spec.BackoffLimit)
	}
	if job.Spec.TTLSecondsAfterFinished == nil || *job.Spec.TTLSecondsAfterFinished != 3600 {
		t.Fatalf("TTL = %v", job.Spec.TTLSecondsAfterFinished)
	}
	if got := job.Labels["app.kubernetes.io/name"]; got != "coalesce-dispatch" {
		t.Fatalf("Job app label = %q", got)
	}
	if got := job.Labels["coalesce.flatheadmill.com/slug"]; got != job.Name {
		t.Fatalf("Job slug label = %q", got)
	}
	if got := job.Annotations[pipelineRepositoryAnnotation]; got != "example_inc/secrets.site" {
		t.Fatalf("Job repository = %q", got)
	}
	if got := job.Annotations[pipelineLabel]; got != "secrets-build" {
		t.Fatalf("Job pipeline = %q", got)
	}
	if _, ok := job.Annotations["coalesce.flatheadmill.com/event"]; ok {
		t.Fatal("event remained on the Job")
	}
	if _, ok := job.Annotations["coalesce.flatheadmill.com/delivery"]; ok {
		t.Fatal("delivery remained on the Job")
	}

	template := job.Spec.Template
	if got := template.Labels["author.example/label"]; got != "kept" {
		t.Fatalf("author label = %q", got)
	}
	if got := template.Labels["app.kubernetes.io/name"]; got != "coalesce-dispatch" {
		t.Fatalf("Pod app label = %q", got)
	}
	if got := template.Annotations["author.example/annotation"]; got != "kept" {
		t.Fatalf("author annotation = %q", got)
	}
	if got := template.Labels["coalesce.flatheadmill.com/slug"]; got != job.Name {
		t.Fatalf("Pod slug label = %q", got)
	}
	if got := template.Annotations[webhookPayloadAnnotation]; got != string(body) {
		t.Fatalf("payload annotation = %q, want %q", got, body)
	}
	if got := template.Annotations["coalesce.flatheadmill.com/event"]; got != "pull_request.synchronize" {
		t.Fatalf("event annotation = %q", got)
	}
	if got := template.Annotations["coalesce.flatheadmill.com/delivery"]; got != "delivery-123" {
		t.Fatalf("delivery annotation = %q", got)
	}
	if template.Spec.ServiceAccountName != "author-runner" {
		t.Fatalf("service account = %q", template.Spec.ServiceAccountName)
	}
	if template.Spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Fatalf("restart policy = %q", template.Spec.RestartPolicy)
	}
	if len(template.Spec.ImagePullSecrets) != 1 ||
		template.Spec.ImagePullSecrets[0].Name != "author-registry" {
		t.Fatalf("image pull secrets = %+v", template.Spec.ImagePullSecrets)
	}
	container := job.Spec.Template.Spec.Containers[0]
	if container.Image != "example.invalid/author:test" {
		t.Fatalf("image = %q", container.Image)
	}
	if container.ImagePullPolicy != corev1.PullIfNotPresent {
		t.Fatalf("image pull policy = %q", container.ImagePullPolicy)
	}
	if got := strings.Join(container.Command, " "); got != "author command" {
		t.Fatalf("command = %q", got)
	}
	if got := strings.Join(container.Args, " "); got != "author program" {
		t.Fatalf("command line = %q", got)
	}
	webhook := volumeNamed(job.Spec.Template.Spec.Volumes, "webhook").DownwardAPI.Items[0]
	if webhook.Path != "payload.json" {
		t.Fatalf("payload path = %q", webhook.Path)
	}
	wantField := "metadata.annotations['" + webhookPayloadAnnotation + "']"
	if webhook.FieldRef.FieldPath != wantField {
		t.Fatalf("payload fieldRef = %q, want %q",
			webhook.FieldRef.FieldPath, wantField)
	}
}

func TestReceiverDispatchesBareEvent(t *testing.T) {
	handler, client, pipelines := testReceiver(t, 1<<20)
	pipelines.upsert(pipelineConfigMap(
		"secrets-build",
		"example_inc/secrets.site",
		"push",
	))
	body := []byte(`{"repository":{"full_name":"Example_Inc/Secrets.Site"},"ref":"refs/heads/main"}`)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, signedRequest(t, "push", body, testSecret))

	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}
	jobs, err := client.BatchV1().Jobs("coalesce").List(t.Context(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got := jobs.Items[0].Spec.Template.Annotations["coalesce.flatheadmill.com/event"]
	if got != "push" {
		t.Fatalf("GITHUB_EVENT = %q", got)
	}
}

func TestReceiverReportsPartialCreation(t *testing.T) {
	handler, client, pipelines := testReceiver(t, 1<<20)
	pipelines.upsert(pipelineConfigMap(
		"secrets-build-copy",
		"example_inc/secrets.site",
		"pull_request.synchronize",
	))
	rejectJobCreates(client, func(job *batchv1.Job) bool {
		return strings.HasPrefix(job.Name, "secrets-build-copy-")
	})
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, signedRequest(t, "pull_request",
		validPayload("Example_Inc/Secrets.Site", "synchronize"), testSecret))

	if response.Code != http.StatusMultiStatus {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}
	result := decodeDispatchResponse(t, response)
	wantRun := dispatchAddress{
		Pipeline: "secrets-build", Namespace: "coalesce", Slug: "secrets-build-511-t0ab28",
	}
	wantFailure := dispatchAddress{
		Pipeline: "secrets-build-copy", Namespace: "coalesce", Slug: "secrets-build-copy-511-t0ab28",
	}
	if result.Status != "partial" || len(result.Runs) != 1 ||
		result.Runs[0] != wantRun || len(result.Failures) != 1 ||
		result.Failures[0] != wantFailure {
		t.Fatalf("response = %+v", result)
	}
}

func TestReceiverReportsFailedCreation(t *testing.T) {
	handler, client, _ := testReceiver(t, 1<<20)
	rejectJobCreates(client, func(*batchv1.Job) bool { return true })
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, signedRequest(t, "pull_request",
		validPayload("Example_Inc/Secrets.Site", "synchronize"), testSecret))

	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}
	result := decodeDispatchResponse(t, response)
	wantFailure := dispatchAddress{
		Pipeline: "secrets-build", Namespace: "coalesce", Slug: "secrets-build-511-t0ab28",
	}
	if result.Status != "failed" || len(result.Runs) != 0 ||
		len(result.Failures) != 1 ||
		result.Failures[0] != wantFailure {
		t.Fatalf("response = %+v", result)
	}
	if !strings.Contains(response.Body.String(), `"runs":[]`) {
		t.Fatalf("empty runs did not encode as an array: %s", response.Body)
	}
}

func TestPipelineCacheRejectsUnknownTemplateField(t *testing.T) {
	pipelines := newPipelineCache("example_inc")
	configMap := pipelineConfigMap(
		"secrets-build",
		"example_inc/secrets.site",
		"push",
	)
	configMap.Data[templateKey] = "spec:\n  computeResources: {}\n"
	if _, err := pipelineFromConfigMap(configMap, "example_inc"); err == nil ||
		!strings.Contains(err.Error(), "computeResources") {
		t.Fatalf("strict decode error = %v", err)
	}
	pipelines.upsert(configMap)

	if got := pipelines.matching("example_inc/secrets.site", "push"); len(got) != 0 {
		t.Fatalf("invalid template produced %d matches", len(got))
	}
}

func TestReceiverDeepCopiesCachedTemplate(t *testing.T) {
	handler, _, pipelines := testReceiver(t, 1<<20)
	pipeline := pipelines.matching(
		"example_inc/secrets.site",
		"pull_request.synchronize",
	)[0]
	first := handler.job(pipeline, "first", "first.event", "first-delivery", []byte("first"))
	_ = handler.job(pipeline, "second", "second.event", "second-delivery", []byte("second"))

	if got := first.Spec.Template.Annotations[webhookPayloadAnnotation]; got != "first" {
		t.Fatalf("first payload changed to %q", got)
	}
	if got := pipeline.Template.Annotations[webhookPayloadAnnotation]; got != "author-value" {
		t.Fatalf("cached payload changed to %q", got)
	}

	withoutMetadata := pipeline
	withoutMetadata.Template.ObjectMeta = metav1.ObjectMeta{}
	job := handler.job(withoutMetadata, "third", "third.event", "third-delivery", []byte("third"))
	if job.Spec.Template.Labels["coalesce.flatheadmill.com/slug"] != "third" ||
		job.Spec.Template.Annotations[webhookPayloadAnnotation] != "third" {
		t.Fatalf("empty metadata was not stamped: %+v", job.Spec.Template.ObjectMeta)
	}
}

func TestReceiverDoesNotDeduplicate(t *testing.T) {
	handler, client, _ := testReceiver(t, 1<<20)
	next := testNow
	handler.now = func() time.Time {
		current := next
		next = next.Add(time.Second)
		return current
	}
	body := validPayload("Example_Inc/Secrets.Site", "synchronize")

	for range 2 {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, signedRequest(t, "pull_request", body, testSecret))
		if response.Code != http.StatusCreated {
			t.Fatalf("status = %d, body = %s", response.Code, response.Body)
		}
	}

	jobs, err := client.BatchV1().Jobs("coalesce").List(t.Context(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs.Items) != 2 {
		t.Fatalf("created %d Jobs, want 2", len(jobs.Items))
	}
}

// Two pipelines claiming one event both launch. The slug opens with each
// pipeline's own name, so there is no shared Job name to fight over — the
// old surfaced-collision behavior was an artifact of a slug minted once per
// delivery, and it retired with that slug.
func TestReceiverLaunchesEveryClaimingPipeline(t *testing.T) {
	handler, client, pipelines := testReceiver(t, 1<<20)
	pipelines.upsert(pipelineConfigMap(
		"secrets-build-copy",
		"example_inc/secrets.site",
		"pull_request.synchronize",
	))
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, signedRequest(t, "pull_request",
		validPayload("Example_Inc/Secrets.Site", "synchronize"), testSecret))

	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}
	result := decodeDispatchResponse(t, response)
	if len(result.Runs) != 2 ||
		result.Runs[0].Pipeline != "secrets-build" ||
		result.Runs[1].Pipeline != "secrets-build-copy" {
		t.Fatalf("runs are not in pipeline order: %+v", result.Runs)
	}
	jobs, err := client.BatchV1().Jobs("coalesce").List(t.Context(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs.Items) != 2 {
		t.Fatalf("created %d Jobs, want both claims", len(jobs.Items))
	}
	names := map[string]struct{}{}
	for _, job := range jobs.Items {
		names[job.Name] = struct{}{}
	}
	if len(names) != 2 {
		t.Fatalf("claims shared a Job name: %v", names)
	}
}

func TestReceiverAnswersPingWithoutCreatingJob(t *testing.T) {
	handler, client, _ := testReceiver(t, 1<<20)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, signedRequest(t, "ping",
		[]byte(`{"zen":"keep it logically awesome"}`), testSecret))

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}
	jobs, err := client.BatchV1().Jobs("coalesce").List(t.Context(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs.Items) != 0 {
		t.Fatalf("ping created %d Jobs", len(jobs.Items))
	}
}

func TestReceiverIgnoresUnclaimedEvent(t *testing.T) {
	handler, client, _ := testReceiver(t, 1<<20)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, signedRequest(t, "pull_request",
		validPayload("Example_Inc/Secrets.Site", "closed"), testSecret))

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}
	if !strings.Contains(response.Body.String(), `"status":"ignored"`) {
		t.Fatalf("body = %s", response.Body)
	}
	jobs, err := client.BatchV1().Jobs("coalesce").List(t.Context(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs.Items) != 0 {
		t.Fatalf("ignored event created %d Jobs", len(jobs.Items))
	}
}

func TestPipelineCacheTracksArrivalUpdateAndDeparture(t *testing.T) {
	pipelines := newPipelineCache("example_inc")
	configMap := pipelineConfigMap(
		"secrets-build",
		"example_inc/secrets.site",
		"pull_request.opened",
	)
	pipelines.upsert(configMap)
	if got := pipelines.matching(
		"example_inc/secrets.site",
		"pull_request.opened",
	); len(got) != 1 {
		t.Fatalf("arrival produced %d matches", len(got))
	}

	updated := configMap.DeepCopy()
	updated.Annotations[pipelineEventsAnnotation] = "pull_request.synchronize"
	pipelines.upsert(updated)
	if got := pipelines.matching(
		"example_inc/secrets.site",
		"pull_request.opened",
	); len(got) != 0 {
		t.Fatalf("old event produced %d matches after update", len(got))
	}
	if got := pipelines.matching(
		"example_inc/secrets.site",
		"pull_request.synchronize",
	); len(got) != 1 {
		t.Fatalf("new event produced %d matches after update", len(got))
	}

	pipelines.delete(updated)
	if got := pipelines.matching(
		"example_inc/secrets.site",
		"pull_request.synchronize",
	); len(got) != 0 {
		t.Fatalf("departure left %d matches", len(got))
	}
}

func TestPipelineCacheRejectsIncompleteContract(t *testing.T) {
	pipelines := newPipelineCache("example_inc")
	configMap := pipelineConfigMap(
		"secrets-build",
		"example_inc/secrets.site",
		"pull_request.synchronize",
	)
	delete(configMap.Data, templateKey)

	pipelines.upsert(configMap)

	if got := pipelines.matching(
		"example_inc/secrets.site",
		"pull_request.synchronize",
	); len(got) != 0 {
		t.Fatalf("invalid ConfigMap produced %d matches", len(got))
	}
}

func TestReceiverRejectsBadRequestsBeforeCreatingJob(t *testing.T) {
	tests := []struct {
		name        string
		event       string
		body        []byte
		secret      string
		method      string
		want        int
		maxBytes    int64
		noDelivery  bool
		contentType string
		check       string
		logContains []string
	}{
		{
			name:     "invalid signature",
			event:    "pull_request",
			body:     validPayload("Example_Inc/Secrets.Site", "synchronize"),
			secret:   "wrong",
			method:   http.MethodPost,
			want:     http.StatusUnauthorized,
			maxBytes: 1 << 20,
			check:    "signature",
		},
		{
			name:     "oversized body",
			event:    "pull_request",
			body:     validPayload("Example_Inc/Secrets.Site", "synchronize"),
			secret:   testSecret,
			method:   http.MethodPost,
			want:     http.StatusRequestEntityTooLarge,
			maxBytes: 32,
			check:    "body_size",
			logContains: []string{
				`"maximum_bytes":32`,
				`"content_length":`,
			},
		},
		{
			name:        "form encoded body is invalid JSON",
			event:       "pull_request",
			body:        []byte(`payload=%7B%22action%22%3A%22synchronize%22%7D`),
			secret:      testSecret,
			method:      http.MethodPost,
			want:        http.StatusBadRequest,
			maxBytes:    1 << 20,
			contentType: "application/x-www-form-urlencoded",
			check:       "json",
			logContains: []string{
				`"content_type":"application/x-www-form-urlencoded"`,
			},
		},
		{
			name:       "missing delivery ID",
			event:      "pull_request",
			body:       validPayload("Example_Inc/Secrets.Site", "synchronize"),
			secret:     testSecret,
			method:     http.MethodPost,
			want:       http.StatusBadRequest,
			maxBytes:   1 << 20,
			noDelivery: true,
			check:      "delivery",
		},
		{
			name:     "wrong organization",
			event:    "pull_request",
			body:     validPayload("someone/else", "synchronize"),
			secret:   testSecret,
			method:   http.MethodPost,
			want:     http.StatusForbidden,
			maxBytes: 1 << 20,
			check:    "organization",
			logContains: []string{
				`"repository":"someone/else"`,
			},
		},
		{
			name:     "wrong method",
			event:    "pull_request",
			body:     validPayload("Example_Inc/Secrets.Site", "synchronize"),
			secret:   testSecret,
			method:   http.MethodGet,
			want:     http.StatusMethodNotAllowed,
			maxBytes: 1 << 20,
			check:    "method",
			logContains: []string{
				`"method":"GET"`,
			},
		},
		{
			name:     "invalid repository",
			event:    "pull_request",
			body:     validPayload("not-an-owner-name", "synchronize"),
			secret:   testSecret,
			method:   http.MethodPost,
			want:     http.StatusUnprocessableEntity,
			maxBytes: 1 << 20,
			check:    "repository",
			logContains: []string{
				`"repository":"not-an-owner-name"`,
			},
		},
		{
			name:     "invalid event",
			event:    "pull-request",
			body:     validPayload("Example_Inc/Secrets.Site", "synchronize"),
			secret:   testSecret,
			method:   http.MethodPost,
			want:     http.StatusBadRequest,
			maxBytes: 1 << 20,
			check:    "event",
			logContains: []string{
				`"event_value":"pull-request"`,
			},
		},
		{
			name:     "invalid action",
			event:    "pull_request",
			body:     validPayload("Example_Inc/Secrets.Site", "not.valid"),
			secret:   testSecret,
			method:   http.MethodPost,
			want:     http.StatusUnprocessableEntity,
			maxBytes: 1 << 20,
			check:    "action",
			logContains: []string{
				`"action":"not.valid"`,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler, client, _ := testReceiver(t, test.maxBytes)
			logs := captureLogs(t)
			request := signedRequest(t, test.event, test.body, test.secret)
			request.Method = test.method
			if test.contentType != "" {
				request.Header.Set("Content-Type", test.contentType)
			}
			if test.noDelivery {
				request.Header.Del("X-GitHub-Delivery")
			}
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, request)

			if response.Code != test.want {
				t.Fatalf("status = %d, want %d, body = %s",
					response.Code, test.want, response.Body)
			}
			jobs, err := client.BatchV1().Jobs("coalesce").List(
				t.Context(),
				metav1.ListOptions{},
			)
			if err != nil {
				t.Fatal(err)
			}
			if len(jobs.Items) != 0 {
				t.Fatalf("rejected request created %d Jobs", len(jobs.Items))
			}
			log := logs.String()
			if !strings.Contains(log, `"level":"INFO"`) ||
				!strings.Contains(log, `"msg":"GitHub webhook rejected"`) ||
				!strings.Contains(log, `"check":"`+test.check+`"`) {
				t.Fatalf("rejection log = %s", log)
			}
			for _, value := range test.logContains {
				if !strings.Contains(log, value) {
					t.Errorf("rejection log does not contain %q: %s", value, log)
				}
			}
			if strings.Contains(log, testSecret) ||
				strings.Contains(log, request.Header.Get("X-Hub-Signature-256")) {
				t.Errorf("rejection log contains a secret: %s", log)
			}
		})
	}
}

func TestReceiverLogsBodyReadFailure(t *testing.T) {
	handler, client, _ := testReceiver(t, 1<<20)
	logs := captureLogs(t)
	request := httptest.NewRequest(http.MethodPost, webhookPath, failingReader{})
	request.Header.Set("X-GitHub-Delivery", "delivery-read")
	request.Header.Set("X-GitHub-Event", "pull_request")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}
	jobs, err := client.BatchV1().Jobs("coalesce").List(
		t.Context(),
		metav1.ListOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs.Items) != 0 {
		t.Fatalf("rejected request created %d Jobs", len(jobs.Items))
	}
	log := logs.String()
	for _, value := range []string{
		`"level":"INFO"`,
		`"check":"body_read"`,
		`"delivery":"delivery-read"`,
		`"error":"read failed"`,
	} {
		if !strings.Contains(log, value) {
			t.Errorf("rejection log does not contain %q: %s", value, log)
		}
	}
}

// The slug opens with the pipeline's own name and carries the event's human
// identity — a pull request number, or a pushed SHA shortened to seven — then
// the epoch in base36. No derivation, no truncation, no budget arithmetic:
// the 63-byte total is the pipeline author's lookout, enforced loudly by the
// executor where the child-name arithmetic lives.
func TestRunSlugCarriesNameIdentityAndEpoch(t *testing.T) {
	epoch := strconv.FormatInt(testNow.Unix(), 36)

	pr := runSlug("webster-build", webhookEnvelope{Number: 511}, testNow)
	if pr != "webster-build-511-"+epoch {
		t.Fatalf("pull request slug = %q", pr)
	}

	push := runSlug("webster-zero", webhookEnvelope{
		After: "AA427401a7f535ce2a69b51e851f5e5f1e1990d7",
	}, testNow)
	if push != "webster-zero-aa42740-"+epoch {
		t.Fatalf("push slug = %q", push)
	}

	bare := runSlug("webster-close", webhookEnvelope{}, testNow)
	if bare != "webster-close-"+epoch {
		t.Fatalf("bare slug = %q", bare)
	}

	for _, slug := range []string{pr, push, bare} {
		if problems := validation.IsDNS1123Label(slug); len(problems) != 0 {
			t.Fatalf("%q is invalid: %v", slug, problems)
		}
	}
}

// Two pipelines claiming one event mint two names, because the slug opens
// with the pipeline's name — the old shared-slug collision cannot happen.
func TestRunSlugSeparatesPipelines(t *testing.T) {
	build := runSlug("example-build", webhookEnvelope{Number: 7}, testNow)
	scan := runSlug("example-scan", webhookEnvelope{Number: 7}, testNow)
	if build == scan {
		t.Fatalf("pipelines produced the same slug %q", build)
	}
}

func testReceiver(
	t *testing.T,
	maxBodyBytes int64,
) (*receiver, *fake.Clientset, *pipelineCache) {
	t.Helper()
	client := fake.NewSimpleClientset()
	pipelines := newPipelineCache("example_inc")
	pipelines.upsert(pipelineConfigMap(
		"secrets-build",
		"example_inc/secrets.site",
		"pull_request.opened,pull_request.reopened,pull_request.synchronize",
	))
	handler, err := newReceiver(
		client.BatchV1().Jobs("coalesce"),
		pipelines,
		receiverConfig{
			Secret:       []byte(testSecret),
			Organization: "example_inc",
			Namespace:    "coalesce",
			MaxBodyBytes: maxBodyBytes,
			APITimeout:   time.Second,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	handler.now = func() time.Time { return testNow }
	return handler, client, pipelines
}

func pipelineConfigMap(name, repository, events string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "coalesce",
			Labels: map[string]string{
				pipelineLabel: "",
			},
			Annotations: map[string]string{
				pipelineRepositoryAnnotation: repository,
				pipelineEventsAnnotation:     events,
			},
		},
		Data: map[string]string{
			templateKey: testPipelineTemplate,
		},
		BinaryData: map[string][]byte{
			archiveKey: []byte("tar bytes"),
		},
	}
}

func decodeDispatchResponse(t *testing.T, response *httptest.ResponseRecorder) dispatchResponse {
	t.Helper()
	var result dispatchResponse
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode response: %v; body = %s", err, response.Body)
	}
	return result
}

func rejectJobCreates(client *fake.Clientset, reject func(*batchv1.Job) bool) {
	client.Fake.PrependReactor("create", "jobs", func(action ktesting.Action) (
		bool, runtime.Object, error,
	) {
		job := action.(ktesting.CreateAction).GetObject().(*batchv1.Job)
		if reject(job) {
			return true, nil, errors.New("create refused")
		}
		return false, nil, nil
	})
}

func volumeNamed(volumes []corev1.Volume, name string) corev1.Volume {
	for _, volume := range volumes {
		if volume.Name == name {
			return volume
		}
	}
	return corev1.Volume{}
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) {
	return 0, errors.New("read failed")
}

func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buffer bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buffer, nil)))
	t.Cleanup(func() {
		slog.SetDefault(previous)
	})
	return &buffer
}

func signedRequest(t *testing.T, event string, body []byte, secret string) *http.Request {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, webhookPath,
		strings.NewReader(string(body)))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-GitHub-Delivery", "delivery-test")
	request.Header.Set("X-GitHub-Event", event)
	request.Header.Set("X-Hub-Signature-256", signature([]byte(secret), body))
	return request
}

func signature(secret, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func validPayload(repository, action string) []byte {
	return []byte(fmt.Sprintf(`{
		"action": %q,
		"number": 511,
		"repository": {"full_name": %q},
		"contributor_input": "$(touch /tmp/not-code)"
	}`, action, repository))
}
