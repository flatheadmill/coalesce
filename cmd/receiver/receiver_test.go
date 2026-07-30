package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes/fake"
)

const testSecret = "correct horse battery staple"

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
	jobs, err := client.BatchV1().Jobs("coalesce").List(t.Context(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs.Items) != 1 {
		t.Fatalf("created %d Jobs, want 1", len(jobs.Items))
	}

	job := jobs.Items[0]
	if job.Name != "secrets-site-pull-request-synchronize-1754000000" {
		t.Fatalf("Job name = %q", job.Name)
	}
	if problems := validation.IsDNS1123Label(job.Name); len(problems) != 0 {
		t.Fatalf("Job name %q is invalid: %v", job.Name, problems)
	}
	container := job.Spec.Template.Spec.Containers[0]
	if container.Image != "example.invalid/runtime:test" {
		t.Fatalf("image = %q", container.Image)
	}
	if got := strings.Join(container.Command, " "); got != "/bin/sh -c" {
		t.Fatalf("command = %q", got)
	}
	if container.WorkingDir != "/run/pipeline" {
		t.Fatalf("working directory = %q", container.WorkingDir)
	}
	if !strings.Contains(container.Args[0], "exec ./run") {
		t.Fatalf("args = %q", container.Args[0])
	}

	env := environmentMap(container.Env)
	want := map[string]string{
		"COALESCE_NAMESPACE":          "coalesce",
		"COALESCE_PIPELINE":           "secrets-build",
		"COALESCE_SLUG":               "secrets-site-pull-request-synchronize-1754000000",
		"COALESCE_URL":                "http://coalesce.coalesce.svc.cluster.local",
		"GITHUB_DELIVERY":             "delivery-123",
		"GITHUB_EVENT":                "pull_request.synchronize",
		"GITHUB_WEBHOOK_PAYLOAD_FILE": webhookPayloadPath,
	}
	for name, value := range want {
		if env[name] != value {
			t.Errorf("%s = %q, want %q", name, env[name], value)
		}
	}
	if _, ok := env["GITHUB_WEBHOOK_PAYLOAD"]; ok {
		t.Error("raw payload remained in the container environment")
	}

	volume := job.Spec.Template.Spec.Volumes[0]
	if volume.ConfigMap.Name != "secrets-build" {
		t.Fatalf("pipeline ConfigMap = %q", volume.ConfigMap.Name)
	}
	if volume.ConfigMap.Items[0].Key != pipelineArchiveKey {
		t.Fatalf("archive key = %q", volume.ConfigMap.Items[0].Key)
	}
	if got := job.Spec.Template.Annotations[webhookPayloadAnnotation]; got != string(body) {
		t.Fatalf("payload annotation = %q, want %q", got, body)
	}
	webhook := job.Spec.Template.Spec.Volumes[2].DownwardAPI.Items[0]
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
	got := environmentMap(jobs.Items[0].Spec.Template.Spec.Containers[0].Env)["GITHUB_EVENT"]
	if got != "push" {
		t.Fatalf("GITHUB_EVENT = %q", got)
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

func TestReceiverSurfacesDuplicateClaim(t *testing.T) {
	handler, client, pipelines := testReceiver(t, 1<<20)
	pipelines.upsert(pipelineConfigMap(
		"secrets-build-copy",
		"example_inc/secrets.site",
		"pull_request.synchronize",
	))
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, signedRequest(t, "pull_request",
		validPayload("Example_Inc/Secrets.Site", "synchronize"), testSecret))

	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}
	jobs, err := client.BatchV1().Jobs("coalesce").List(t.Context(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs.Items) != 1 {
		t.Fatalf("created %d Jobs, want one successful claim", len(jobs.Items))
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
	delete(configMap.Annotations, pipelineImageAnnotation)

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
		name       string
		event      string
		body       []byte
		secret     string
		method     string
		want       int
		maxBytes   int64
		noDelivery bool
	}{
		{
			name:     "invalid signature",
			event:    "pull_request",
			body:     validPayload("Example_Inc/Secrets.Site", "synchronize"),
			secret:   "wrong",
			method:   http.MethodPost,
			want:     http.StatusUnauthorized,
			maxBytes: 1 << 20,
		},
		{
			name:     "oversized body",
			event:    "pull_request",
			body:     validPayload("Example_Inc/Secrets.Site", "synchronize"),
			secret:   testSecret,
			method:   http.MethodPost,
			want:     http.StatusRequestEntityTooLarge,
			maxBytes: 32,
		},
		{
			name:     "invalid JSON",
			event:    "pull_request",
			body:     []byte(`{`),
			secret:   testSecret,
			method:   http.MethodPost,
			want:     http.StatusBadRequest,
			maxBytes: 1 << 20,
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
		},
		{
			name:     "wrong organization",
			event:    "pull_request",
			body:     validPayload("someone/else", "synchronize"),
			secret:   testSecret,
			method:   http.MethodPost,
			want:     http.StatusForbidden,
			maxBytes: 1 << 20,
		},
		{
			name:     "wrong method",
			event:    "pull_request",
			body:     validPayload("Example_Inc/Secrets.Site", "synchronize"),
			secret:   testSecret,
			method:   http.MethodGet,
			want:     http.StatusMethodNotAllowed,
			maxBytes: 1 << 20,
		},
		{
			name:     "invalid repository",
			event:    "pull_request",
			body:     validPayload("not-an-owner-name", "synchronize"),
			secret:   testSecret,
			method:   http.MethodPost,
			want:     http.StatusUnprocessableEntity,
			maxBytes: 1 << 20,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler, client, _ := testReceiver(t, test.maxBytes)
			request := signedRequest(t, test.event, test.body, test.secret)
			request.Method = test.method
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
		})
	}
}

func TestRunSlugIsStructuralAndBounded(t *testing.T) {
	first := runSlug("owner/foo_bar", "pull_request.opened", testNow)
	second := runSlug("owner/foo.bar", "pull_request.opened", testNow)
	if first != second {
		t.Fatalf("sanitization collision was engineered apart: %q != %q", first, second)
	}

	long := runSlug("owner/"+strings.Repeat("a", 100), "pull_request.synchronize", testNow)
	if len(long) > 63 {
		t.Fatalf("%q is %d characters", long, len(long))
	}
	if problems := validation.IsDNS1123Label(long); len(problems) != 0 {
		t.Fatalf("%q is invalid: %v", long, problems)
	}
}

func TestRunSlugSeparatesQualifiedEvents(t *testing.T) {
	opened := runSlug("owner/repository", "pull_request.opened", testNow)
	labeled := runSlug("owner/repository", "pull_request.labeled", testNow)
	if opened == labeled {
		t.Fatalf("qualified events produced the same slug %q", opened)
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
			CoalesceURL:  "http://coalesce.coalesce.svc.cluster.local",
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
				pipelineImageAnnotation:      "example.invalid/runtime:test",
			},
		},
		BinaryData: map[string][]byte{
			pipelineArchiveKey: []byte("tar bytes"),
		},
	}
}

func environmentMap(environment []corev1.EnvVar) map[string]string {
	values := make(map[string]string, len(environment))
	for _, variable := range environment {
		values[variable.Name] = variable.Value
	}
	return values
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
		"repository": {"full_name": %q},
		"contributor_input": "$(touch /tmp/not-code)"
	}`, action, repository))
}
