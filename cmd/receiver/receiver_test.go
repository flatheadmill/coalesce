package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
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
	if got := strings.Join(container.Command, " "); got != "tini -g -- zsh -c" {
		t.Fatalf("command = %q", got)
	}
	// The program is the same constant in every Job; the variance is data in
	// the environment, asserted below.
	if got := strings.Join(container.Args, " "); got != layoutScript {
		t.Fatalf("command line = %q", got)
	}
	if got := mountPath(container.VolumeMounts, "source-0"); got != benchRoot+"/mnt/secrets-build" {
		t.Fatalf("pipeline source mounted at %q", got)
	}
	if got := mountPath(container.VolumeMounts, "src"); got != benchRoot+"/src" {
		t.Fatalf("source trees mounted at %q", got)
	}

	env := environmentMap(container.Env)
	want := map[string]string{
		"COALESCE_NAMESPACE":          "coalesce",
		"COALESCE_PIPELINE":           "secrets-build",
		"COALESCE_SOURCES":            "secrets-build",
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

	// The whole ConfigMap is mounted rather than a named key. Every source
	// carries ball.tar.gz, but selecting it with items would turn a wrong key
	// into a mount that never resolves and a Pod stuck ContainerCreating;
	// mounting whole lets tar fail fast and name the missing path.
	volume := volumeNamed(job.Spec.Template.Spec.Volumes, "source-0")
	if volume.ConfigMap == nil || volume.ConfigMap.Name != "secrets-build" {
		t.Fatalf("pipeline source volume = %+v", volume.VolumeSource)
	}
	if len(volume.ConfigMap.Items) != 0 {
		t.Fatalf("source selected keys %+v", volume.ConfigMap.Items)
	}
	if got := job.Spec.Template.Annotations[webhookPayloadAnnotation]; got != string(body) {
		t.Fatalf("payload annotation = %q, want %q", got, body)
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
	got := environmentMap(jobs.Items[0].Spec.Template.Spec.Containers[0].Env)["GITHUB_EVENT"]
	if got != "push" {
		t.Fatalf("GITHUB_EVENT = %q", got)
	}
}

// A pipeline that calls a library names it as a source, and the whole cost of
// that on the receiver's side is one more mount and one more word in
// COALESCE_SOURCES. The order asserted here is the order the list is built in,
// not a contract the script depends on — it cds into COALESCE_PIPELINE, which
// arrives separately. What the order buys is a readable list and a mount whose
// position matches it.
func TestReceiverMountsNamedSourcesInOrder(t *testing.T) {
	handler, client, pipelines := testReceiver(t, 1<<20)
	configMap := pipelineConfigMap(
		"secrets-build",
		"example_inc/secrets.site",
		"pull_request.synchronize",
	)
	configMap.Annotations[pipelineSourcesAnnotation] = "millwright, secrets-build ,toolbelt"
	pipelines.upsert(configMap)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, signedRequest(t, "pull_request",
		validPayload("Example_Inc/Secrets.Site", "synchronize"), testSecret))

	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}
	jobs, err := client.BatchV1().Jobs("coalesce").List(t.Context(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pod := jobs.Items[0].Spec.Template.Spec
	container := pod.Containers[0]
	// The pipeline names itself among its sources; it is still listed once, and
	// still first.
	sources := environmentMap(container.Env)["COALESCE_SOURCES"]
	if sources != "secrets-build millwright toolbelt" {
		t.Fatalf("COALESCE_SOURCES = %q", sources)
	}
	for index, source := range []string{"secrets-build", "millwright", "toolbelt"} {
		volume := fmt.Sprintf("source-%d", index)
		if got := mountPath(container.VolumeMounts, volume); got != benchRoot+"/mnt/"+source {
			t.Errorf("%s mounted at %q", source, got)
		}
		if got := configMapVolume(pod.Volumes, volume); got != source {
			t.Errorf("%s backed by ConfigMap %q", source, got)
		}
	}
}

func TestPipelineCacheRejectsUnusableSourceName(t *testing.T) {
	configMap := pipelineConfigMap(
		"secrets-build",
		"example_inc/secrets.site",
		"push",
	)
	configMap.Annotations[pipelineSourcesAnnotation] = "Not A ConfigMap"

	if _, err := pipelineFromConfigMap(configMap, "example_inc"); err == nil {
		t.Fatal("a source name that cannot resolve to a ConfigMap was accepted")
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

func TestRunSlugIsStructuralAndBounded(t *testing.T) {
	first := runSlug("owner/foo_bar", "pull_request.opened", testNow)
	second := runSlug("owner/foo.bar", "pull_request.opened", testNow)
	if first != second {
		t.Fatalf("sanitization collision was engineered apart: %q != %q", first, second)
	}

	long := runSlug("owner/"+strings.Repeat("a", 100), "pull_request.synchronize", testNow)
	if len(long) > 48 {
		t.Fatalf("%q is %d characters", long, len(long))
	}
	if problems := validation.IsDNS1123Label(long); len(problems) != 0 {
		t.Fatalf("%q is invalid: %v", long, problems)
	}
	wantSuffix := fmt.Sprintf("-pull-request-synchronize-%d", testNow.Unix())
	if !strings.HasSuffix(long, wantSuffix) {
		t.Fatalf("qualified event or epoch was truncated in %q", long)
	}
	childName := long + "-ffffffff-build"
	if len(childName) > 63 {
		t.Fatalf("reserved child name %q is %d characters", childName, len(childName))
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
			archiveKey: []byte("tar bytes"),
		},
	}
}

func mountPath(mounts []corev1.VolumeMount, name string) string {
	for _, mount := range mounts {
		if mount.Name == name {
			return mount.MountPath
		}
	}
	return ""
}

func volumeNamed(volumes []corev1.Volume, name string) corev1.Volume {
	for _, volume := range volumes {
		if volume.Name == name {
			return volume
		}
	}
	return corev1.Volume{}
}

func configMapVolume(volumes []corev1.Volume, name string) string {
	if volume := volumeNamed(volumes, name); volume.ConfigMap != nil {
		return volume.ConfigMap.Name
	}
	return ""
}

func environmentMap(environment []corev1.EnvVar) map[string]string {
	values := make(map[string]string, len(environment))
	for _, variable := range environment {
		values[variable.Name] = variable.Value
	}
	return values
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
		"repository": {"full_name": %q},
		"contributor_input": "$(touch /tmp/not-code)"
	}`, action, repository))
}
