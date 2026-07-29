package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes/fake"
)

const testSecret = "correct horse battery staple"

func TestReceiverCreatesRunnerJob(t *testing.T) {
	handler, client := testReceiver(t, 1<<20)
	request := signedRequest(t, "pull_request", validPayload("Example_Inc/Secrets.Site"), testSecret)
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
	if problems := validation.IsDNS1123Label(job.Name); len(problems) != 0 {
		t.Fatalf("Job name %q is invalid: %v", job.Name, problems)
	}
	if len(job.Name) > 63 {
		t.Fatalf("Job name is %d characters", len(job.Name))
	}
	if job.Annotations["flatheadmill.github.io/delivery"] != "delivery-123" {
		t.Fatalf("delivery annotation = %q", job.Annotations["flatheadmill.github.io/delivery"])
	}
	if job.Spec.Template.Spec.ServiceAccountName != "coalesce-runner" {
		t.Fatalf("service account = %q", job.Spec.Template.Spec.ServiceAccountName)
	}

	container := job.Spec.Template.Spec.Containers[0]
	env := make(map[string]string)
	for _, variable := range container.Env {
		env[variable.Name] = variable.Value
	}
	want := map[string]string{
		"GITHUB_ACTION":     "synchronize",
		"GITHUB_REPOSITORY": "Example_Inc/Secrets.Site",
		"GITHUB_PR_NUMBER":  "42",
		"GITHUB_BASE_REF":   "main",
		"GITHUB_BASE_SHA":   strings.Repeat("a", 40),
		"GITHUB_HEAD_REF":   "Topic/Build_It",
		"GITHUB_HEAD_SHA":   strings.Repeat("b", 40),
	}
	for name, value := range want {
		if env[name] != value {
			t.Errorf("%s = %q, want %q", name, env[name], value)
		}
	}
	if got := container.Args[len(container.Args)-1]; got != "/run/coalesce/pipelines/build.coalesce.zsh" {
		t.Fatalf("pipeline argument = %q", got)
	}
}

func TestReceiverTreatsRedeliveryAsSuccess(t *testing.T) {
	handler, _ := testReceiver(t, 1<<20)
	body := validPayload("Example_Inc/Secrets.Site")

	first := httptest.NewRecorder()
	handler.ServeHTTP(first, signedRequest(t, "pull_request", body, testSecret))
	if first.Code != http.StatusCreated {
		t.Fatalf("first status = %d", first.Code)
	}

	second := httptest.NewRecorder()
	handler.ServeHTTP(second, signedRequest(t, "pull_request", body, testSecret))
	if second.Code != http.StatusOK {
		t.Fatalf("redelivery status = %d, body = %s", second.Code, second.Body)
	}
	if !strings.Contains(second.Body.String(), `"status":"exists"`) {
		t.Fatalf("redelivery body = %s", second.Body)
	}
}

func TestReceiverAnswersPingWithoutCreatingJob(t *testing.T) {
	handler, client := testReceiver(t, 1<<20)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, signedRequest(t, "ping", []byte(`{"zen":"keep it logically awesome"}`), testSecret))

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

func TestReceiverIgnoresNonBuildPullRequestActions(t *testing.T) {
	handler, client := testReceiver(t, 1<<20)
	body := validPayloadWith(t, "Example_Inc/Secrets.Site", func(payload map[string]any) {
		payload["action"] = "closed"
	})
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, signedRequest(t, "pull_request", body, testSecret))

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
		t.Fatalf("ignored action created %d Jobs", len(jobs.Items))
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
			body:     validPayload("Example_Inc/Secrets.Site"),
			secret:   "wrong",
			method:   http.MethodPost,
			want:     http.StatusUnauthorized,
			maxBytes: 1 << 20,
		},
		{
			name:     "oversized body",
			event:    "pull_request",
			body:     validPayload("Example_Inc/Secrets.Site"),
			secret:   testSecret,
			method:   http.MethodPost,
			want:     http.StatusRequestEntityTooLarge,
			maxBytes: 32,
		},
		{
			name:     "unsupported event",
			event:    "push",
			body:     []byte(`{}`),
			secret:   testSecret,
			method:   http.MethodPost,
			want:     http.StatusUnprocessableEntity,
			maxBytes: 1 << 20,
		},
		{
			name:       "missing delivery ID",
			event:      "pull_request",
			body:       validPayload("Example_Inc/Secrets.Site"),
			secret:     testSecret,
			method:     http.MethodPost,
			want:       http.StatusBadRequest,
			maxBytes:   1 << 20,
			noDelivery: true,
		},
		{
			name:     "repository not allowed",
			event:    "pull_request",
			body:     validPayload("someone/else"),
			secret:   testSecret,
			method:   http.MethodPost,
			want:     http.StatusForbidden,
			maxBytes: 1 << 20,
		},
		{
			name:     "wrong method",
			event:    "pull_request",
			body:     validPayload("Example_Inc/Secrets.Site"),
			secret:   testSecret,
			method:   http.MethodGet,
			want:     http.StatusMethodNotAllowed,
			maxBytes: 1 << 20,
		},
		{
			name:  "mismatched pull request number",
			event: "pull_request",
			body: validPayloadWith(t, "Example_Inc/Secrets.Site", func(payload map[string]any) {
				payload["number"] = float64(41)
			}),
			secret:   testSecret,
			method:   http.MethodPost,
			want:     http.StatusUnprocessableEntity,
			maxBytes: 1 << 20,
		},
		{
			name:  "invalid head SHA",
			event: "pull_request",
			body: validPayloadWith(t, "Example_Inc/Secrets.Site", func(payload map[string]any) {
				pullRequest := payload["pull_request"].(map[string]any)
				head := pullRequest["head"].(map[string]any)
				head["sha"] = "not-a-sha"
			}),
			secret:   testSecret,
			method:   http.MethodPost,
			want:     http.StatusUnprocessableEntity,
			maxBytes: 1 << 20,
		},
		{
			name:  "invalid Git ref",
			event: "pull_request",
			body: validPayloadWith(t, "Example_Inc/Secrets.Site", func(payload map[string]any) {
				pullRequest := payload["pull_request"].(map[string]any)
				head := pullRequest["head"].(map[string]any)
				head["ref"] = "-looks-like-an-option"
			}),
			secret:   testSecret,
			method:   http.MethodPost,
			want:     http.StatusUnprocessableEntity,
			maxBytes: 1 << 20,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler, client := testReceiver(t, test.maxBytes)
			request := signedRequest(t, test.event, test.body, test.secret)
			request.Method = test.method
			if test.noDelivery {
				request.Header.Del("X-GitHub-Delivery")
			}
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, request)

			if response.Code != test.want {
				t.Fatalf("status = %d, want %d, body = %s", response.Code, test.want, response.Body)
			}
			jobs, err := client.BatchV1().Jobs("coalesce").List(t.Context(), metav1.ListOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if len(jobs.Items) != 0 {
				t.Fatalf("rejected request created %d Jobs", len(jobs.Items))
			}
		})
	}
}

func TestJobNameSeparatesSanitizationCollisions(t *testing.T) {
	head := strings.Repeat("c", 40)
	first := jobName("owner/foo_bar", 7, head)
	second := jobName("owner/foo.bar", 7, head)

	if first == second {
		t.Fatalf("colliding repositories produced %q", first)
	}
	for _, name := range []string{first, second} {
		if len(name) > 63 {
			t.Errorf("%q is %d characters", name, len(name))
		}
		if problems := validation.IsDNS1123Label(name); len(problems) != 0 {
			t.Errorf("%q is invalid: %v", name, problems)
		}
	}
}

func testReceiver(t *testing.T, maxBodyBytes int64) (*receiver, *fake.Clientset) {
	t.Helper()
	client := fake.NewSimpleClientset()
	handler, err := newReceiver(client.BatchV1().Jobs("coalesce"), receiverConfig{
		Secret: []byte(testSecret),
		Allowed: map[string]struct{}{
			"example_inc/secrets.site": {},
		},
		Namespace:         "coalesce",
		RunnerImage:       "example.invalid/coalesce-runner:test",
		PipelineConfigMap: "coalesce-pipeline-secrets",
		PipelineFile:      "build.coalesce.zsh",
		CoalesceURL:       "http://coalesce.coalesce.svc.cluster.local",
		MaxBodyBytes:      maxBodyBytes,
		APITimeout:        time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return handler, client
}

func signedRequest(t *testing.T, event string, body []byte, secret string) *http.Request {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, webhookPath, strings.NewReader(string(body)))
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

func validPayload(repository string) []byte {
	return []byte(fmt.Sprintf(`{
		"action": "synchronize",
		"number": 42,
		"repository": {"full_name": %q},
		"pull_request": {
			"number": 42,
			"base": {"ref": "main", "sha": %q},
			"head": {"ref": "Topic/Build_It", "sha": %q}
		}
	}`, repository, strings.Repeat("a", 40), strings.Repeat("b", 40)))
}

func validPayloadWith(t *testing.T, repository string, change func(map[string]any)) []byte {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(validPayload(repository), &payload); err != nil {
		t.Fatal(err)
	}
	change(payload)
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return body
}
