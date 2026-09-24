package main

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apimachinery/pkg/util/wait"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
)

func testController(t *testing.T, source *unstructured.Unstructured, existing ...runtime.Object) (*controller, *kubernetesfake.Clientset, *record.FakeRecorder) {
	t.Helper()
	var objects []runtime.Object
	if source != nil {
		objects = append(objects, source)
	}
	dynamic := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), objects...)
	core := kubernetesfake.NewSimpleClientset(existing...)
	recorder := record.NewFakeRecorder(20)
	c := newController("coalesce", dynamic.Resource(coalesceResource).Namespace("coalesce"), core.CoreV1().ConfigMaps("coalesce"), recorder)
	t.Cleanup(c.queue.ShutDown)
	return c, core, recorder
}

func reconcile(t *testing.T, c *controller) {
	t.Helper()
	if err := c.reconcile(context.Background(), "coalesce/hello"); err != nil {
		t.Fatal(err)
	}
}

func currentConfigMap(t *testing.T, c *controller) *corev1.ConfigMap {
	t.Helper()
	cm, err := c.configMaps.Get(context.Background(), "hello", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return cm
}

func updateSource(t *testing.T, c *controller, source *unstructured.Unstructured) {
	t.Helper()
	if _, err := c.sources.Update(context.Background(), source, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func warning(t *testing.T, recorder *record.FakeRecorder, reason string) {
	t.Helper()
	select {
	case event := <-recorder.Events:
		if !strings.HasPrefix(event, "Warning "+reason+" ") || !strings.Contains(event, "left unchanged") {
			t.Fatalf("event = %q", event)
		}
	default:
		t.Fatalf("no %s Warning", reason)
	}
}

func TestReconcileCreatesRepairsAndUpdatesArtifact(t *testing.T) {
	source := testSource(sourceFile{"run", true, "first\n"})
	spec := source.Object["spec"].(map[string]interface{})
	spec["template"] = map[string]interface{}{"spec": map[string]interface{}{"futurePodField": true}}
	spec["configMapMetadata"] = map[string]interface{}{"labels": map[string]interface{}{"old": "label"}}
	c, core, _ := testController(t, source)
	reconcile(t, c)
	first := currentConfigMap(t, c)
	if first.Data[templateKey] == "" || first.Annotations[renderedGeneration] != "1" {
		t.Fatalf("pipeline = %#v", first)
	}
	core.ClearActions()
	reconcile(t, c)
	for _, action := range core.Actions() {
		if action.GetVerb() != "get" {
			t.Fatalf("unchanged reconciliation wrote: %#v", action)
		}
	}

	// A child update event repairs its data and metadata, using the current
	// object's resourceVersion rather than manufacturing a new object.
	damaged := first.DeepCopy()
	damaged.ResourceVersion = "17"
	damaged.Data[templateKey] = "broken"
	damaged.BinaryData = map[string][]byte{"unexpected": []byte("bad")}
	damaged.Labels = map[string]string{"stray": "label"}
	damaged.Annotations = map[string]string{"stray": "annotation"}
	if _, err := c.configMaps.Update(context.Background(), damaged, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	c.handlers().UpdateFunc(first, damaged)
	c.processNext(context.Background())
	repaired := currentConfigMap(t, c)
	first.ResourceVersion = "17"
	if !reflect.DeepEqual(repaired, first) {
		t.Fatalf("repair = %#v; want %#v", repaired, first)
	}

	// A library no longer carries a template or labels removed from its spec.
	source.SetGeneration(2)
	spec["files"] = []interface{}{map[string]interface{}{"path": "run", "body": "second"}}
	delete(spec, "template")
	delete(spec, "configMapMetadata")
	updateSource(t, c, source)
	c.handlers().UpdateFunc(nil, source)
	c.processNext(context.Background())
	second := currentConfigMap(t, c)
	if len(second.Data) != 0 || len(second.Labels) != 0 || second.Annotations[renderedGeneration] != "2" ||
		unpack(t, second.BinaryData[archiveKey])["run"] != (extractedFile{"second", 0644}) {
		t.Fatalf("updated library = %#v", second)
	}

	if err := c.configMaps.Delete(context.Background(), "hello", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	c.handlers().DeleteFunc(cache.DeletedFinalStateUnknown{Key: "coalesce/hello", Obj: second})
	c.processNext(context.Background())
	recreated := currentConfigMap(t, c)
	if recreated.Annotations[renderedGeneration] != "2" || !reflect.DeepEqual(recreated.BinaryData, second.BinaryData) {
		t.Fatal("deleted child was not rebuilt from the current source")
	}

	// Changes to authored metadata are rendering inputs too.
	source.SetGeneration(3)
	spec["configMapMetadata"] = map[string]interface{}{"annotations": map[string]interface{}{"example/note": "changed"}}
	updateSource(t, c, source)
	c.handlers().UpdateFunc(nil, source)
	c.processNext(context.Background())
	if currentConfigMap(t, c).Annotations["example/note"] != "changed" {
		t.Fatal("metadata edit was missed")
	}
}

func TestOwnershipCollisionIsNotAdoptedAndDeletionWakesSource(t *testing.T) {
	for _, kind := range []string{"direct", "old UID", "different controller", "noncontroller"} {
		t.Run(kind, func(t *testing.T) {
			source := testSource(sourceFile{path: "run", body: "source"})
			existing, err := renderConfigMap(source)
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "direct":
				existing.OwnerReferences = nil
			case "old UID":
				existing.OwnerReferences[0].UID = "previous-source-uid"
			case "different controller":
				existing.OwnerReferences[0].Kind = "Other"
			case "noncontroller":
				*existing.OwnerReferences[0].Controller = false
			}
			c, _, recorder := testController(t, source, existing)
			c.queue.Add("coalesce/hello")
			c.processNext(context.Background())
			if c.queue.NumRequeues("coalesce/hello") != 0 || !reflect.DeepEqual(currentConfigMap(t, c), existing) {
				t.Fatal("collision was retried or changed")
			}
			warning(t, recorder, "OwnershipConflict")
			if err := c.configMaps.Delete(context.Background(), "hello", metav1.DeleteOptions{}); err != nil {
				t.Fatal(err)
			}
			c.handlers().DeleteFunc(existing)
			c.processNext(context.Background())
			owner := metav1.GetControllerOf(currentConfigMap(t, c))
			if owner == nil || owner.UID != source.GetUID() || owner.Kind != coalesceKind {
				t.Fatal("removing collision did not permit creation")
			}
		})
	}
}

func TestInvalidEditKeepsLastArtifactAndGeneration(t *testing.T) {
	source := testSource(sourceFile{path: "run", body: "good"})
	c, _, recorder := testController(t, source)
	reconcile(t, c)
	lastValid := currentConfigMap(t, c)
	source.SetGeneration(2)
	source.Object["spec"].(map[string]interface{})["files"] = []interface{}{map[string]interface{}{"path": "../escape", "body": "bad"}}
	updateSource(t, c, source)
	c.queue.Add("coalesce/hello")
	c.processNext(context.Background())
	if c.queue.NumRequeues("coalesce/hello") != 0 || !reflect.DeepEqual(currentConfigMap(t, c), lastValid) {
		t.Fatal("invalid input retried or replaced the last valid artifact")
	}
	warning(t, recorder, "InvalidSource")

	source = testSource(sourceFile{path: "run", body: "fixed"})
	source.SetGeneration(3)
	updateSource(t, c, source)
	c.handlers().UpdateFunc(nil, source)
	c.processNext(context.Background())
	if currentConfigMap(t, c).Annotations[renderedGeneration] != "3" {
		t.Fatal("corrected input stayed stuck after rejection")
	}
}

func TestTransientWritesRetryFromFreshState(t *testing.T) {
	for _, verb := range []string{"create", "update"} {
		t.Run(verb, func(t *testing.T) {
			source := testSource(sourceFile{path: "run", body: "first"})
			var existing []runtime.Object
			if verb == "update" {
				cm, err := renderConfigMap(source)
				if err != nil {
					t.Fatal(err)
				}
				existing = append(existing, cm)
				source.SetGeneration(2)
			}
			c, core, _ := testController(t, source, existing...)
			failed := false
			core.PrependReactor(verb, "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
				if !failed {
					failed = true
					if verb == "update" {
						return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, "hello", nil)
					}
					return true, nil, apierrors.NewServiceUnavailable("temporarily unavailable")
				}
				return false, nil, nil
			})
			c.queue.Add("coalesce/hello")
			c.processNext(context.Background())
			if c.queue.NumRequeues("coalesce/hello") != 1 {
				t.Fatal("transient error was not rate limited")
			}
			newer := testSource(sourceFile{path: "run", body: "newest"})
			newer.SetGeneration(9)
			updateSource(t, c, newer)
			if err := wait.PollUntilContextTimeout(context.Background(), time.Millisecond, time.Second, true,
				func(context.Context) (bool, error) { return c.queue.Len() > 0, nil }); err != nil {
				t.Fatal("retry was not queued:", err)
			}
			c.processNext(context.Background())
			cm := currentConfigMap(t, c)
			if cm.Annotations[renderedGeneration] != "9" || unpack(t, cm.BinaryData[archiveKey])["run"].body != "newest" ||
				c.queue.NumRequeues("coalesce/hello") != 0 {
				t.Fatal("retry did not converge on the newest input")
			}
		})
	}
}

func TestAPIRejectionDoesNotRetryOrReplaceArtifact(t *testing.T) {
	source := testSource(sourceFile{path: "run", body: "good"})
	existing, err := renderConfigMap(source)
	if err != nil {
		t.Fatal(err)
	}
	source.SetGeneration(2)
	c, core, recorder := testController(t, source, existing)
	core.PrependReactor("update", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewInvalid(schema.GroupKind{Kind: "ConfigMap"}, "hello",
			field.ErrorList{field.Invalid(field.NewPath("metadata", "labels"), "bad", "invalid label")})
	})
	c.queue.Add("coalesce/hello")
	c.processNext(context.Background())
	if c.queue.NumRequeues("coalesce/hello") != 0 || !reflect.DeepEqual(currentConfigMap(t, c), existing) {
		t.Fatal("API rejection retried or changed the last artifact")
	}
	warning(t, recorder, "ArtifactRejected")
}

func TestMissingAndDeletingSourcesLeaveConfigMapsAlone(t *testing.T) {
	for _, deleting := range []bool{false, true} {
		var source *unstructured.Unstructured
		if deleting {
			source = testSource()
			now := metav1.Now()
			source.SetDeletionTimestamp(&now)
		}
		direct := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "hello", Namespace: "coalesce"}, Data: map[string]string{"keep": "me"}}
		c, core, _ := testController(t, source, direct)
		c.handlers().AddFunc(direct)
		c.processNext(context.Background())
		if len(core.Actions()) != 0 {
			t.Fatal("missing/deleting source touched ConfigMaps")
		}
	}
}
