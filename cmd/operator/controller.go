package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
)

type controller struct {
	namespace  string
	sources    dynamic.ResourceInterface
	configMaps coreclient.ConfigMapInterface
	recorder   record.EventRecorder
	queue      workqueue.RateLimitingInterface
}

func newController(namespace string, sources dynamic.ResourceInterface, configMaps coreclient.ConfigMapInterface, recorder record.EventRecorder) *controller {
	return &controller{
		namespace: namespace, sources: sources, configMaps: configMaps, recorder: recorder,
		queue: workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "coalesce"),
	}
}

// Both informers use these handlers. In particular, do not filter ConfigMaps
// by owner: deleting a direct, conflicting ConfigMap must wake the same key.
func (c *controller) handlers() cache.ResourceEventHandlerFuncs {
	enqueue := func(object interface{}) {
		key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(object)
		if err != nil {
			slog.Error("Cannot queue object", "error", err)
			return
		}
		c.queue.Add(key)
	}
	return cache.ResourceEventHandlerFuncs{
		AddFunc: enqueue, UpdateFunc: func(_, current interface{}) { enqueue(current) }, DeleteFunc: enqueue,
	}
}

func (c *controller) run(ctx context.Context) {
	go func() {
		<-ctx.Done()
		c.queue.ShutDown()
	}()
	for c.processNext(ctx) {
	}
}

func (c *controller) processNext(ctx context.Context) bool {
	item, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(item)
	key := item.(string)
	request, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := c.reconcile(request, key); err != nil && ctx.Err() == nil {
		slog.Error("Retrying Coalesce render", "resource", key, "error", err)
		c.queue.AddRateLimited(key)
	} else {
		c.queue.Forget(key)
	}
	return true
}

func (c *controller) reconcile(ctx context.Context, key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != c.namespace {
		return nil
	}
	// Read the current source for every attempt, including retries. Queue keys
	// are names, not snapshots or promises to apply an earlier generation.
	source, err := c.sources.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if source.GetDeletionTimestamp() != nil {
		return nil
	}
	desired, err := renderConfigMap(source)
	if err != nil {
		c.reject(source, "InvalidSource", err)
		return nil
	}
	current, err := c.configMaps.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = c.configMaps.Create(ctx, desired, metav1.CreateOptions{})
		return c.writeResult(source, err)
	}
	if err != nil {
		return err
	}
	owner := metav1.GetControllerOf(current)
	if owner == nil || owner.UID != source.GetUID() || owner.Name != source.GetName() ||
		owner.APIVersion != coalesceResource.GroupVersion().String() || owner.Kind != coalesceKind {
		c.reject(source, "OwnershipConflict", fmt.Errorf("ConfigMap %s/%s is not controlled by this Coalesce UID; it will not be adopted", namespace, name))
		return nil
	}
	if current.DeletionTimestamp != nil {
		// Its eventual delete event will wake us. Updating a terminating child
		// cannot cancel deletion and would only race garbage collection.
		return nil
	}
	updated := current.DeepCopy()
	updated.Labels = desired.Labels
	updated.Annotations = desired.Annotations
	updated.OwnerReferences = desired.OwnerReferences
	updated.Data = desired.Data
	updated.BinaryData = desired.BinaryData
	if equality.Semantic.DeepEqual(current, updated) {
		return nil
	}
	// Preserve resourceVersion and other server-owned fields; a concurrent
	// change returns Conflict and is reconciled from fresh state on retry.
	_, err = c.configMaps.Update(ctx, updated, metav1.UpdateOptions{})
	return c.writeResult(source, err)
}

func (c *controller) writeResult(source *unstructured.Unstructured, err error) error {
	if apierrors.IsInvalid(err) || apierrors.IsBadRequest(err) || apierrors.IsRequestEntityTooLargeError(err) {
		c.reject(source, "ArtifactRejected", err)
		return nil
	}
	return err
}

func (c *controller) reject(source *unstructured.Unstructured, reason string, err error) {
	message := fmt.Sprintf("Generation %d was not rendered: %v. The existing ConfigMap, if any, was left unchanged.", source.GetGeneration(), err)
	slog.Error("Coalesce render rejected", "resource", source.GetNamespace()+"/"+source.GetName(), "reason", reason, "error", message)
	c.recorder.Event(source, corev1.EventTypeWarning, reason, message)
}
