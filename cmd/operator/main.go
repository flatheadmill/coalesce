// The operator produces the same ConfigMap as coalesce make from readable
// cluster resources. It does not dispatch pipelines or interpret Pod templates.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	namespace := os.Getenv("WATCH_NAMESPACE")
	if namespace == "" {
		namespace = "coalesce"
	}
	config, err := rest.InClusterConfig()
	if err != nil {
		return err
	}
	sources, err := dynamic.NewForConfig(config)
	if err != nil {
		return err
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}
	broadcaster := record.NewBroadcaster()
	defer broadcaster.Shutdown()
	broadcaster.StartRecordingToSink(&coreclient.EventSinkImpl{Interface: clientset.CoreV1().Events(namespace)})
	recorder := broadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: "coalesce-operator"})
	c := newController(namespace, sources.Resource(coalesceResource).Namespace(namespace), clientset.CoreV1().ConfigMaps(namespace), recorder)
	defer c.queue.ShutDown()
	custom := dynamicinformer.NewFilteredDynamicSharedInformerFactory(sources, 0, namespace, nil)
	core := informers.NewSharedInformerFactoryWithOptions(clientset, 0, informers.WithNamespace(namespace))
	resourceInformer := custom.ForResource(coalesceResource).Informer()
	configMapInformer := core.Core().V1().ConfigMaps().Informer()
	for _, informer := range []cache.SharedIndexInformer{resourceInformer, configMapInformer} {
		if _, err := informer.AddEventHandler(c.handlers()); err != nil {
			return err
		}
	}
	custom.Start(ctx.Done())
	core.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), resourceInformer.HasSynced, configMapInformer.HasSynced) {
		return ctx.Err()
	}
	log.Printf("Coalesce operator watching namespace %s", namespace)
	c.run(ctx)
	return nil
}
