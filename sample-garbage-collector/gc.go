package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"k8s.io/kubernetes/staging/src/k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/kubernetes/staging/src/k8s.io/client-go/metadata"
	"k8s.io/kubernetes/staging/src/k8s.io/client-go/util/workqueue"
)

type workQueueItemAction int

const (
	requeueItem = iota
	forgetItem
)

// GarbageCollector runs reflectors to watch for changes of managed API
// objects, funnels the results to a single-threaded dependencyGraphBuilder,
// which builds a graph caching the dependencies among objects. Triggered by the
// graph changes, the dependencyGraphBuilder enqueues objects that can
// potentially be garbage-collected to the `attemptToDelete` queue, and enqueues
// objects whose dependents need to be orphaned to the `attemptToOrphan` queue.
// The GarbageCollector has workers who consume these two queues, send requests
// to the API server to delete/update the objects accordingly.
// Note that having the dependencyGraphBuilder notify the garbage collector
// ensures that the garbage collector operates with a graph that is at least as
// up to date as the notification is sent.
type GarbageCollector struct {
	restMapper     meta.ResettableRESTMapper
	metadataClient metadata.Interface
	// garbage collector attempts to delete the items in attemptToDelete queue when the time is ripe.
	attemptToDelete workqueue.RateLimitingInterface
	// garbage collector attempts to orphan the dependents of the items in the attemptToOrphan queue, then deletes the items.
	attemptToOrphan        workqueue.RateLimitingInterface
	dependencyGraphBuilder *GraphBuilder
	// GC caches the owners that do not exist according to the API server.
	absentOwnerCache *ReferenceCache

	workerLock sync.RWMutex
}

// Run starts garbage collector workers.
func (gc *GarbageCollector) Run(ctx context.Context, workers int) {
	defer gc.attemptToDelete.ShutDown()
	defer gc.attemptToOrphan.ShutDown()
	defer gc.dependencyGraphBuilder.graphChanges.ShutDown()

	fmt.Printf("Starting garbage collector controller\n")
	defer fmt.Printf("Shutting down garbage collector controller\n")

	go gc.dependencyGraphBuilder.Run(ctx.Done())

	// gc workers
	for i := 0; i < workers; i++ {
		go func() {
		loop:
			for {
				select {
				case <-ctx.Done():
					break loop
				default:
				}

				gc.runAttemptToDeleteWorker(ctx)
				time.Sleep(time.Second)
			}
		}()
	}

	<-ctx.Done()
}

func (gc *GarbageCollector) runAttemptToDeleteWorker(ctx context.Context) {
	for gc.processAttemptToDeleteWorker(ctx) {
	}
}

func (gc *GarbageCollector) processAttemptToDeleteWorker(ctx context.Context) bool {
	item, quit := gc.attemptToDelete.Get()
	gc.workerLock.RLock()
	defer gc.workerLock.RUnlock()
	if quit {
		return false
	}
	defer gc.attemptToDelete.Done(item)

	action := gc.attemptToDeleteWorker(ctx, item)
	switch action {
	case forgetItem:
		gc.attemptToDelete.Forget(item)
	case requeueItem:
		gc.attemptToDelete.Add(item)
	}

	return true
}

func (gc *GarbageCollector) attemptToDeleteWorker(ctx context.Context, item interface{}) workQueueItemAction {
	n, _ := item.(*node)

	// if !n.isObserved() {
	// 	nodeFromGraph, existsInGraph := gc.dependencyGraphBuilder.uidToNode.Read(n.uid)
	// 	if !existsInGraph {
	// 		// this can happen if attemptToDelete loops on a requeued virtual node because attemptToDeleteItem returned an error,
	// 		// and in the meantime a deletion of the real object associated with that uid was observed
	// 		fmt.Printf("item %s no longer in the graph, skipping attemptToDeleteItem\n", n)
	// 		return forgetItem
	// 	}
	// 	if nodeFromGraph.isObserved() {
	// 		// this can happen if attemptToDelete loops on a requeued virtual node because attemptToDeleteItem returned an error,
	// 		// and in the meantime the real object associated with that uid was observed
	// 		fmt.Printf("item %s no longer virtual in the graph, skipping attemptToDeleteItem on virtual node\n", n)
	// 		return forgetItem
	// 	}
	// }

	gc.attemptToDeleteItem(ctx, n)
	// if err == enqueuedVirtualDeleteEventErr {
	// 	// a virtual event was produced and will be handled by processGraphChanges, no need to requeue this node
	// 	return forgetItem
	// } else if err == namespacedOwnerOfClusterScopedObjectErr {
	// 	// a cluster-scoped object referring to a namespaced owner is an error that will not resolve on retry, no need to requeue this node
	// 	return forgetItem
	// } else if !n.isObserved() {
	// 	// requeue if item hasn't been observed via an informer event yet.
	// 	// otherwise a virtual node for an item added AND removed during watch reestablishment can get stuck in the graph and never removed.
	// 	// see https://issue.k8s.io/56121
	// 	klog.V(5).Infof("item %s hasn't been observed via informer yet", n.uid)
	// 	return requeueItem
	// }

	return forgetItem
}

// attemptToDeleteItem looks up the live API object associated with the node,
// and issues a delete IFF the uid matches, the item is not blocked on deleting dependents,
// and all owner references are dangling.
//
// if the API get request returns a NotFound error, or the retrieved item's uid does not match,
// a virtual delete event for the node is enqueued and enqueuedVirtualDeleteEventErr is returned.
func (gc *GarbageCollector) attemptToDeleteItem(ctx context.Context, item *node) error {
	fmt.Printf("Processing object: objectUID: %s\n", item.uid)

	// "being deleted" is an one-way trip to the final deletion. We'll just wait for the final deletion, and then process the object's dependents.
	if item.isBeingDeleted() && !item.isDeletingDependents() {
		fmt.Printf("processing item %s returned at once, because it is being deleted or deleting dependents\n", item.uid)
		return nil
	}
	// TODO: It's only necessary to talk to the API server if this is a
	// "virtual" node. The local graph could lag behind the real status, but in
	// practice, the difference is small.
	// latest, err := gc.getObject(item.uid)
	// switch {
	// case errors.IsNotFound(err):
	// 	// the GraphBuilder can add "virtual" node for an owner that doesn't
	// 	// exist yet, so we need to enqueue a virtual Delete event to remove
	// 	// the virtual node from GraphBuilder.uidToNode.
	// 	klog.V(5).Infof("item %v not found, generating a virtual delete event", item.uid)
	// 	gc.dependencyGraphBuilder.enqueueVirtualDeleteEvent(item.uid)
	// 	return enqueuedVirtualDeleteEventErr
	// case err != nil:
	// 	return err
	// }

	// if latest.GetUID() != item.uid.UID {
	// 	klog.V(5).Infof("UID doesn't match, item %v not found, generating a virtual delete event", item.uid)
	// 	gc.dependencyGraphBuilder.enqueueVirtualDeleteEvent(item.uid)
	// 	return enqueuedVirtualDeleteEventErr
	// }

	// TODO: attemptToOrphanWorker() routine is similar. Consider merging
	// attemptToOrphanWorker() into attemptToDeleteItem() as well.
	if item.isDeletingDependents() {
		return gc.processDeletingDependentsItem(item)
	}

	// compute if we should delete the item
	ownerReferences := item.owners
	if len(ownerReferences) == 0 {
		fmt.Printf("object %s's doesn't have an owner, continue on next item\n", item.uid)
		return nil
	}

	solid, dangling, waitingForDependentsDeletion, err := gc.classifyReferences(ctx, item, ownerReferences)
	if err != nil {
		return err
	}
	fmt.Printf("classify references of %s.\nsolid: %#v\ndangling: %#v\nwaitingForDependentsDeletion: %#v\n", item.uid, solid, dangling, waitingForDependentsDeletion)

	switch {
	case len(solid) != 0:
		fmt.Printf("object %#v has at least one existing owner: %#v, will not garbage collect\n", item.uid, solid)
		if len(dangling) == 0 && len(waitingForDependentsDeletion) == 0 {
			return nil
		}
		fmt.Printf("remove dangling references %#v and waiting references %#v for object %s", dangling, waitingForDependentsDeletion, item.uid)
		// waitingForDependentsDeletion needs to be deleted from the
		// ownerReferences, otherwise the referenced objects will be stuck with
		// the FinalizerDeletingDependents and never get deleted.
		ownerUIDs := append(ownerRefsToUIDs(dangling), ownerRefsToUIDs(waitingForDependentsDeletion)...)
		p, err := c.GenerateDeleteOwnerRefStrategicMergeBytes(item.uid.UID, ownerUIDs)
		if err != nil {
			return err
		}
		_, err = gc.patch(item, p, func(n *node) ([]byte, error) {
			return gc.deleteOwnerRefJSONMergePatch(n, ownerUIDs...)
		})
		return err
	case len(waitingForDependentsDeletion) != 0 && item.dependentsLength() != 0:
		deps := item.getDependents()
		for _, dep := range deps {
			if dep.isDeletingDependents() {
				// this circle detection has false positives, we need to
				// apply a more rigorous detection if this turns out to be a
				// problem.
				// there are multiple workers run attemptToDeleteItem in
				// parallel, the circle detection can fail in a race condition.
				klog.V(2).Infof("processing object %s, some of its owners and its dependent [%s] have FinalizerDeletingDependents, to prevent potential cycle, its ownerReferences are going to be modified to be non-blocking, then the object is going to be deleted with Foreground", item.uid, dep.identity)
				patch, err := item.unblockOwnerReferencesStrategicMergePatch()
				if err != nil {
					return err
				}
				if _, err := gc.patch(item, patch, gc.unblockOwnerReferencesJSONMergePatch); err != nil {
					return err
				}
				break
			}
		}
		klog.V(2).Infof("at least one owner of object %s has FinalizerDeletingDependents, and the object itself has dependents, so it is going to be deleted in Foreground", item.uid)
		// the deletion event will be observed by the graphBuilder, so the item
		// will be processed again in processDeletingDependentsItem. If it
		// doesn't have dependents, the function will remove the
		// FinalizerDeletingDependents from the item, resulting in the final
		// deletion of the item.
		policy := metav1.DeletePropagationForeground
		return gc.deleteObject(item.uid, &policy)
	default:
		// item doesn't have any solid owner, so it needs to be garbage
		// collected. Also, none of item's owners is waiting for the deletion of
		// the dependents, so set propagationPolicy based on existing finalizers.
		var policy metav1.DeletionPropagation
		switch {
		case hasOrphanFinalizer(latest):
			// if an existing orphan finalizer is already on the object, honor it.
			policy = metav1.DeletePropagationOrphan
		case hasDeleteDependentsFinalizer(latest):
			// if an existing foreground finalizer is already on the object, honor it.
			policy = metav1.DeletePropagationForeground
		default:
			// otherwise, default to background.
			policy = metav1.DeletePropagationBackground
		}
		klog.V(2).InfoS("Deleting object", "object", klog.KRef(item.uid.Namespace, item.uid.Name),
			"objectUID", item.uid.UID, "kind", item.uid.Kind, "propagationPolicy", policy)
		return gc.deleteObject(item.uid, &policy)
	}
}

// classify the latestReferences to three categories:
// solid: the owner exists, and is not "waitingForDependentsDeletion"
// dangling: the owner does not exist
// waitingForDependentsDeletion: the owner exists, its deletionTimestamp is non-nil, and it has
// FinalizerDeletingDependents
// This function communicates with the server.
func (gc *GarbageCollector) classifyReferences(ctx context.Context, item *node, latestReferences []owner) (
	solid, dangling, waitingForDependentsDeletion []owner, err error) {
	for _, reference := range latestReferences {
		isDangling, owner, err := gc.isDangling(ctx, reference, item)
		if err != nil {
			return nil, nil, nil, err
		}
		if isDangling {
			dangling = append(dangling, reference)
			continue
		}

		if beingDeleted(owner) && hasDeleteDependentsFinalizer(owner) {
			waitingForDependentsDeletion = append(waitingForDependentsDeletion, reference)
		} else {
			solid = append(solid, reference)
		}
	}
	return solid, dangling, waitingForDependentsDeletion, nil
}

func hasDeleteDependentsFinalizer(accessor interface{}) bool {
	return hasFinalizer(accessor, FinalizerDeleteDependents)
}

// isDangling check if a reference is pointing to an object that doesn't exist.
// If isDangling looks up the referenced object at the API server, it also
// returns its latest state.
func (gc *GarbageCollector) isDangling(ctx context.Context, reference owner, item *node) (
	dangling bool, ow *owner, err error) {

	dangling = true
	ow = &owner{}
	err = nil

	return dangling, ow, err
}
