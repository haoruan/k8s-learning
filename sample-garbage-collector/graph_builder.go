package main

import (
	"fmt"
	"sync"
	"time"

	"k8s.io/kubernetes/staging/src/k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/kubernetes/staging/src/k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/kubernetes/staging/src/k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/kubernetes/staging/src/k8s.io/client-go/util/workqueue"
	"k8s.io/kubernetes/staging/src/k8s.io/controller-manager/pkg/informerfactory"
)

const (
	FinalizerOrphanDependents = "orphan"
	FinalizerDeleteDependents = "foregroundDeletion"
)

// GraphBuilder processes events supplied by the informers, updates uidToNode,
// a graph that caches the dependencies as we know, and enqueues
// items to the attemptToDelete and attemptToOrphan.
type GraphBuilder struct {
	monitorLock sync.RWMutex

	informersStarted <-chan struct{}

	// stopCh drives shutdown. When a receive from it unblocks, monitors will shut down.
	// This channel is also protected by monitorLock.
	stopCh <-chan struct{}

	// running tracks whether Run() has been called.
	// it is protected by monitorLock.
	running bool

	// monitors are the producer of the graphChanges queue, graphBuilder alters
	// the in-memory graph according to the changes.
	graphChanges workqueue.RateLimitingInterface
	// uidToNode doesn't require a lock to protect, because only the
	// single-threaded GraphBuilder.processGraphChanges() reads/writes it.
	uidToNode *concurrentUIDToNode
	// GraphBuilder is the producer of attemptToDelete and attemptToOrphan, GC is the consumer.
	attemptToDelete workqueue.RateLimitingInterface
	attemptToOrphan workqueue.RateLimitingInterface
	// GraphBuilder and GC share the absentOwnerCache. Objects that are known to
	// be non-existent are added to the cached.
	absentOwnerCache *ReferenceCache
	sharedInformers  informerfactory.InformerFactory
	ignoredResources map[schema.GroupResource]struct{}
}

// Run sets the stop channel and starts monitor execution until stopCh is
// closed. Any running monitors will be stopped before Run returns.
func (gb *GraphBuilder) Run(stopCh <-chan struct{}) {
	fmt.Printf("GraphBuilder running\n")
	defer fmt.Printf("GraphBuilder stopping\n")

	// Set up the stop channel.
	gb.monitorLock.Lock()
	gb.stopCh = stopCh
	gb.running = true
	gb.monitorLock.Unlock()

	// Start monitors and begin change processing until the stop channel is
	// closed.
	gb.startMonitors()
	wait.Until(gb.runProcessGraphChanges, 1*time.Second, stopCh)

	// Stop any running monitors.
	gb.monitorLock.Lock()
	defer gb.monitorLock.Unlock()
	monitors := gb.monitors
	stopped := 0
	for _, monitor := range monitors {
		if monitor.stopCh != nil {
			stopped++
			close(monitor.stopCh)
		}
	}

	// reset monitors so that the graph builder can be safely re-run/synced.
	gb.monitors = nil
	fmt.Printf("stopped %d of %d monitors", stopped, len(monitors))
}

func (gb *GraphBuilder) runProcessGraphChanges() {
	for gb.processGraphChanges() {
	}
}

// Dequeueing an event from graphChanges, updating graph, populating dirty_queue.
func (gb *GraphBuilder) processGraphChanges() bool {
	// Get item from queue
	existingNode, found := gb.uidToNode.Read(accessor.GetUID())
	if found && !event.virtual && !existingNode.isObserved() {
		// this marks the node as having been observed via an informer event
		// 1. this depends on graphChanges only containing add/update events from the actual informer
		// 2. this allows things tracking virtual nodes' existence to stop polling and rely on informer events
		observedIdentity := identityFromEvent(event, accessor)
		if observedIdentity != existingNode.identity {
			// find dependents that don't match the identity we observed
			_, potentiallyInvalidDependents := partitionDependents(existingNode.getDependents(), observedIdentity)
			// add those potentially invalid dependents to the attemptToDelete queue.
			// if their owners are still solid the attemptToDelete will be a no-op.
			// this covers the bad child -> good parent observation sequence.
			// the good parent -> bad child observation sequence is handled in addDependentToOwners
			for _, dep := range potentiallyInvalidDependents {
				if len(observedIdentity.Namespace) > 0 && dep.identity.Namespace != observedIdentity.Namespace {
					// Namespace mismatch, this is definitely wrong
					fmt.Printf("node %s references an owner %s but does not match namespaces\n", dep.identity, observedIdentity)
					gb.reportInvalidNamespaceOwnerRef(dep, observedIdentity.UID)
				}
				gb.attemptToDelete.Add(dep)
			}

			// make a copy (so we don't modify the existing node in place), store the observed identity, and replace the virtual node
			fmt.Printf("replacing virtual node %s with observed node %s\n", existingNode.identity, observedIdentity)
			existingNode = existingNode.clone()
			existingNode.identity = observedIdentity
			gb.uidToNode.Write(existingNode)
		}
		existingNode.markObserved()
	}

	switch {
	case (event.eventType == addEvent || event.eventType == updateEvent) && !found:
		newNode := &node{}
		gb.insertNode(newNode)
		// the underlying delta_fifo may combine a creation and a deletion into
		// one event, so we need to further process the event.
		gb.processTransitions(event.oldObj, accessor, newNode)
	case (event.eventType == addEvent || event.eventType == updateEvent) && found:
		added, removed, changed := referencesDiffs(existingNode.owners, accessor.GetOwnerReferences())
		if len(added) != 0 || len(removed) != 0 || len(changed) != 0 {
			// check if the changed dependency graph unblock owners that are
			// waiting for the deletion of their dependents.
			gb.addUnblockedOwnersToDeleteQueue(removed, changed)
			// update the node itself
			existingNode.owners = accessor.GetOwnerReferences()
			// Add the node to its new owners' dependent lists.
			gb.addDependentToOwners(existingNode, added)
			// remove the node from the dependent list of node that are no longer in
			// the node's owners list.
			gb.removeDependentFromOwners(existingNode, removed)
		}

		if beingDeleted(accessor) {
			existingNode.markBeingDeleted()
		}
		gb.processTransitions(event.oldObj, accessor, existingNode)
	case event.eventType == deleteEvent:
		if !found {
			fmt.Printf("%v doesn't exist in the graph, this shouldn't happen\n", accessor.GetUID())
			return true
		}

		// removeNode updates the graph
		gb.removeNode(existingNode)
		existingNode.dependentsLock.RLock()
		defer existingNode.dependentsLock.RUnlock()
		if len(existingNode.dependents) > 0 {
			gb.absentOwnerCache.Add(identityFromEvent(event, accessor))
		}
		for dep := range existingNode.dependents {
			gb.attemptToDelete.Add(dep)
		}
		for _, owner := range existingNode.owners {
			ownerNode, found := gb.uidToNode.Read(owner.UID)
			if !found || !ownerNode.isDeletingDependents() {
				continue
			}
			// this is to let attempToDeleteItem check if all the owner's
			// dependents are deleted, if so, the owner will be deleted.
			gb.attemptToDelete.Add(ownerNode)
		}
	}

	return true
}

// insertNode insert the node to gb.uidToNode; then it finds all owners as listed
// in n.owners, and adds the node to their dependents list.
func (gb *GraphBuilder) insertNode(n *node) {
	gb.uidToNode.Write(n)
	gb.addDependentToOwners(n, n.owners)
}

// addDependentToOwners adds n to owners' dependents list. If the owner does not
// exist in the gb.uidToNode yet, a "virtual" node will be created to represent
// the owner. The "virtual" node will be enqueued to the attemptToDelete, so that
// attemptToDeleteItem() will verify if the owner exists according to the API server.
func (gb *GraphBuilder) addDependentToOwners(n *node, owners []metav1.OwnerReference) {
	// track if some of the referenced owners already exist in the graph and have been observed,
	// and the dependent's ownerRef does not match their observed coordinates
	hasPotentiallyInvalidOwnerReference := false

	for _, owner := range owners {
		ownerNode, ok := gb.uidToNode.Read(owner.UID)
		ownerNode.addDependent(n)

		ownerIsNamespaced := len(ownerNode.identity.Namespace) > 0
		if ownerIsNamespaced && ownerNode.identity.Namespace != n.identity.Namespace {
			if ownerNode.isObserved() {
				// The owner node has been observed via an informer
				// the dependent's namespace doesn't match the observed owner's namespace, this is definitely wrong.
				// cluster-scoped owners can be referenced as an owner from any namespace or cluster-scoped object.
				fmt.Printf("node %s references an owner %s but does not match namespaces\n", n.identity, ownerNode.identity)
				gb.reportInvalidNamespaceOwnerRef(n, owner.UID)
			}
			hasPotentiallyInvalidOwnerReference = true
		} else if !ownerReferenceMatchesCoordinates(owner, ownerNode.identity.OwnerReference) {
			if ownerNode.isObserved() {
				// The owner node has been observed via an informer
				// n's owner reference doesn't match the observed identity, this might be wrong.
				fmt.Printf("node %s references an owner %s with coordinates that do not match the observed identity\n", n.identity, ownerNode.identity)
			}
			hasPotentiallyInvalidOwnerReference = true
		} else if !ownerIsNamespaced && ownerNode.identity.Namespace != n.identity.Namespace && !ownerNode.isObserved() {
			// the ownerNode is cluster-scoped and virtual, and does not match the child node's namespace.
			// the owner could be a missing instance of a namespaced type incorrectly referenced by a cluster-scoped child (issue #98040).
			// enqueue this child to attemptToDelete to verify parent references.
			hasPotentiallyInvalidOwnerReference = true
		}
	}

	if hasPotentiallyInvalidOwnerReference {
		// Enqueue the potentially invalid dependent node into attemptToDelete.
		// The garbage processor will verify whether the owner references are dangling
		// and delete the dependent if all owner references are confirmed absent.
		gb.attemptToDelete.Add(n)
	}
}

// removeDependentFromOwners remove n from owners' dependents list.
func (gb *GraphBuilder) removeDependentFromOwners(n *node, owners []metav1.OwnerReference) {
	for _, owner := range owners {
		ownerNode, ok := gb.uidToNode.Read(owner.UID)
		if !ok {
			continue
		}
		ownerNode.deleteDependent(n)
	}
}

// if an blocking ownerReference points to an object gets removed, or gets set to
// "BlockOwnerDeletion=false", add the object to the attemptToDelete queue.
func (gb *GraphBuilder) addUnblockedOwnersToDeleteQueue(removed []metav1.OwnerReference, changed []ownerRefPair) {
}

// startMonitors ensures the current set of monitors are running. Any newly
// started monitors will also cause shared informers to be started.
//
// If called before Run, startMonitors does nothing (as there is no stop channel
// to support monitor/informer execution).
func (gb *GraphBuilder) startMonitors() {
	gb.monitorLock.Lock()
	defer gb.monitorLock.Unlock()

	if !gb.running {
		return
	}

	// we're waiting until after the informer start that happens once all the controllers are initialized.  This ensures
	// that they don't get unexpected events on their work queues.
	<-gb.informersStarted

	monitors := gb.monitors
	started := 0
	for _, monitor := range monitors {
		if monitor.stopCh == nil {
			monitor.stopCh = make(chan struct{})
			gb.sharedInformers.Start(gb.stopCh)
			go monitor.Run()
			started++
		}
	}
	fmt.Printf("started %d new monitors, %d currently running\n", started, len(monitors))
}

func (gb *GraphBuilder) processTransitions(oldObj interface{}, newAccessor metav1.Object, n *node) {
	if startsWaitingForDependentsOrphaned(oldObj, newAccessor) {
		fmt.Printf("add %s to the attemptToOrphan\n", n.identity)
		gb.attemptToOrphan.Add(n)
		return
	}
	if startsWaitingForDependentsDeleted(oldObj, newAccessor) {
		fmt.Printf("add %s to the attemptToDelete, because it's waiting for its dependents to be deleted\n", n.identity)
		// if the n is added as a "virtual" node, its deletingDependents field is not properly set, so always set it here.
		n.markDeletingDependents()
		for dep := range n.dependents {
			gb.attemptToDelete.Add(dep)
		}
		gb.attemptToDelete.Add(n)
	}
}

// this function takes newAccessor directly because the caller already
// instantiates an accessor for the newObj.
func startsWaitingForDependentsDeleted(oldObj interface{}, newAccessor string) bool {
	return deletionStartsWithFinalizer(oldObj, newAccessor, FinalizerDeleteDependents)
}

// this function takes newAccessor directly because the caller already
// instantiates an accessor for the newObj.
func startsWaitingForDependentsOrphaned(oldObj interface{}, newAccessor string) bool {
	return deletionStartsWithFinalizer(oldObj, newAccessor, FinalizerOrphanDependents)
}

func deletionStartsWithFinalizer(oldObj interface{}, newAccessor string, matchingFinalizer string) bool {
	// if the new object isn't being deleted, or doesn't have the finalizer we're interested in, return false
	if !beingDeleted(newAccessor) || !hasFinalizer(newAccessor, matchingFinalizer) {
		return false
	}

	// if the old object is nil, or wasn't being deleted, or didn't have the finalizer, return true
	if oldObj == nil {
		return true
	}
	oldAccessor, err := meta.Accessor(oldObj)
	return !beingDeleted(oldAccessor) || !hasFinalizer(oldAccessor, matchingFinalizer)
}

func beingDeleted(accessor string) bool {
	return accessor == "beingDeleted"
}

func hasFinalizer(accessor, matchingFinalizer string) bool {
	return accessor == matchingFinalizer
}