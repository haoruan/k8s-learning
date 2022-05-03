package main

import "sync"

type owner struct {
	uid string
	//val string
	BlockOwnerDeletion bool
}

type node struct {
	uid string
	obj interface{}
	// dependents will be read by the orphan() routine, we need to protect it with a lock.
	dependentsLock sync.RWMutex
	// dependents are the nodes that have node.identity as a
	// metadata.ownerReference.
	dependents map[*node]struct{}
	// this is set by processGraphChanges() if the object has non-nil DeletionTimestamp
	// and has the FinalizerDeleteDependents.
	deletingDependents     bool
	deletingDependentsLock sync.RWMutex
	// this records if the object's deletionTimestamp is non-nil.
	beingDeleted     bool
	beingDeletedLock sync.RWMutex
	// this records if the object was constructed virtually and never observed via informer event
	// virtual     bool
	// virtualLock sync.RWMutex
	// when processing an Update event, we need to compare the updated
	// ownerReferences with the owners recorded in the graph.
	owners []owner
}

func (n *node) addDependent(dependent *node) {
	n.dependentsLock.Lock()
	defer n.dependentsLock.Unlock()
	n.dependents[dependent] = struct{}{}
}

func (n *node) deleteDependent(dependent *node) {
	n.dependentsLock.Lock()
	defer n.dependentsLock.Unlock()
	delete(n.dependents, dependent)
}

func (n *node) markDeletingDependents() {
	n.deletingDependentsLock.Lock()
	defer n.deletingDependentsLock.Unlock()
	n.deletingDependents = true
}

func (n *node) isBeingDeleted() bool {
	n.beingDeletedLock.RLock()
	defer n.beingDeletedLock.RUnlock()
	return n.beingDeleted
}

func (n *node) dependentsLength() int {
	n.dependentsLock.RLock()
	defer n.dependentsLock.RUnlock()
	return len(n.dependents)
}

// An object is on a one way trip to its final deletion if it starts being
// deleted, so we only provide a function to set beingDeleted to true.
func (n *node) markBeingDeleted() {
	n.beingDeletedLock.Lock()
	defer n.beingDeletedLock.Unlock()
	n.beingDeleted = true
}

func (n *node) isDeletingDependents() bool {
	n.deletingDependentsLock.RLock()
	defer n.deletingDependentsLock.RUnlock()
	return n.deletingDependents
}

// Note that this function does not provide any synchronization guarantees;
// items could be added to or removed from ownerNode.dependents the moment this
// function returns.
func (n *node) getDependents() []*node {
	n.dependentsLock.RLock()
	defer n.dependentsLock.RUnlock()
	var ret []*node
	for dep := range n.dependents {
		ret = append(ret, dep)
	}
	return ret
}

// blockingDependents returns the dependents that are blocking the deletion of
// n, i.e., the dependent that has an ownerReference pointing to n, and
// the BlockOwnerDeletion field of that ownerReference is true.
// Note that this function does not provide any synchronization guarantees;
// items could be added to or removed from ownerNode.dependents the moment this
// function returns.
func (n *node) blockingDependents() []*node {
	dependents := n.getDependents()
	var ret []*node
	for _, dep := range dependents {
		for _, owner := range dep.owners {
			if owner.uid == n.uid && owner.BlockOwnerDeletion {
				ret = append(ret, dep)
			}
		}
	}
	return ret
}

type concurrentUIDToNode struct {
	uidToNodeLock sync.RWMutex
	uidToNode     map[string]*node
}

func NewConcurrentUIDToNode() *concurrentUIDToNode {
	return &concurrentUIDToNode{
		uidToNode: make(map[string]*node),
	}
}

func (m *concurrentUIDToNode) Write(node *node) {
	m.uidToNodeLock.Lock()
	defer m.uidToNodeLock.Unlock()
	m.uidToNode[node.uid] = node
}

func (m *concurrentUIDToNode) Read(uid string) (*node, bool) {
	m.uidToNodeLock.RLock()
	defer m.uidToNodeLock.RUnlock()
	n, ok := m.uidToNode[uid]
	return n, ok
}

func (m *concurrentUIDToNode) Delete(uid string) {
	m.uidToNodeLock.Lock()
	defer m.uidToNodeLock.Unlock()
	delete(m.uidToNode, uid)
}
