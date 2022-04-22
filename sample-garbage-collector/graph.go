package main

import "sync"

type node struct {
	identity string
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
	virtual     bool
	virtualLock sync.RWMutex
	// when processing an Update event, we need to compare the updated
	// ownerReferences with the owners recorded in the graph.
	owners []string
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

type concurrentUIDToNode struct {
	uidToNodeLock sync.RWMutex
	uidToNode     map[string]*node
}

func (m *concurrentUIDToNode) Write(node *node) {
	m.uidToNodeLock.Lock()
	defer m.uidToNodeLock.Unlock()
	m.uidToNode[node.identity] = node
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
