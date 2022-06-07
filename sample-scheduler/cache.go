package main

import (
	"fmt"
	"sync"
)

type imageState struct {
	size int64
	// A set of node names for nodes having this image present
	nodes map[string]struct{}
}

type cacheImpl struct {
	nodes map[string]*nodeInfoListItem
	// This mutex guards all fields within this cache struct.
	m sync.RWMutex
	// a map from pod key to podState.
	// a set of assumed pod keys.
	// The key could further be used to get an entry in podStates.
	assumedPods map[string]struct{}
	podStates   map[string]*podState
	imageStates map[string]*imageState
	headNode    *nodeInfoListItem
	nodeTree    *nodeTree
}

func NewCache() *cacheImpl {
	return &cacheImpl{
		nodes:       make(map[string]*nodeInfoListItem),
		nodeTree:    newNodeTree(nil),
		podStates:   make(map[string]*podState),
		imageStates: make(map[string]*imageState),
		assumedPods: make(map[string]struct{}),
	}
}

func (cache *cacheImpl) AddNode(node *Node) *NodeInfo {
	cache.m.Lock()
	defer cache.m.Unlock()

	n, ok := cache.nodes[node.name]
	if !ok {
		n = NewNodeInfoListItem(NewNodeInfo())
		cache.nodes[node.name] = n
	} else {
		cache.removeNodeImageStates(n.info.node)
	}
	cache.moveNodeInfoToHead(node.name)

	cache.nodeTree.addNode(node)
	cache.addNodeImageStates(node, n.info)
	n.info.node = node
	return n.info
}

func (cache *cacheImpl) UpdateNode(oldNode, newNode *Node) *NodeInfo {
	cache.m.Lock()
	defer cache.m.Unlock()

	n, ok := cache.nodes[newNode.name]
	if !ok {
		n = NewNodeInfoListItem(NewNodeInfo())
		cache.nodes[newNode.name] = n
		cache.nodeTree.addNode(newNode)
	} else {
		cache.removeNodeImageStates(n.info.node)
	}
	cache.moveNodeInfoToHead(newNode.name)

	cache.nodeTree.updateNode(oldNode, newNode)
	cache.addNodeImageStates(newNode, n.info)
	n.info.node = newNode
	return n.info
}

// RemoveNode removes a node from the cache's tree.
// The node might still have pods because their deletion events didn't arrive
// yet. Those pods are considered removed from the cache, being the node tree
// the source of truth.
// However, we keep a ghost node with the list of pods until all pod deletion
// events have arrived. A ghost node is skipped from snapshots.
func (cache *cacheImpl) RemoveNode(node *Node) error {
	cache.m.Lock()
	defer cache.m.Unlock()

	n, ok := cache.nodes[node.name]
	if !ok {
		return fmt.Errorf("node %v is not found", node.name)
	}
	// n.info.RemoveNode()
	// We remove NodeInfo for this node only if there aren't any pods on this node.
	// We can't do it unconditionally, because notifications about pods are delivered
	// in a different watch, and thus can potentially be observed later, even though
	// they happened before node removal.
	if len(n.info.Pods) == 0 {
		cache.removeNodeInfoFromList(node.name)
	} else {
		cache.moveNodeInfoToHead(node.name)
	}
	if err := cache.nodeTree.removeNode(node); err != nil {
		return err
	}
	cache.removeNodeImageStates(node)
	return nil
}

// moveNodeInfoToHead moves a NodeInfo to the head of "cache.nodes" doubly
// linked list. The head is the most recently updated NodeInfo.
// We assume cache lock is already acquired.
func (cache *cacheImpl) moveNodeInfoToHead(name string) {
	ni := cache.nodes[name]
	// if the node info list item is already at the head, we are done.
	if ni == cache.headNode {
		return
	}

	if ni.prev != nil {
		ni.prev.next = ni.next
	}
	if ni.next != nil {
		ni.next.prev = ni.prev
	}
	if cache.headNode != nil {
		cache.headNode.prev = ni
	}
	ni.next = cache.headNode
	ni.prev = nil
	cache.headNode = ni
}

// removeNodeInfoFromList removes a NodeInfo from the "cache.nodes" doubly
// linked list.
// We assume cache lock is already acquired.
func (cache *cacheImpl) removeNodeInfoFromList(name string) {
	ni := cache.nodes[name]

	if ni.prev != nil {
		ni.prev.next = ni.next
	}
	if ni.next != nil {
		ni.next.prev = ni.prev
	}
	// if the removed item was at the head, we must update the head.
	if ni == cache.headNode {
		cache.headNode = ni.next
	}
	delete(cache.nodes, name)
}

// addNodeImageStates adds states of the images on given node to the given nodeInfo and update the imageStates in
// scheduler cache. This function assumes the lock to scheduler cache has been acquired.
func (cache *cacheImpl) addNodeImageStates(node *Node, nodeInfo *NodeInfo) {
	newSum := make(map[string]*ImageStateSummary)

	for _, image := range node.images {
		// update the entry in imageStates
		state, ok := cache.imageStates[image.Name]
		if !ok {
			state = &imageState{
				nodes: map[string]struct{}{node.name: {}},
			}
			cache.imageStates[image.Name] = state
		} else {
			state.nodes[node.name] = struct{}{}
		}
		// create the imageStateSummary for this image
		newSum[image.Name] = &ImageStateSummary{
			Size:     state.size,
			NumNodes: len(state.nodes),
		}
	}
	nodeInfo.ImageStates = newSum
}

// removeNodeImageStates removes the given node record from image entries having the node
// in imageStates cache. After the removal, if any image becomes free, i.e., the image
// is no longer available on any node, the image entry will be removed from imageStates.
func (cache *cacheImpl) removeNodeImageStates(node *Node) {
	if node == nil {
		return
	}

	for _, image := range node.images {
		state, ok := cache.imageStates[image.Name]
		if ok {
			delete(state.nodes, node.name)
			if len(state.nodes) == 0 {
				// Remove the unused image to make sure the length of
				// imageStates represents the total number of different
				// images on all nodes
				delete(cache.imageStates, image.Name)
			}
		}
	}
}

// UpdateSnapshot takes a snapshot of cached NodeInfo map. This is called at
// beginning of every scheduling cycle.
// The snapshot only includes Nodes that are not deleted at the time this function is called.
// nodeinfo.Node() is guaranteed to be not nil for all the nodes in the snapshot.
// This function tracks generation number of NodeInfo and updates only the
// entries of an existing snapshot that have changed after the snapshot was taken.
func (cache *cacheImpl) UpdateSnapshot(nodeSnapshot *Snapshot) error {
	cache.m.Lock()
	defer cache.m.Unlock()

	// Get the last generation of the snapshot.
	snapshotGeneration := nodeSnapshot.generation

	// NodeInfoList and HavePodsWithAffinityNodeInfoList must be re-created if a node was added
	// or removed from the cache.
	updateAllLists := false
	// HavePodsWithAffinityNodeInfoList must be re-created if a node changed its
	// status from having pods with affinity to NOT having pods with affinity or the other
	// way around.
	updateNodesHavePodsWithAffinity := false
	// HavePodsWithRequiredAntiAffinityNodeInfoList must be re-created if a node changed its
	// status from having pods with required anti-affinity to NOT having pods with required
	// anti-affinity or the other way around.
	updateNodesHavePodsWithRequiredAntiAffinity := false

	// Start from the head of the NodeInfo doubly linked list and update snapshot
	// of NodeInfos updated after the last snapshot.
	for node := cache.headNode; node != nil; node = node.next {
		if node.info.Generation <= snapshotGeneration {
			// all the nodes are updated before the existing snapshot. We are done.
			break
		}
		if np := node.info.node; np != nil {
			existing, ok := nodeSnapshot.nodeInfoMap[np.name]
			if !ok {
				updateAllLists = true
				existing = &NodeInfo{}
				nodeSnapshot.nodeInfoMap[np.name] = existing
			}
			// We need to preserve the original pointer of the NodeInfo struct since it
			// is used in the NodeInfoList, which we may not update.
			*existing = *node.info
		}
	}
	// Update the snapshot generation with the latest NodeInfo generation.
	if cache.headNode != nil {
		nodeSnapshot.generation = cache.headNode.info.Generation
	}

	// Comparing to pods in nodeTree.
	// Deleted nodes get removed from the tree, but they might remain in the nodes map
	// if they still have non-deleted Pods.
	if len(nodeSnapshot.nodeInfoMap) > cache.nodeTree.numNodes {
		cache.removeDeletedNodesFromSnapshot(nodeSnapshot)
		updateAllLists = true
	}

	if updateAllLists || updateNodesHavePodsWithAffinity || updateNodesHavePodsWithRequiredAntiAffinity {
		cache.updateNodeInfoSnapshotList(nodeSnapshot, updateAllLists)
	}

	if len(nodeSnapshot.nodeInfoList) != cache.nodeTree.numNodes {
		errMsg := fmt.Sprintf("snapshot state is not consistent, length of NodeInfoList=%v not equal to length of nodes in tree=%v "+
			", length of NodeInfoMap=%v, length of nodes in cache=%v"+
			", trying to recover",
			len(nodeSnapshot.nodeInfoList), cache.nodeTree.numNodes,
			len(nodeSnapshot.nodeInfoMap), len(cache.nodes))
		fmt.Printf("%s\n", errMsg)
		// We will try to recover by re-creating the lists for the next scheduling cycle, but still return an
		// error to surface the problem, the error will likely cause a failure to the current scheduling cycle.
		cache.updateNodeInfoSnapshotList(nodeSnapshot, true)
		return fmt.Errorf(errMsg)
	}

	return nil
}

// If certain nodes were deleted after the last snapshot was taken, we should remove them from the snapshot.
func (cache *cacheImpl) removeDeletedNodesFromSnapshot(snapshot *Snapshot) {
	toDelete := len(snapshot.nodeInfoMap) - cache.nodeTree.numNodes
	for name := range snapshot.nodeInfoMap {
		if toDelete <= 0 {
			break
		}
		if n, ok := cache.nodes[name]; !ok || n.info.node == nil {
			delete(snapshot.nodeInfoMap, name)
			toDelete--
		}
	}
}

func (cache *cacheImpl) updateNodeInfoSnapshotList(snapshot *Snapshot, updateAll bool) {
	if updateAll {
		// Take a snapshot of the nodes order in the tree
		snapshot.nodeInfoList = make([]*NodeInfo, 0, cache.nodeTree.numNodes)
		nodesList, _ := cache.nodeTree.list()
		for _, nodeName := range nodesList {
			if nodeInfo := snapshot.nodeInfoMap[nodeName]; nodeInfo != nil {
				snapshot.nodeInfoList = append(snapshot.nodeInfoList, nodeInfo)
			} else {
				fmt.Printf("Node exists in nodeTree but not in NodeInfoMap, this should not happen, node %s\n", nodeName)
			}
		}
	}
}

func (cache *cacheImpl) AssumePod(pod *Pod) error {
	key := pod.uid

	cache.m.Lock()
	defer cache.m.Unlock()
	if _, ok := cache.podStates[key]; ok {
		return fmt.Errorf("pod %v is in the cache, so can't be assumed", key)
	}

	return cache.addPod(pod, true)
}

func (cache *cacheImpl) FinishBinding(pod *Pod) error {
	return nil
}

// Assumes that lock is already acquired.
func (cache *cacheImpl) addPod(pod *Pod, assumePod bool) error {
	key := pod.uid
	n, ok := cache.nodes[pod.nodeName]
	// In what case the node doesn't exist in cache.nodes ?
	if !ok {
		n = NewNodeInfoListItem(NewNodeInfo())
		cache.nodes[pod.nodeName] = n
	}
	n.info.AddPod(pod)
	cache.moveNodeInfoToHead(pod.nodeName)
	ps := &podState{
		pod: pod,
	}
	cache.podStates[key] = ps
	if assumePod {
		cache.assumedPods[key] = struct{}{}
	}
	return nil
}
