package main

import (
	"sync"

	"k8s.io/kubernetes/pkg/scheduler/framework"
)

type imageState struct {
	// A set of node names for nodes having this image present
	nodes map[string]struct{}
}

type cacheImpl struct {
	nodes map[string]*nodeInfoListItem
	// This mutex guards all fields within this cache struct.
	m           sync.RWMutex
	imageStates map[string]*imageState
	headNode    *nodeInfoListItem
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

// moveNodeInfoToHead moves a NodeInfo to the head of "cache.nodes" doubly
// linked list. The head is the most recently updated NodeInfo.
// We assume cache lock is already acquired.
func (cache *cacheImpl) moveNodeInfoToHead(name string) {
	ni, _ := cache.nodes[name]
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

// addNodeImageStates adds states of the images on given node to the given nodeInfo and update the imageStates in
// scheduler cache. This function assumes the lock to scheduler cache has been acquired.
func (cache *cacheImpl) addNodeImageStates(node *Node, nodeInfo *NodeInfo) {
	newSum := make(map[string]*framework.ImageStateSummary)

	for _, image := range node.images {
		// update the entry in imageStates
		state, ok := cache.imageStates[image]
		if !ok {
			state = &imageState{
				nodes: map[string]struct{}{node.name: struct{}{}},
			}
			cache.imageStates[image] = state
		} else {
			state.nodes[node.name] = struct{}{}
		}
		// create the imageStateSummary for this image
		if _, ok := newSum[image]; !ok {
			newSum[image] = cache.createImageStateSummary(state)
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
		state, ok := cache.imageStates[image]
		if ok {
			delete(state.nodes, node.name)
			if len(state.nodes) == 0 {
				// Remove the unused image to make sure the length of
				// imageStates represents the total number of different
				// images on all nodes
				delete(cache.imageStates, image)
			}
		}
	}
}
