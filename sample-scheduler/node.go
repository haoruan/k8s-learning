package main

import v1 "k8s.io/kubernetes/staging/src/k8s.io/api/core/v1"

type Node struct {
	name   string
	images []string
}

type NodeInfo struct {
	node *Node
	// Pods running on the node.
	Pods []*PodInfo

	// The subset of pods with affinity.
	PodsWithAffinity []*PodInfo
}

type nodeInfoListItem struct {
	info *NodeInfo
	next *nodeInfoListItem
	prev *nodeInfoListItem
}

// NewNodeInfo returns a ready to use empty NodeInfo object.
// If any pods are given in arguments, their information will be aggregated in
// the returned object.
func NewNodeInfo(pods ...*Pod) *NodeInfo {
	ni := &NodeInfo{}
	for _, pod := range pods {
		ni.AddPod(pod)
	}
	return ni
}

// newNodeInfoListItem initializes a new nodeInfoListItem.
func NewNodeInfoListItem(ni *NodeInfo) *nodeInfoListItem {
	return &nodeInfoListItem{
		info: ni,
	}
}

// AddPodInfo adds pod information to this NodeInfo.
// Consider using this instead of AddPod if a PodInfo is already computed.
func (n *NodeInfo) AddPodInfo(podInfo *PodInfo) {
	res, non0CPU, non0Mem := calculateResource(podInfo.Pod)
	n.Requested.MilliCPU += res.MilliCPU
	n.Requested.Memory += res.Memory
	n.Requested.EphemeralStorage += res.EphemeralStorage
	if n.Requested.ScalarResources == nil && len(res.ScalarResources) > 0 {
		n.Requested.ScalarResources = map[v1.ResourceName]int64{}
	}
	for rName, rQuant := range res.ScalarResources {
		n.Requested.ScalarResources[rName] += rQuant
	}
	n.NonZeroRequested.MilliCPU += non0CPU
	n.NonZeroRequested.Memory += non0Mem
	n.Pods = append(n.Pods, podInfo)
	if podWithAffinity(podInfo.Pod) {
		n.PodsWithAffinity = append(n.PodsWithAffinity, podInfo)
	}
	if podWithRequiredAntiAffinity(podInfo.Pod) {
		n.PodsWithRequiredAntiAffinity = append(n.PodsWithRequiredAntiAffinity, podInfo)
	}

	// Consume ports when pods added.
	n.updateUsedPorts(podInfo.Pod, true)
	n.updatePVCRefCounts(podInfo.Pod, true)

	n.Generation = nextGeneration()
}

// AddPod is a wrapper around AddPodInfo.
func (n *NodeInfo) AddPod(pod *Pod) {
	n.AddPodInfo(NewPodInfo(pod))
}
