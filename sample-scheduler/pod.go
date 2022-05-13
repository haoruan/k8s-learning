package main

type Pod struct {
	name     string
	nodeName string
}

type PodInfo struct {
	name          string
	schedulerName string
	t             int
	idx           int
}

// NewPodInfo returns a new PodInfo.
func NewPodInfo(pod *Pod) *PodInfo {
	pInfo := &PodInfo{}
	pInfo.Update(pod)
	return pInfo
}

// Update creates a full new PodInfo by default. And only updates the pod when the PodInfo
// has been instantiated and the passed pod is the exact same one as the original pod.
func (pi *PodInfo) Update(pod *Pod) {
}
