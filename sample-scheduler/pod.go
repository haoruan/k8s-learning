package main

type Pod struct {
	uid      string
	name     string
	nodeName string
}

type PodInfo struct {
	pod           *Pod
	schedulerName string
	t             int
	idx           int
}

type podState struct {
	pod *Pod
	// Used by assumedPod to determinate expiration.
	// deadline *time.Time
	// Used to block cache from expiring assumedPod if binding still runs
	// bindingFinished bool
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
	pi.pod = pod
}
