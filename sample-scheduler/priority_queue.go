package main

import (
	"container/heap"
	"fmt"
	"sync"
	"time"
)

// lessFunc is a function that receives two items and returns true if the first
// item should be placed before the second one when the list is sorted.
type lessFunc = func(item1, item2 interface{}) bool

// KeyFunc is a function type to get the key from an object.
type KeyFunc func(obj interface{}) (string, error)

type heapItem struct {
	key string
	obj interface{}
	idx int
}

type data struct {
	items map[string]*heapItem
	queue []string
	// keyFunc is used to make the key used for queued item insertion and retrieval, and
	// should be deterministic.
	keyFunc KeyFunc
	// lessFunc is used to compare two objects in the heap.
	lessFunc lessFunc
}

type Heap struct {
	data *data
}

// UnschedulablePods holds pods that cannot be scheduled. This data structure
// is used to implement unschedulablePods.
type UnschedulablePods struct {
	// podInfoMap is a map key by a pod's full-name and the value is a pointer to the QueuedPodInfo.
	podInfoMap map[string]*PodInfo
	keyFunc    func(*Pod) string
}

// Add adds a pod to the unschedulable podInfoMap.
func (u *UnschedulablePods) addOrUpdate(pInfo *PodInfo) {
	podID := u.keyFunc(pInfo.pod)
	u.podInfoMap[podID] = pInfo
}

// Delete deletes a pod from the unschedulable podInfoMap.
func (u *UnschedulablePods) delete(pod *Pod) {
	podID := u.keyFunc(pod)
	delete(u.podInfoMap, podID)
}

// Get returns the QueuedPodInfo if a pod with the same key as the key of the given "pod"
// is found in the map. It returns nil otherwise.
func (u *UnschedulablePods) get(pod *Pod) *PodInfo {
	podKey := u.keyFunc(pod)
	if pInfo, exists := u.podInfoMap[podKey]; exists {
		return pInfo
	}
	return nil
}

// Clear removes all the entries from the unschedulable podInfoMap.
func (u *UnschedulablePods) clear() {
	u.podInfoMap = make(map[string]*PodInfo)
}

type NominatingMode int

const (
	ModeNoop NominatingMode = iota
	ModeOverride
)

type NominatingInfo struct {
	NominatedNodeName string
	NominatingMode    NominatingMode
}

// PodNominator abstracts operations to maintain nominated Pods.
type PodNominator interface {
	// AddNominatedPod adds the given pod to the nominator or
	// updates it if it already exists.
	AddNominatedPod(pod *PodInfo, nominatingInfo *NominatingInfo)
	// DeleteNominatedPodIfExists deletes nominatedPod from internal cache. It's a no-op if it doesn't exist.
	DeleteNominatedPodIfExists(pod *Pod)
	// UpdateNominatedPod updates the <oldPod> with <newPod>.
	UpdateNominatedPod(oldPod *Pod, newPodInfo *PodInfo)
	// NominatedPodsForNode returns nominatedPods on the given node.
	NominatedPodsForNode(nodeName string) []*PodInfo
}

// nominator is a structure that stores pods nominated to run on nodes.
// It exists because nominatedNodeName of pod objects stored in the structure
// may be different than what scheduler has here. We should be able to find pods
// by their UID and update/delete them.
type nominator struct {
	// nominatedPods is a map keyed by a node name and the value is a list of
	// pods which are nominated to run on the node. These are pods which can be in
	// the activeQ or unschedulablePods.
	nominatedPods map[string][]*PodInfo
	// nominatedPodToNode is map keyed by a Pod UID to the node name where it is
	// nominated.
	nominatedPodToNode map[string]string

	sync.RWMutex
}

func (ni *NominatingInfo) Mode() NominatingMode {
	if ni == nil {
		return ModeNoop
	}
	return ni.NominatingMode
}

func (npm *nominator) add(pi *PodInfo, nominatingInfo *NominatingInfo) {
	// Always delete the pod if it already exists, to ensure we never store more than
	// one instance of the pod.
	npm.delete(pi.pod)

	var nodeName string
	if nominatingInfo.Mode() == ModeOverride {
		nodeName = nominatingInfo.NominatedNodeName
	} else if nominatingInfo.Mode() == ModeNoop {
		if pi.pod.nominatedNodeName == "" {
			return
		}
		nodeName = pi.pod.nominatedNodeName
	}

	npm.nominatedPodToNode[pi.pod.uid] = nodeName
	for _, npi := range npm.nominatedPods[nodeName] {
		if npi.pod.uid == pi.pod.uid {
			fmt.Printf("Pod already exists in the nominator, pod %s\n", npi.pod.name)
			return
		}
	}
	npm.nominatedPods[nodeName] = append(npm.nominatedPods[nodeName], pi)
}

func (npm *nominator) delete(p *Pod) {
	nnn, ok := npm.nominatedPodToNode[p.uid]
	if !ok {
		return
	}
	for i, np := range npm.nominatedPods[nnn] {
		if np.pod.uid == p.uid {
			npm.nominatedPods[nnn] = append(npm.nominatedPods[nnn][:i], npm.nominatedPods[nnn][i+1:]...)
			if len(npm.nominatedPods[nnn]) == 0 {
				delete(npm.nominatedPods, nnn)
			}
			break
		}
	}
	delete(npm.nominatedPodToNode, p.uid)
}

// NominatedPodsForNode returns a copy of pods that are nominated to run on the given node,
// but they are waiting for other pods to be removed from the node.
func (npm *nominator) NominatedPodsForNode(nodeName string) []*PodInfo {
	npm.RLock()
	defer npm.RUnlock()
	// Make a copy of the nominated Pods so the caller can mutate safely.
	pods := make([]*PodInfo, len(npm.nominatedPods[nodeName]))
	for i := 0; i < len(pods); i++ {
		pods[i] = npm.nominatedPods[nodeName][i]
	}
	return pods
}

// AddNominatedPod adds a pod to the nominated pods of the given node.
// This is called during the preemption process after a node is nominated to run
// the pod. We update the structure before sending a request to update the pod
// object to avoid races with the following scheduling cycles.
func (npm *nominator) AddNominatedPod(pi *PodInfo, nominatingInfo *NominatingInfo) {
	npm.Lock()
	npm.add(pi, nominatingInfo)
	npm.Unlock()
}

// DeleteNominatedPodIfExists deletes <pod> from nominatedPods.
func (npm *nominator) DeleteNominatedPodIfExists(pod *Pod) {
	npm.Lock()
	npm.delete(pod)
	npm.Unlock()
}

// UpdateNominatedPod updates the <oldPod> with <newPod>.
func (npm *nominator) UpdateNominatedPod(oldPod *Pod, newPodInfo *PodInfo) {
	npm.Lock()
	defer npm.Unlock()
	// In some cases, an Update event with no "NominatedNode" present is received right
	// after a node("NominatedNode") is reserved for this pod in memory.
	// In this case, we need to keep reserving the NominatedNode when updating the pod pointer.
	var nominatingInfo *NominatingInfo
	// We won't fall into below `if` block if the Update event represents:
	// (1) NominatedNode info is added
	// (2) NominatedNode info is updated
	// (3) NominatedNode info is removed
	if oldPod.nominatedNodeName == "" && newPodInfo.pod.nominatedNodeName == "" {
		if nnn, ok := npm.nominatedPodToNode[oldPod.uid]; ok {
			// This is the only case we should continue reserving the NominatedNode
			nominatingInfo = &NominatingInfo{
				NominatingMode:    ModeOverride,
				NominatedNodeName: nnn,
			}
		}
	}
	// We update irrespective of the nominatedNodeName changed or not, to ensure
	// that pod pointer is updated.
	npm.delete(oldPod)
	npm.add(newPodInfo, nominatingInfo)
}

// NewPodNominator creates a nominator as a backing of framework.PodNominator.
// A podLister is passed in so as to check if the pod exists
// before adding its nominatedNode info.
func NewPodNominator() PodNominator {
	return &nominator{
		nominatedPods:      make(map[string][]*PodInfo),
		nominatedPodToNode: make(map[string]string),
	}
}

// GetPodFullName returns a name that uniquely identifies a pod.
func getPodFullName(pod *Pod) string {
	// Use underscore as the delimiter because it is not allowed in pod name
	// (DNS subdomain format).
	return pod.name
}

// newUnschedulablePods initializes a new object of UnschedulablePods.
func newUnschedulablePods() *UnschedulablePods {
	return &UnschedulablePods{
		podInfoMap: make(map[string]*PodInfo),
		keyFunc:    getPodFullName,
	}
}

type PriorityQueue struct {
	activeQ *Heap
	// podBackoffQ is a heap ordered by backoff expiry. Pods which have completed backoff
	// are popped from this heap before the scheduler looks at activeQ
	podBackoffQ *Heap
	PodNominator

	podInitialBackoffDuration         time.Duration
	podMaxInUnschedulablePodsDuration time.Duration

	moveRequestCycle int64
	stopCh           chan struct{}

	lock  sync.Mutex
	cond  sync.Cond
	items []*PodInfo

	// unschedulablePods holds pods that have been tried and determined unschedulable.
	unschedulablePods *UnschedulablePods
}

func newActiveQ() *Heap {
	return &Heap{
		data: &data{
			items: make(map[string]*heapItem),
		},
	}
}

func NewPriorityQueue() *PriorityQueue {
	return &PriorityQueue{
		activeQ: newActiveQ(),
	}
}

var _ heap.Interface = &data{}

func (d *data) Push(x any) {
	item := x.(*heapItem)
	d.items[item.key] = item
	d.queue = append(d.queue, item.key)

}

func (d *data) Pop() any {
	n := len(d.queue)
	item := d.queue[n-1]
	d.queue = d.queue[0 : n-1]

	v, ok := d.items[item]
	if !ok {
		// error
		return nil
	}
	delete(d.items, v.key)

	return item
}

func (d *data) Len() int {
	return len(d.queue)
}

func (d *data) Less(i, j int) bool {
	v1, ok := d.items[d.queue[i]]
	if !ok {
		return false
	}
	v2, ok := d.items[d.queue[j]]
	if !ok {
		return false
	}

	return d.lessFunc(v1, v2)
}

func (d *data) Swap(i, j int) {
	d.queue[i], d.queue[j] = d.queue[j], d.queue[i]
	v1, ok := d.items[d.queue[i]]
	if !ok {
		return
	}
	v2, ok := d.items[d.queue[j]]
	if !ok {
		return
	}

	v1.idx = i
	v2.idx = j
}

// Update is the same as Add in this implementation. When the item does not
// exist, it is added.
func (h *Heap) Update(obj interface{}) {
	h.Add(obj)
}

func (h *Heap) Add(obj interface{}) {
	key, _ := h.data.keyFunc(obj)
	if item, ok := h.data.items[key]; ok {
		h.data.items[key].obj = obj
		heap.Fix(h.data, item.idx)

	} else {
		item = &heapItem{key, obj, len(h.data.queue)}
		heap.Push(h.data, item)
	}
}

func (h *Heap) Top() interface{} {
	if len(h.data.queue) > 0 {
		return h.data.items[h.data.queue[0]].obj
	}

	return nil
}

func (h *Heap) Pop() interface{} {
	if len(h.data.queue) > 0 {
		return heap.Pop(h.data)
	}

	return nil
}

func (h *Heap) Get(obj interface{}) (interface{}, bool) {
	key, _ := h.data.keyFunc(obj)
	item, ok := h.data.items[key]
	return item, ok
}

func (h *Heap) Len() int {
	return h.data.Len()
}

// Delete removes an item.
func (h *Heap) Delete(obj interface{}) {
	key, _ := h.data.keyFunc(obj)
	if item, ok := h.data.items[key]; ok {
		heap.Remove(h.data, item.idx)
	}
}

func (pq *PriorityQueue) Pop() *PodInfo {
	pq.lock.Lock()
	defer pq.lock.Unlock()

	return pq.activeQ.Pop().(*PodInfo)
}

func (pq *PriorityQueue) Len() int {
	pq.lock.Lock()
	defer pq.lock.Unlock()

	return pq.activeQ.Len()
}

// Add adds a pod to the active queue. It should be called only when a new pod
// is added so there is no chance the pod is already in active/unschedulable/backoff queues
func (p *PriorityQueue) Add(pInfo *PodInfo) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.activeQ.Add(pInfo)
	if p.unschedulablePods.get(pInfo.pod) != nil {
		fmt.Printf("Error: pod is already in the unschedulable queue, pod %s\n", pInfo.pod.name)
		p.unschedulablePods.delete(pInfo.pod)
	}
	// Delete pod from backoffQ if it is backing off
	p.podBackoffQ.Delete(pInfo)

	p.PodNominator.AddNominatedPod(pInfo, nil)
	p.cond.Broadcast()

	return nil
}

// Activate moves the given pods to activeQ iff they're in unschedulablePods or backoffQ.
func (p *PriorityQueue) Activate(pods map[string]*Pod) {
	p.lock.Lock()
	defer p.lock.Unlock()

	activated := false
	for _, pod := range pods {
		if p.activate(pod) {
			activated = true
		}
	}

	if activated {
		p.cond.Broadcast()
	}
}

// Run starts the goroutine to pump from podBackoffQ to activeQ
func (p *PriorityQueue) Run() {
	go func() {
		for {
			select {
			case <-p.stopCh:
				return
			default:
				p.flushBackoffQCompleted()
				time.Sleep(1 * time.Second)
			}
		}
	}()

	go func() {
		for {
			select {
			case <-p.stopCh:
				return
			default:
				p.flushUnschedulablePodsLeftover()
				time.Sleep(10 * time.Second)
			}
		}
	}()
}

func (p *PriorityQueue) Close() {
	p.lock.Lock()
	defer p.lock.Unlock()

	close(p.stopCh)
	p.cond.Broadcast()
}

func (p *PriorityQueue) activate(pod *Pod) bool {
	// Verify if the pod is present in activeQ.
	if _, ok := p.activeQ.Get(NewPodInfo(pod)); ok {
		// No need to activate if it's already present in activeQ.
		return false
	}

	var pInfo *PodInfo
	// Verify if the pod is present in unschedulablePods or backoffQ.
	if pInfo = p.unschedulablePods.get(pod); pInfo == nil {
		// If the pod doesn't belong to unschedulablePods or backoffQ, don't activate it.
		if obj, exists := p.podBackoffQ.Get(NewPodInfo(pod)); !exists {
			fmt.Printf("To-activate pod does not exist in unschedulablePods or backoffQ, pod %s\n", pod.name)
			return false
		} else {
			pInfo = obj.(*PodInfo)
		}
	}

	if pInfo == nil {
		// Redundant safe check. We shouldn't reach here.
		fmt.Printf("Internal error: cannot obtain pInfo")
		return false
	}

	p.activeQ.Add(pInfo)
	p.unschedulablePods.delete(pod)
	p.podBackoffQ.Delete(pInfo)
	p.PodNominator.AddNominatedPod(pInfo, nil)
	return true
}

// AddUnschedulableIfNotPresent inserts a pod that cannot be scheduled into
// the queue, unless it is already in the queue. Normally, PriorityQueue puts
// unschedulable pods in `unschedulablePods`. But if there has been a recent move
// request, then the pod is put in `podBackoffQ`.
func (p *PriorityQueue) AddUnschedulableIfNotPresent(pInfo *PodInfo, podSchedulingCycle int64) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	pod := pInfo.pod
	if p.unschedulablePods.get(pod) != nil {
		return fmt.Errorf("Pod %s is already present in unschedulable queue", pod.name)
	}

	if _, exists := p.activeQ.Get(pInfo); exists {
		return fmt.Errorf("Pod %s is already present in the active queue", pod.name)
	}
	if _, exists := p.podBackoffQ.Get(pInfo); exists {
		return fmt.Errorf("Pod %s is already present in the backoff queue", pod.name)
	}

	// Refresh the timestamp since the pod is re-added.
	pInfo.timestamp = time.Now()

	if p.moveRequestCycle >= podSchedulingCycle {
		p.podBackoffQ.Add(pInfo)
	} else {
		p.unschedulablePods.addOrUpdate(pInfo)

	}

	p.PodNominator.AddNominatedPod(pInfo, nil)
	return nil
}

// flushBackoffQCompleted Moves all pods from backoffQ which have completed backoff in to activeQ
func (p *PriorityQueue) flushBackoffQCompleted() {
	p.lock.Lock()
	defer p.lock.Unlock()
	activated := false
	for {
		rawPodInfo := p.podBackoffQ.Top()
		if rawPodInfo == nil {
			break
		}
		boTime := rawPodInfo.(*PodInfo).timestamp.Add(p.podInitialBackoffDuration)
		if boTime.After(time.Now()) {
			break
		}
		p.podBackoffQ.Pop()
		p.activeQ.Add(rawPodInfo)
		activated = true
	}

	if activated {
		p.cond.Broadcast()
	}
}

// flushUnschedulablePodsLeftover moves pods which stay in unschedulablePods
// longer than podMaxInUnschedulablePodsDuration to backoffQ or activeQ.
func (p *PriorityQueue) flushUnschedulablePodsLeftover() {
	p.lock.Lock()
	defer p.lock.Unlock()

	var podsToMove []*PodInfo
	currentTime := time.Now()
	for _, pInfo := range p.unschedulablePods.podInfoMap {
		lastScheduleTime := pInfo.timestamp
		if currentTime.Sub(lastScheduleTime) > p.podMaxInUnschedulablePodsDuration {
			podsToMove = append(podsToMove, pInfo)
		}
	}

	if len(podsToMove) > 0 {
		p.movePodsToActiveOrBackoffQueue(podsToMove)
	}
}

// NOTE: this function assumes lock has been acquired in caller
func (p *PriorityQueue) movePodsToActiveOrBackoffQueue(podInfoList []*PodInfo) {
	activated := false
	for _, pInfo := range podInfoList {
		pod := pInfo.pod
		if pInfo.timestamp.Add(p.podInitialBackoffDuration).After(time.Now()) {
			p.podBackoffQ.Add(pInfo)
		} else {
			p.activeQ.Add(pInfo)
			activated = true
		}
		p.unschedulablePods.delete(pod)
	}
	// p.moveRequestCycle = p.schedulingCycle
	if activated {
		p.cond.Broadcast()
	}
}
