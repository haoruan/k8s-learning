package main

import (
	"container/heap"
	"sync"
)

type PriorityQueue struct {
	lock  sync.Mutex
	items []*PodInfo
}

var _ heap.Interface = &PriorityQueue{}

func (pq *PriorityQueue) Push(x any) {
	pq.items = append(pq.items, x.(*PodInfo))
}

func (pq *PriorityQueue) Pop() any {
	n := len(pq.items)
	item := pq.items[n-1]
	pq.items = pq.items[0 : n-1]

	return item
}

func (pq *PriorityQueue) Len() int {
	return len(pq.items)
}

func (pq *PriorityQueue) Less(i, j int) bool {
	return pq.items[i].t < pq.items[j].t
}

func (pq *PriorityQueue) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
	pq.items[i].idx = i
	pq.items[j].idx = j
}

func (pq *PriorityQueue) Add(x *PodInfo) {
	pq.lock.Lock()
	defer pq.lock.Unlock()
	heap.Push(pq, x)
}

func (pq *PriorityQueue) Top(x *PodInfo) *PodInfo {
	pq.lock.Lock()
	defer pq.lock.Unlock()
	return pq.items[0]
}

func (pq *PriorityQueue) Get() *PodInfo {
	pq.lock.Lock()
	defer pq.lock.Unlock()
	return heap.Pop(pq).(*PodInfo)
}

func (pq *PriorityQueue) Update(x *PodInfo) {
	pq.lock.Lock()
	defer pq.lock.Unlock()
	pq.items[x.idx] = x
	heap.Fix(pq, x.idx)
}
