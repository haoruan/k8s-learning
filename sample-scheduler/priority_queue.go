package main

import (
	"container/heap"
	"sync"
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

type PriorityQueue struct {
	activeQ *Heap
	lock    sync.Mutex
	items   []*PodInfo
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

func (h *Heap) Get() interface{} {
	if len(h.data.queue) > 0 {
		return heap.Pop(h.data)
	}

	return nil
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
