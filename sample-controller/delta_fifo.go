package main

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

// DeltaType is the type of a change (addition, deletion, etc)
type DeltaType string

// Change type definition
const (
	DeltaAdded    DeltaType = "Added"
	DeltaUpdated  DeltaType = "Updated"
	DeltaDeleted  DeltaType = "Deleted"
	DeltaReplaced DeltaType = "Replaced"
)

// ErrFIFOClosed used when FIFO is closed
var ErrFIFOClosed = errors.New("DeltaFIFO: manipulating with closed queue")

// KeyFunc knows how to make a key from an object. Implementations should be deterministic.
type KeyFunc func(obj interface{}) (string, error)

// Store is a generic object storage and processing interface.  A
// Store holds a map from string keys to accumulators, and has
// operations to add, update, and delete a given object to/from the
// accumulator currently associated with a given key.  A Store also
// knows how to extract the key from a given object, so many operations
// are given only the object.
//
// In the simplest Store implementations each accumulator is simply
// the last given object, or empty after Delete, and thus the Store's
// behavior is simple storage.
//
// Reflector knows how to watch a server and update a Store.  This
// package provides a variety of implementations of Store.
type Store interface {

	// Add adds the given object to the accumulator associated with the given object's key
	Add(obj interface{}) error

	// Update updates the given object in the accumulator associated with the given object's key
	Update(obj interface{}) error

	// Delete deletes the given object from the accumulator associated with the given object's key
	Delete(obj interface{}) error

	// Replace will delete the contents of the store, using instead the
	// given list. Store takes ownership of the list, you should not reference
	// it after calling this function.
	Replace([]interface{}) error

	// Get returns the accumulator associated with the given object's key
	Get(obj interface{}) (item interface{}, exists bool, err error)
}

// Queue extends Store with a collection of Store keys to "process".
// Every Add, Update, or Delete may put the object's key in that collection.
// A Queue has a way to derive the corresponding key given an accumulator.
// A Queue can be accessed concurrently from multiple goroutines.
// A Queue can be "closed", after which Pop operations return an error.
type Queue interface {
	Store

	// Pop blocks until there is at least one key to process or the
	// Queue is closed.  In the latter case Pop returns with an error.
	// In the former case Pop atomically picks one key to process,
	// removes that (key, accumulator) association from the Store, and
	// processes the accumulator.  Pop returns the accumulator that
	// was processed and the result of processing.  The PopProcessFunc
	// may return an ErrRequeue{inner} and in this case Pop will (a)
	// return that (key, accumulator) association to the Queue as part
	// of the atomic processing and (b) return the inner error from
	// Pop.
	Pop(ProcessFunc) (interface{}, error)

	// Close the queue
	Close()
}

type Delta struct {
	actionType DeltaType
	obj        interface{}
}

type Deltas []Delta

type DeltaFIFO struct {
	// lock/cond protects access to 'items' and 'queue'.
	lock sync.RWMutex
	cond sync.Cond

	// `items` maps a key to a Deltas.
	// Each such Deltas has at least one Delta.
	items map[string]Deltas

	// `queue` maintains FIFO order of keys for consumption in Pop().
	// There are no duplicates in `queue`.
	// A key is in `queue` if and only if it is in `items`.
	queue []string

	// keyFunc is used to make the key used for queued item
	// insertion and retrieval, and should be deterministic.
	keyFunc KeyFunc

	// Used to indicate a queue is closed so a control loop can exit when a queue is empty.
	// Currently, not used to gate any of CRUD operations.
	closed bool
}

func keyFunction(obj interface{}) (string, error) {
	return "key:" + strings.Split(obj.(string), ",")[1], nil
}

// NewDeltaFIFOWithOptions returns a Queue which can be used to process changes to
// items. See also the comment on DeltaFIFO.
func NewDeltaFIFOWithOptions() *DeltaFIFO {
	f := &DeltaFIFO{
		items:   map[string]Deltas{},
		queue:   []string{},
		keyFunc: keyFunction,
	}
	f.cond.L = &f.lock
	return f
}

// Pop blocks until the queue has some items, and then returns one.  If
// multiple items are ready, they are returned in the order in which they were
// added/updated. The item is removed from the queue (and the store) before it
// is returned, so if you don't successfully process it, you need to add it back
// with AddIfNotPresent().
// process function is called under lock, so it is safe to update data structures
// in it that need to be in sync with the queue (e.g. knownKeys). The PopProcessFunc
// may return an instance of ErrRequeue with a nested error to indicate the current
// item should be requeued (equivalent to calling AddIfNotPresent under the lock).
// process should avoid expensive I/O operation so that other queue operations, i.e.
// Add() and Get(), won't be blocked for too long.
//
// Pop returns a 'Deltas', which has a complete list of all the things
// that happened to the object (deltas) while it was sitting in the queue.
func (f *DeltaFIFO) Pop(process ProcessFunc) (interface{}, error) {
	f.lock.Lock()
	defer f.lock.Unlock()
	for {
		for len(f.queue) == 0 {
			// When the queue is empty, invocation of Pop() is blocked until new item is enqueued.
			// When Close() is called, the f.closed is set and the condition is broadcasted.
			// Which causes this loop to continue and return from the Pop().
			if f.closed {
				return nil, ErrFIFOClosed
			}

			f.cond.Wait()
		}
		id := f.queue[0]
		f.queue = f.queue[1:]
		item, ok := f.items[id]
		if !ok {
			// This should never happen
			fmt.Printf("Inconceivable! %q was in f.queue but not f.items; ignoring.\n", id)
			continue
		}
		delete(f.items, id)
		err := process(item)

		// Don't need to copyDeltas here, because we're transferring
		// ownership to the caller.
		return item, err
	}
}

// Add inserts an item, and puts it in the queue. The item is only enqueued
// if it doesn't already exist in the set.
func (f *DeltaFIFO) Add(obj interface{}) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.queueActionLocked(DeltaAdded, obj)
}

// Update is just like Add, but makes an Updated Delta.
func (f *DeltaFIFO) Update(obj interface{}) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.queueActionLocked(DeltaUpdated, obj)
}

// Delete is just like Add, but makes a Deleted Delta. If the given
// object does not already exist, it will be ignored. (It may have
// already been deleted by a Replace (re-list), for example.)  In this
// method `f.knownObjects`, if not nil, provides (via GetByKey)
// _additional_ objects that are considered to already exist.
func (f *DeltaFIFO) Delete(obj interface{}) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.queueActionLocked(DeltaDeleted, obj)
}

// queueActionLocked appends to the delta list for the object.
// Caller must lock first.
func (f *DeltaFIFO) queueActionLocked(actionType DeltaType, obj interface{}) error {
	id, _ := f.keyFunc(obj)

	oldDeltas := f.items[id]
	newDeltas := append(oldDeltas, Delta{actionType, obj})

	if _, exists := f.items[id]; !exists {
		f.queue = append(f.queue, id)
	}
	f.items[id] = newDeltas
	f.cond.Broadcast()

	return nil
}

// Replace atomically does two things: (1) it adds the given objects
// using the Sync or Replace DeltaType and then (2) it does some deletions.
// In particular: for every pre-existing key K that is not the key of
// an object in `list` there is the effect of
// `Delete(DeletedFinalStateUnknown{K, O})` where O is current object
// of K.  If `f.knownObjects == nil` then the pre-existing keys are
// those in `f.items` and the current object of K is the `.Newest()`
// of the Deltas associated with K.  Otherwise the pre-existing keys
// are those listed by `f.knownObjects` and the current object of K is
// what `f.knownObjects.GetByKey(K)` returns.
func (f *DeltaFIFO) Replace(list []interface{}) error {
	f.lock.Lock()
	defer f.lock.Unlock()

	// keep backwards compat for old clients
	action := DeltaReplaced

	// Add Sync/Replaced action for each new item.
	for _, item := range list {
		if err := f.queueActionLocked(action, item); err != nil {
			return fmt.Errorf("couldn't enqueue object: %v", err)
		}
	}

	return nil
}

func (f *DeltaFIFO) Get(obj interface{}) (item interface{}, exists bool, err error) {
	f.lock.RLock()
	defer f.lock.RUnlock()

	key, _ := f.keyFunc(obj)
	d, exists := f.items[key]

	return d, exists, nil
}

func (f *DeltaFIFO) Close() {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.closed = true
	f.cond.Broadcast()
}
