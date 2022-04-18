package main

import (
	"errors"
	"sync"
)

const (
	initialBufferSize = 1000
)

// SharedIndexInformer provides add and get Indexers ability based on SharedInformer.
type SharedIndexInformer interface {
	AddEventHandler(handler ResourceEventHandler)
	Run(stopCh <-chan struct{})

	OnAdd(obj interface{})
	OnUpdate(old interface{}, new interface{})
	OnDelete(obj interface{})
}

// `*sharedIndexInformer` implements SharedIndexInformer and has three
// main components.  One is an indexed local cache, `indexer Indexer`.
// The second main component is a Controller that pulls
// objects/notifications using the ListerWatcher and pushes them into
// a DeltaFIFO --- whose knownObjects is the informer's local cache
// --- while concurrently Popping Deltas values from that fifo and
// processing them with `sharedIndexInformer::eandleDeltas`.  Each
// updates the local cache and stuffs the relevant notification into
// the sharedProcessor.  The third main component is that
// sharedProcessor, which is responsible for relaying those
// notifications to each of the informer's clients.
type sharedIndexInformer struct {
	indexer    Indexer
	controller Controller

	processor *sharedProcessor

	listerWatcher ListerWatcher

	started, stopped bool
	startedLock      sync.Mutex

	// blockDeltas gives a way to stop all event distribution so that a late event handler
	// can safely join the shared informer.
	blockDeltas sync.Mutex
}

func NewSharedIndexInformer(lw ListerWatcher, indexer Indexer) SharedIndexInformer {
	sharedIndexInformer := &sharedIndexInformer{
		indexer:       indexer,
		listerWatcher: lw,
		processor:     &sharedProcessor{},
	}
	return sharedIndexInformer
}

func (s *sharedIndexInformer) Run(stopCh <-chan struct{}) {

	fifo := NewDeltaFIFOWithOptions()

	cfg := &Config{
		Queue:         fifo,
		ListerWatcher: s.listerWatcher,
		Process:       s.HandleDeltas,
	}

	func() {
		s.controller = New(cfg)
	}()

	// Separate stop channel because Processor should be stopped strictly after controller
	processorStopCh := make(chan struct{})
	var wg sync.WaitGroup
	defer wg.Wait()              // Wait for Processor to stop
	defer close(processorStopCh) // Tell Processor to stop

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.processor.run(processorStopCh)
	}()

	defer func() {
		s.startedLock.Lock()
		defer s.startedLock.Unlock()
		s.stopped = true // Don't want any new listeners
	}()

	s.controller.Run(stopCh)
}

func (s *sharedIndexInformer) AddEventHandler(handler ResourceEventHandler) {
	if s.stopped {
		return
	}

	listener := newProcessListener(handler, initialBufferSize)

	if !s.started {
		s.processor.addListener(listener)
		return
	}

	// in order to safely join, we have to
	// 1. stop sending add/update/delete notifications
	// 2. do a list against the store
	// 3. send synthetic "Add" events to the new handler
	// 4. unblock
	s.blockDeltas.Lock()
	defer s.blockDeltas.Unlock()

	s.processor.addListener(listener)
	// for _, item := range s.indexer.List() {
	// 	listener.add(addNotification{newObj: item})
	// }

}

func (s *sharedIndexInformer) HandleDeltas(obj interface{}) error {
	s.blockDeltas.Lock()
	defer s.blockDeltas.Unlock()

	if deltas, ok := obj.(Deltas); ok {
		return processDeltas(s, s.indexer, deltas)
	}
	return errors.New("object given as Process argument is not Deltas")
}

// Multiplexes updates in the form of a list of Deltas into a Store, and informs
// a given handler of events OnUpdate, OnAdd, OnDelete
func processDeltas(
	// Object which receives event notifications from the given deltas
	s SharedIndexInformer,
	clientState Store,
	deltas Deltas,
) error {
	// from oldest to newest
	for _, d := range deltas {
		obj := d.obj

		switch d.actionType {
		case DeltaAdded, DeltaUpdated, DeltaReplaced:
			if old, exists, err := clientState.Get(obj); err == nil && exists {
				if err := clientState.Update(obj); err != nil {
					return err
				}
				s.OnUpdate(old, obj)
			} else {
				if err := clientState.Add(obj); err != nil {
					return err
				}
				s.OnAdd(obj)
			}
		case DeltaDeleted:
			if err := clientState.Delete(obj); err != nil {
				return err
			}
			s.OnDelete(obj)
		}
	}
	return nil
}

func (s *sharedIndexInformer) OnAdd(obj interface{}) {
	s.processor.distribute(addNotification{obj}, false)
}

func (s *sharedIndexInformer) OnUpdate(old interface{}, new interface{}) {
	s.processor.distribute(updateNotification{old, new}, false)
}

func (s *sharedIndexInformer) OnDelete(obj interface{}) {
	s.processor.distribute(deleteNotification{obj}, false)
}
