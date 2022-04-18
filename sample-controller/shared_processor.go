package main

import (
	"fmt"
	"sync"
)

type updateNotification struct {
	oldObj interface{}
	newObj interface{}
}

type addNotification struct {
	newObj interface{}
}

type deleteNotification struct {
	oldObj interface{}
}

// ResourceEventHandler can handle notifications for events that
// happen to a resource. The events are informational only, so you
// can't return an error.  The handlers MUST NOT modify the objects
// received; this concerns not only the top level of structure but all
// the data structures reachable from it.
//  * OnAdd is called when an object is added.
//  * OnUpdate is called when an object is modified. Note that oldObj is the
//      last known state of the object-- it is possible that several changes
//      were combined together, so you can't use this to see every single
//      change. OnUpdate is also called when a re-list happens, and it will
//      get called even if nothing changed. This is useful for periodically
//      evaluating or syncing something.
//  * OnDelete will get the final state of the item if it is known, otherwise
//      it will get an object of type DeletedFinalStateUnknown. This can
//      happen if the watch is closed and misses the delete event and we don't
//      notice the deletion until the subsequent re-list.
type ResourceEventHandler interface {
	OnAdd(obj interface{})
	OnUpdate(oldObj, newObj interface{})
	OnDelete(obj interface{})
}

type defaultResourceEventHanlder struct{}

func (h *defaultResourceEventHanlder) OnAdd(obj interface{}) {
	fmt.Printf("OnAdd, %s\n", obj.(string))
}

func (h *defaultResourceEventHanlder) OnUpdate(oldObj, newObj interface{}) {
	fmt.Printf("OnUpdate, %s - %s\n", oldObj.(string), newObj.(string))

}
func (h *defaultResourceEventHanlder) OnDelete(obj interface{}) {
	fmt.Printf("OnDelete, %s\n", obj.(string))
}

// processorListener relays notifications from a sharedProcessor to
// one ResourceEventHandler --- using two goroutines, two unbuffered
// channels, and an unbounded ring buffer.  The `add(notification)`
// function sends the given notification to `addCh`.  One goroutine
// runs `pop()`, which pumps notifications from `addCh` to `nextCh`
// using storage in the ring buffer while `nextCh` is not keeping up.
// Another goroutine runs `run()`, which receives notifications from
// `nextCh` and synchronously invokes the appropriate handler method.
//
// processorListener also keeps track of the adjusted requested resync
// period of the listener.
type processorListener struct {
	nextCh chan interface{}
	addCh  chan interface{}

	handler ResourceEventHandler

	// pendingNotifications is an unbounded ring buffer that holds all notifications not yet distributed.
	// There is one per listener, but a failing/stalled listener will have infinite pendingNotifications
	// added until we OOM.
	// TODO: This is no worse than before, since reflectors were backed by unbounded DeltaFIFOs, but
	// we should try to do something better.
	pendingNotifications []interface{}
}

func newProcessListener(handler ResourceEventHandler, bufferSize int) *processorListener {
	ret := &processorListener{
		nextCh:               make(chan interface{}),
		addCh:                make(chan interface{}),
		handler:              handler,
		pendingNotifications: make([]interface{}, bufferSize),
	}

	return ret
}

func (p *processorListener) run() {
	for next := range p.nextCh {
		switch notification := next.(type) {
		case updateNotification:
			p.handler.OnUpdate(notification.oldObj, notification.newObj)
		case addNotification:
			p.handler.OnAdd(notification.newObj)
		case deleteNotification:
			p.handler.OnDelete(notification.oldObj)
		}
	}
}

func (p *processorListener) pop() {
	defer close(p.nextCh) // Tell .run() to stop

	var nextCh chan<- interface{}
	var notification interface{}
	for {
		select {
		case nextCh <- notification:
			// Notification dispatched
			if len(p.pendingNotifications) != 0 {
				notification = p.pendingNotifications[0]
				p.pendingNotifications = p.pendingNotifications[1:]
			} else { // Nothing to pop
				notification = nil
				nextCh = nil // Disable this select case
			}
		case notificationToAdd, ok := <-p.addCh:
			if !ok {
				return
			}
			if notification == nil { // No notification to pop (and pendingNotifications is empty)
				// Optimize the case - skip adding to pendingNotifications
				notification = notificationToAdd
				nextCh = p.nextCh
			} else { // There is already a notification waiting to be dispatched
				p.pendingNotifications = append(p.pendingNotifications, notificationToAdd)
			}
		}
	}
}

func (p *processorListener) add(notification interface{}) {
	p.addCh <- notification
}

// sharedProcessor has a collection of processorListener and can
// distribute a notification object to its listeners.  There are two
// kinds of distribute operations.  The sync distributions go to a
// subset of the listeners that (a) is recomputed in the occasional
// calls to shouldResync and (b) every listener is initially put in.
// The non-sync distributions go to every listener.
type sharedProcessor struct {
	listenersStarted bool
	listenersLock    sync.RWMutex
	listeners        []*processorListener
	wg               sync.WaitGroup
}

func (p *sharedProcessor) addListener(listener *processorListener) {
	p.listenersLock.Lock()
	defer p.listenersLock.Unlock()

	p.listeners = append(p.listeners, listener)

	if p.listenersStarted {
		p.wg.Add(2)
		go func() {
			defer p.wg.Done()
			listener.run()
		}()
		go func() {
			defer p.wg.Done()
			listener.pop()
		}()
	}
}

func (p *sharedProcessor) distribute(obj interface{}, sync bool) {
	p.listenersLock.RLock()
	defer p.listenersLock.RUnlock()

	for _, listener := range p.listeners {
		listener.add(obj)
	}
}

func (p *sharedProcessor) run(stopCh <-chan struct{}) {
	func() {
		p.listenersLock.RLock()
		defer p.listenersLock.RUnlock()
		for _, listener := range p.listeners {
			p.wg.Add(2)
			go func(l *processorListener) {
				defer p.wg.Done()
				l.run()
			}(listener)
			go func(l *processorListener) {
				defer p.wg.Done()
				l.pop()
			}(listener)
		}
		p.listenersStarted = true
	}()
	<-stopCh
	p.listenersLock.RLock()
	defer p.listenersLock.RUnlock()
	for _, listener := range p.listeners {
		close(listener.addCh) // Tell .pop() to stop. .pop() will tell .run() to stop
	}
	p.wg.Wait() // Wait for all .pop() and .run() to stop
}
