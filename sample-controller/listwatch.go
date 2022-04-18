package main

import (
	"math/rand"
	"sync"
)

const watchQueueLength = 25

type EventType string

const (
	EventAdded    EventType = "ADDED"
	EventModified EventType = "MODIFIED"
	EventDeleted  EventType = "DELETED"
	EventError    EventType = "ERROR"
)

type Event struct {
	Type EventType
	f    string
}

// Interface can be implemented by anything that knows how to watch and report changes.
type Interface interface {
	// Stop stops watching. Will close the channel returned by ResultChan(). Releases
	// any resources used by the watch.
	Stop()

	// ResultChan returns a chan which will receive all the events. If an error occurs
	// or Stop() is called, the implementation will close this channel and
	// release any resources used by the watch.
	ResultChan() <-chan Event

	SendEvent()
}

// Lister is any object that knows how to perform an initial list.
type Lister interface {
	// List should return a list type object; the Items field will be extracted, and the
	// ResourceVersion field will be used to start the watch in the right place.
	List() ([]string, error)
}

// Watcher is any object that knows how to start a watch on a resource.
type Watcher interface {
	// Watch should begin a watch at the specified version.
	Watch() (Interface, error)
	SendEvent()
}

type defaultWatcher struct {
	result  chan Event
	stopped chan struct{}
	stop    sync.Once
}

func (mw *defaultWatcher) SendEvent() {
	eventtype := []EventType{EventAdded, EventDeleted, EventModified}[rand.Intn(3)]
	eventvalue := []string{"a", "b", "c"}[rand.Intn(3)]
	mw.result <- Event{Type: eventtype, f: "type is " + string(eventtype) + "," + eventvalue}
}

// ResultChan returns a channel to use for waiting on events.
func (mw *defaultWatcher) ResultChan() <-chan Event {
	return mw.result
}

// Stop stops watching and removes mw from its list.
// It will block until the watcher stop request is actually executed
func (mw *defaultWatcher) Stop() {
	mw.stop.Do(func() {
		close(mw.stopped)
	})
}

// ListerWatcher is any object that knows how to perform an initial list and start a watch on a resource.
type ListerWatcher interface {
	Lister
	Watcher
}

// ListWatch knows how to list and watch a set of apiserver resources.  It satisfies the ListerWatcher interface.
// It is a convenience function for users of NewReflector, etc.
// ListFunc and WatchFunc must not be nil
type defaultListWatch struct {
	watch Interface
}

func NewDefaultListWatch() ListerWatcher {
	return &defaultListWatch{
		watch: &defaultWatcher{
			result:  make(chan Event, watchQueueLength),
			stopped: make(chan struct{}),
		},
	}
}

func (lw *defaultListWatch) List() ([]string, error) {
	return []string{"type is replace, a", "type is replace, b", "type is replace, c"}, nil
}

func (d *defaultListWatch) Watch() (Interface, error) {
	return d.watch, nil
}

func (d *defaultListWatch) SendEvent() {
	d.watch.SendEvent()
}
