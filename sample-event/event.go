package main

import (
	"fmt"
	"sync"
	"time"
)

const maxQueuedEvents = 1000

var defaultSleepDuration = 10 * time.Second

// EventBroadcaster knows how to receive events and send them to any EventSink, watcher, or log.
type EventBroadcaster interface {
	// StartEventWatcher starts sending events received from this EventBroadcaster to the given
	// event handler function. The return value can be ignored or used to stop recording, if
	// desired.
	StartEventWatcher(eventHandler func(func())) Interface

	StartSimpleEventWatcher() Interface

	// NewRecorder returns an EventRecorder that can be used to send events to this EventBroadcaster
	// with the event source set to the given event source.
	NewRecorder() EventRecorder

	// Shutdown shuts down the broadcaster
	Shutdown()
}

// EventRecorder knows how to record events on behalf of an EventSource.
type EventRecorder interface {
	Event(eventtype EventType)
}

type eventBroadcasterImpl struct {
	*Broadcaster
	wg            sync.WaitGroup
	sleepDuration time.Duration
}

type recorderImpl struct {
	*Broadcaster
}

func (recorder *recorderImpl) Event(eventtype EventType) {
	recorder.generateEvent(eventtype)
}

func (recorder *recorderImpl) generateEvent(eventtype EventType) {
	recorder.Broadcaster.Action(eventtype, func() {
		fmt.Printf("Event: %s\n", eventtype)
	})
}

// Creates a new event broadcaster.
func NewEventBroadcaster() EventBroadcaster {
	return &eventBroadcasterImpl{
		Broadcaster:   NewBroadcaster(maxQueuedEvents, WaitIfChannelFull),
		sleepDuration: defaultSleepDuration,
	}
}

// NewRecorder returns an EventRecorder that records events with the given event source.
func (e *eventBroadcasterImpl) NewRecorder() EventRecorder {
	return &recorderImpl{Broadcaster: e.Broadcaster}
}

func (e *eventBroadcasterImpl) StartSimpleEventWatcher() Interface {
	return e.StartEventWatcher(func(f func()) {
		f()
	})
}

// StartEventWatcher starts sending events received from this EventBroadcaster to the given event handler function.
// The return value can be ignored or used to stop recording, if desired.
func (e *eventBroadcasterImpl) StartEventWatcher(eventHandler func(func())) Interface {
	watcher := e.Watch()
	e.wg.Add(1)
	go func() {
		for watchEvent := range watcher.ResultChan() {
			eventHandler(watchEvent.f)
		}
		e.wg.Done()
	}()
	return watcher
}

func (e *eventBroadcasterImpl) Shutdown() {
	defer e.wg.Wait()
	e.Broadcaster.Shutdown()
}
