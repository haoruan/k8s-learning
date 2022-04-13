package main

import "sync"

// FullChannelBehavior controls how the Broadcaster reacts if a watcher's watch
// channel is full.
type FullChannelBehavior int

const blockQueueMarker = "block-queue-marker"

const (
	WaitIfChannelFull FullChannelBehavior = iota
	DropIfChannelFull
)

type Broadcaster struct {
	watchers     map[int64]*broadcasterWatcher
	nextWatcher  int64
	distributing sync.WaitGroup

	incoming chan Event
	stopped  chan struct{}

	// How large to make watcher's channel.
	watchQueueLength int
	// If one of the watch channels is full, don't wait for it to become empty.
	// Instead just deliver it to the watchers that do have space in their
	// channels and move on to the next event.
	// It's more fair to do this on a per-watcher basis than to do it on the
	// "incoming" channel, which would allow one slow watcher to prevent all
	// other watchers from getting new events.
	fullChannelBehavior FullChannelBehavior
}

// Buffer the incoming queue a little bit even though it should rarely ever accumulate
// anything, just in case a few events are received in such a short window that
// Broadcaster can't move them onto the watchers' queues fast enough.
const incomingQueueLength = 25

// NewBroadcaster creates a new Broadcaster. queueLength is the maximum number of events to queue per watcher.
// It is guaranteed that events will be distributed in the order in which they occur,
// but the order in which a single event is distributed among all of the watchers is unspecified.
func NewBroadcaster(queueLength int, fullChannelBehavior FullChannelBehavior) *Broadcaster {
	m := &Broadcaster{
		watchers:            map[int64]*broadcasterWatcher{},
		incoming:            make(chan Event, incomingQueueLength),
		stopped:             make(chan struct{}),
		watchQueueLength:    queueLength,
		fullChannelBehavior: fullChannelBehavior,
	}
	m.distributing.Add(1)
	go m.loop()
	return m
}

// Shutdown disconnects all watchers (but any queued events will still be distributed).
// You must not call Action or Watch* after calling Shutdown. This call blocks
// until all events have been distributed through the outbound channels. Note
// that since they can be buffered, this means that the watchers might not
// have received the data yet as it can remain sitting in the buffered
// channel. It will block until the broadcaster stop request is actually executed
func (m *Broadcaster) Shutdown() {
	m.blockQueue(func() {
		close(m.stopped)
		close(m.incoming)
	})
	m.distributing.Wait()
}

// Execute f, blocking the incoming queue (and waiting for it to drain first).
// The purpose of this terrible hack is so that watchers added after an event
// won't ever see that event, and will always see any event after they are
// added.
func (m *Broadcaster) blockQueue(f func()) {
	select {
	case <-m.stopped:
		return
	default:
	}

	var wg sync.WaitGroup
	wg.Add(1)
	m.incoming <- Event{
		Type: blockQueueMarker,
		f: func() {
			defer wg.Done()
			f()
		},
	}
	wg.Wait()
}

// Watch adds a new watcher to the list and returns an Interface for it.
// Note: new watchers will only receive new events. They won't get an entire history
// of previous events. It will block until the watcher is actually added to the
// broadcaster.
func (m *Broadcaster) Watch() Interface {
	var w *broadcasterWatcher
	m.blockQueue(func() {
		id := m.nextWatcher
		m.nextWatcher++
		w = &broadcasterWatcher{
			result:  make(chan Event, m.watchQueueLength),
			stopped: make(chan struct{}),
			id:      id,
			m:       m,
		}
		m.watchers[id] = w
	})
	if w == nil {
		// The panic here is to be consistent with the previous interface behavior
		// we are willing to re-evaluate in the future.
		panic("broadcaster already stopped")
	}
	return w
}

// loop receives from m.incoming and distributes to all watchers.
func (m *Broadcaster) loop() {
	// Deliberately not catching crashes here. Yes, bring down the process if there's a
	// bug in watch.Broadcaster.
	for event := range m.incoming {
		if event.Type == blockQueueMarker {
			event.f()
			continue
		}
		m.distribute(event)
	}
	m.closeAll()
	m.distributing.Done()
}

// closeAll disconnects all watchers (presumably in response to a Shutdown call).
func (m *Broadcaster) closeAll() {
	for _, w := range m.watchers {
		close(w.result)
	}
	// Delete everything from the map, since presence/absence in the map is used
	// by stopWatching to avoid double-closing the channel.
	m.watchers = map[int64]*broadcasterWatcher{}
}

// distribute sends event to all watchers. Blocking.
func (m *Broadcaster) distribute(event Event) {
	if m.fullChannelBehavior == DropIfChannelFull {
		for _, w := range m.watchers {
			select {
			case w.result <- event:
			case <-w.stopped:
			default: // Don't block if the event can't be queued.
			}
		}
	} else {
		for _, w := range m.watchers {
			select {
			case w.result <- event:
			case <-w.stopped:
			}
		}
	}
}

// stopWatching stops the given watcher and removes it from the list.
func (m *Broadcaster) stopWatching(id int64) {
	m.blockQueue(func() {
		w, ok := m.watchers[id]
		if !ok {
			// No need to do anything, it's already been removed from the list.
			return
		}
		delete(m.watchers, id)
		close(w.result)
	})
}

// Action distributes the given event among all watchers.
func (m *Broadcaster) Action(action EventType, f func()) {
	m.incoming <- Event{action, f}
}
