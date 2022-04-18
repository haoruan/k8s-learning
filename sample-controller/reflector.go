package main

import (
	"errors"
	"fmt"
	"time"
)

var errorStopRequested = errors.New("stop requested")

// Reflector watches a specified resource and causes all changes to be reflected in the given store.
type Reflector struct {
	// name identifies this reflector. By default it will be a file:line if possible.
	name string

	// The name of the type we expect to place in the store. The name
	// will be the stringification of expectedGVK if provided, and the
	// stringification of expectedType otherwise. It is for display
	// only, and should not be used for parsing or comparison.
	expectedTypeName string
	// The destination to sync up with the watch source
	store Store
	// listerWatcher is used to perform lists and watches.
	listerWatcher ListerWatcher
}

func NewReflector(lw ListerWatcher, store Store) *Reflector {
	return &Reflector{
		name:             "sample-reflector",
		expectedTypeName: "sample-type",
		listerWatcher:    lw,
		store:            store,
	}
}

// Run repeatedly uses the reflector's ListAndWatch to fetch all the
// objects and subsequent deltas.
// Run will exit when stopCh is closed.
func (r *Reflector) Run(stopCh <-chan struct{}) {
	fmt.Printf("Starting reflector %s from %s\n", r.expectedTypeName, r.name)
loop:
	for {
		select {
		case <-stopCh:
			break loop
		default:
		}
		r.ListAndWatch(stopCh)

		time.Sleep(time.Second)
	}

	fmt.Printf("Stopping reflector %s from %s\n", r.expectedTypeName, r.name)
}

// ListAndWatch first lists all items and get the resource version at the moment of call,
// and then use the resource version to watch.
// It returns error if ListAndWatch didn't even try to initialize watch.
func (r *Reflector) ListAndWatch(stopCh <-chan struct{}) error {
	fmt.Printf("Listing and watching %v from %s\n", r.expectedTypeName, r.name)

	if err := func() error {
		listCh := make(chan struct{}, 1)
		var list []string
		go func() {
			list, _ = r.listerWatcher.List()
			close(listCh)
		}()

		select {
		case <-stopCh:
			return nil
		case <-listCh:
		}
		fmt.Printf("Objects listed\n")

		if err := r.syncWith(list); err != nil {
			return fmt.Errorf("unable to sync list result: %v", err)
		}
		fmt.Printf("SyncWith done\n")
		return nil
	}(); err != nil {
		return err
	}

	for {
		// give the stopCh a chance to stop the loop, even in case of continue statements further down on errors
		select {
		case <-stopCh:
			return nil
		default:
		}

		w, _ := r.listerWatcher.Watch()
		r.watchHandler(w, stopCh)
	}
}

// syncWith replaces the store's items with the given list.
func (r *Reflector) syncWith(items []string) error {
	found := make([]interface{}, 0, len(items))
	for _, item := range items {
		found = append(found, item)
	}
	return r.store.Replace(found)
}

// watchHandler watches w and keeps *resourceVersion up to date.
func (r *Reflector) watchHandler(w Interface, stopCh <-chan struct{}) error {
	eventCount := 0

	// Stopping the watcher should be idempotent and if we return from this function there's no way
	// we're coming back in with the same watch interface.
	defer w.Stop()

loop:
	for {
		select {
		case <-stopCh:
			return errorStopRequested
		case event, ok := <-w.ResultChan():
			if !ok {
				break loop
			}

			switch event.Type {
			case EventAdded:
				r.store.Add(event.f)
			case EventModified:
				r.store.Update(event.f)
			case EventDeleted:
				r.store.Delete(event.f)
			}
			eventCount++
		}
	}

	fmt.Printf("%s: Watch close - %v total %v items received\n", r.name, r.expectedTypeName, eventCount)
	return nil
}
