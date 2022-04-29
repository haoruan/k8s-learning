package main

import (
	"context"
	"time"
)

func add_and_delete_event(gc *GarbageCollector) {
	addevent := &event{
		eventType: addEvent,
		obj: &GCObject{
			uid:       "add-1",
			status:    "beingDeleted",
			finalizer: FinalizerDeleteDependents,
		},
	}

	gc.dependencyGraphBuilder.graphChanges.Add(addevent)
}

func add_then_delete_event(gc *GarbageCollector) {
	addevent := &event{
		eventType: addEvent,
		obj: &GCObject{
			uid: "add-2",
		},
	}

	gc.dependencyGraphBuilder.graphChanges.Add(addevent)

	addevent = &event{
		eventType: addEvent,
		obj: &GCObject{
			uid:       "add-2",
			status:    "beingDeleted",
			finalizer: FinalizerDeleteDependents,
		},
	}

	gc.dependencyGraphBuilder.graphChanges.Add(addevent)
}

func add_with_owners_event(gc *GarbageCollector) {
	parentevent := &event{
		eventType: addEvent,
		obj: &GCObject{
			uid: "add-parent-1",
		},
	}

	childevent := &event{
		eventType: addEvent,
		obj: &GCObject{
			uid: "add-child-1",
			owners: []owner{
				{
					uid:                "add-parent-1",
					BlockOwnerDeletion: true,
				},
			},
		},
	}

	gc.dependencyGraphBuilder.graphChanges.Add(parentevent)
	gc.dependencyGraphBuilder.graphChanges.Add(childevent)

	parentevent = &event{
		eventType: addEvent,
		obj: &GCObject{
			uid:       "add-parent-1",
			status:    "beingDeleted",
			finalizer: FinalizerDeleteDependents,
		},
	}

	gc.dependencyGraphBuilder.graphChanges.Add(parentevent)

}

func GenerateEvent(gc *GarbageCollector, ctx context.Context) {
	go func() {
	loop:
		for {
			select {
			case <-ctx.Done():
				break loop
			default:
			}

			add_and_delete_event(gc)
			add_then_delete_event(gc)

			time.Sleep(time.Second)
		}
	}()
}
