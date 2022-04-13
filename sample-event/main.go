package main

func main() {
	eventBroadcaster := NewEventBroadcaster()
	defer eventBroadcaster.Shutdown()

	eventBroadcaster.StartSimpleEventWatcher()
	recorder := eventBroadcaster.NewRecorder()

	recorder.Event(Added)
}
