package main

import (
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var (
	shutdownSignals      = []os.Signal{os.Interrupt, syscall.SIGTERM}
	onlyOneSignalHandler = make(chan struct{})
)

func SetupSignalHandler() (stopCh <-chan struct{}) {
	close(onlyOneSignalHandler) // panics when called twice

	stop := make(chan struct{})
	c := make(chan os.Signal, 2)
	signal.Notify(c, shutdownSignals...)
	go func() {
		<-c
		close(stop)
		<-c
		os.Exit(1) // second signal. Exit directly.
	}()

	return stop
}

func main() {
	stopCh := SetupSignalHandler()

	listwatch := &defaultListWatch{}
	informer := NewSharedIndexInformer(listwatch, "Example", NewCache(keyFunction))
	informer.AddEventHandler(&defaultResourceEventHanlder{})

	informer.Run(stopCh)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		select {
		case <-stopCh:
			return
		default:
		}

		listwatch.SendEvent()
		time.Sleep(time.Second)
	}()

	wg.Wait()
}
