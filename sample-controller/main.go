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

	listwatch := NewDefaultListWatch()
	informer := NewSharedIndexInformer(listwatch, NewCache(keyFunction))
	informer.AddEventHandler(&defaultResourceEventHanlder{})

	var wg sync.WaitGroup
	defer wg.Wait()

	wg.Add(1)
	go func() {
		defer wg.Done()

	loop:
		for {
			select {
			case <-stopCh:
				break loop
			default:
			}

			listwatch.SendEvent()
			time.Sleep(time.Second)
		}
	}()

	informer.Run(stopCh)
}
