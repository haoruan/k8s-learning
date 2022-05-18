package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

var onlyOneSignalHandler = make(chan struct{})
var shutdownSignals = []os.Signal{os.Interrupt, syscall.SIGTERM}

func SetupSignalContext() context.Context {
	close(onlyOneSignalHandler)

	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 2)
	signal.Notify(c, shutdownSignals...)
	go func() {
		<-c
		cancel()
		<-c
		os.Exit(1)
	}()

	return ctx
}

func main() {
	queue := &PriorityQueue{}
	ctx := SetupSignalContext()
	sched := NewScheduler(queue)
	sched.Run(ctx)
}
