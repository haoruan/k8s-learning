package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

var (
	shutdownSignals      = []os.Signal{os.Interrupt, syscall.SIGTERM}
	onlyOneSignalHandler = make(chan struct{})
)

func SetupSignalContext() context.Context {
	close(onlyOneSignalHandler) // panics when called twice

	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 2)
	signal.Notify(c, shutdownSignals...)
	go func() {
		<-c
		cancel()
		<-c
		os.Exit(1) // second signal. Exit directly.
	}()

	return ctx
}

func main() {
	ctx := SetupSignalContext()
	gc, _ := NewGarbageCollector()
	GenerateEvent(gc, ctx)
	gc.Run(ctx, 1)
}
