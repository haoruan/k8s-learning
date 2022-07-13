package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"time"
)

func Run(stopCh <-chan struct{}) {
	CreateServerChain()
	run(stopCh)
}

func run(stopCh <-chan struct{}) {

}

func serve(handler http.Handler) {
	secureServer := &http.Server{
		Addr:              "",
		Handler:           handler,
		TLSConfig:         &tls.Config{},
		MaxHeaderBytes:    1 << 20,
		ReadHeaderTimeout: 32 * time.Second,
	}

	runServer(secureServer)
}

func runServer(server *http.Server, stopCh <-chan struct{}) {
	// Shutdown server gracefully.
	serverShutdownCh, listenerStoppedCh := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(serverShutdownCh)
		<-stopCh
		ctx, cancel := context.WithCancel(context.Background())
		server.Shutdown(ctx)
		cancel()
	}()

	go func() {
		defer close(listenerStoppedCh)

		var listener net.Listener
		listener = tcpKeepAliveListener{ln}
		if server.TLSConfig != nil {
			listener = tls.NewListener(listener, server.TLSConfig)
		}

		err := server.Serve(listener)

		msg := fmt.Sprintf("Stopped listening on %s", ln.Addr().String())
		select {
		case <-stopCh:
			fmt.Println(msg)
		default:
			panic(fmt.Sprintf("%s due to error: %v", msg, err))
		}
	}()

	return serverShutdownCh, listenerStoppedCh, nil
}

func CreateServerChain() {
	config := Config{}
	completeConfig := config.Complete()
	completeConfig.NewAPIServer()
}

func CreateKubeAPIServer() {

}

func CreateAggregatorServer() {

}

func CreateAPIExtensionsServer() {

}
