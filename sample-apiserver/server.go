package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"time"
)

const (
	defaultKeepAlivePeriod = 3 * time.Minute
)

func Run(stopCh <-chan struct{}) {
	s, err := CreateServerChain()
	if err != nil {
		fmt.Printf("%s\n", err)
		return
	}

	run(s.Handler.Director, stopCh)
}

func run(handler http.Handler, stopCh <-chan struct{}) {
	err := serve(handler, stopCh)
	if err != nil {
		fmt.Printf("%s\n", err)
	}
}

func serve(handler http.Handler, stopCh <-chan struct{}) error {
	//tlsConfig := &tls.Config{
	//	MinVersion: tls.VersionTLS12,
	//	NextProtos: []string{"h2", "http/1.1"},
	//}
	ln, err := net.Listen("tcp", ":3333")
	if err != nil {
		return err
	}

	secureServer := &http.Server{
		Addr:    "",
		Handler: handler,
		// TLSConfig:         tlsConfig,
		MaxHeaderBytes:    1 << 20,
		ReadHeaderTimeout: 32 * time.Second,
	}

	serverShutdownCh, err := runServer(secureServer, ln, stopCh)
	if err != nil {
		return err
	}

	<-serverShutdownCh

	return nil
}

// tcpKeepAliveListener sets TCP keep-alive timeouts on accepted
// connections. It's used by ListenAndServe and ListenAndServeTLS so
// dead TCP connections (e.g. closing laptop mid-download) eventually
// go away.
//
// Copied from Go 1.7.2 net/http/server.go
type tcpKeepAliveListener struct {
	net.Listener
}

func (ln tcpKeepAliveListener) Accept() (net.Conn, error) {
	c, err := ln.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if tc, ok := c.(*net.TCPConn); ok {
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(defaultKeepAlivePeriod)
	}
	return c, nil
}

func runServer(server *http.Server, ln net.Listener, stopCh <-chan struct{}) (<-chan struct{}, error) {
	// Shutdown server gracefully.
	serverShutdownCh := make(chan struct{})
	go func() {
		defer close(serverShutdownCh)
		<-stopCh
		ctx, cancel := context.WithCancel(context.Background())
		server.Shutdown(ctx)
		cancel()
	}()

	go func() {
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

	return serverShutdownCh, nil
}

func CreateServerChain() (*GenericAPIServer, error) {
	config := Config{}
	completeConfig := config.Complete()
	s, err := completeConfig.NewAPIServer()
	if err != nil {
		return nil, err
	}

	return s, nil
}

func CreateKubeAPIServer() {

}

func CreateAggregatorServer() {

}

func CreateAPIExtensionsServer() {

}
