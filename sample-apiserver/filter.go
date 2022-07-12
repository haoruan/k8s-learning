package main

import (
	"fmt"
	"net/http"
)

func WithAuthorization(handler http.Handler) http.Handler {
	fmt.Printf("Auhorizing...\n")
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		handler.ServeHTTP(w, req)
	})
}

func WithLogging(handler http.Handler) http.Handler {
	fmt.Printf("Logging...\n")
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		handler.ServeHTTP(w, req)
	})
}
