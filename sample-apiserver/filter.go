package main

import (
	"fmt"
	"net/http"
)

func WithAuthorization(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		fmt.Printf("Auhorizing...\n")
		handler.ServeHTTP(w, req)
	})
}

func WithLogging(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		fmt.Printf("Logging...\n")
		handler.ServeHTTP(w, req)
	})
}
