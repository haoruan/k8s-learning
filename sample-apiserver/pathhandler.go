package main

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
)

// prefixHandler holds the prefix it should match and the handler to use
type prefixHandler struct {
	// prefix is the prefix to test for a request match
	prefix string
	// handler is used to satisfy matching requests
	handler http.Handler
}

// pathHandler is an http.Handler that will satisfy requests first by exact match, then by prefix,
// then by notFoundHandler
type PathHandler struct {
	// muxName is used for logging so you can trace requests through
	muxName string

	lock sync.Mutex

	// pathToHandler is a map of exactly matching request to its handler
	pathToHandler map[string]http.Handler

	// this has to be sorted by most slashes then by length
	prefixHandlers []prefixHandler

	// notFoundHandler is the handler to use for satisfying requests with no other match
	notFoundHandler http.Handler
}

func NewPathHandler(name string) *PathHandler {
	return &PathHandler{
		muxName:         name,
		pathToHandler:   make(map[string]http.Handler),
		notFoundHandler: http.NotFoundHandler(),
	}
}

// Handle registers the handler for the given pattern.
// If a handler already exists for pattern, Handle panics.
func (m *PathHandler) Handle(path string, handler http.Handler) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.pathToHandler[path] = handler
}

func (m *PathHandler) HandlePrefix(path string, handler http.Handler) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.prefixHandlers = append(m.prefixHandlers, prefixHandler{path, handler})
}

// ServeHTTP makes it an http.Handler
func (h *PathHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if exactHandler, ok := h.pathToHandler[r.URL.Path]; ok {
		fmt.Printf("%v: %q satisfied by exact match\n", h.muxName, r.URL.Path)
		exactHandler.ServeHTTP(w, r)
		return
	}

	for _, prefixHandler := range h.prefixHandlers {
		if strings.HasPrefix(r.URL.Path, prefixHandler.prefix) {
			fmt.Printf("%v: %q satisfied by prefix %v\n", h.muxName, r.URL.Path, prefixHandler.prefix)
			prefixHandler.handler.ServeHTTP(w, r)
			return
		}
	}

	fmt.Printf("%v: %q satisfied by NotFoundHandler\n", h.muxName, r.URL.Path)
	h.notFoundHandler.ServeHTTP(w, r)
}
