package main

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
)

// pathHandler is an http.Handler that will satisfy requests first by exact match, then by prefix,
// then by notFoundHandler
type PathHandler struct {
	// muxName is used for logging so you can trace requests through
	muxName string

	lock sync.Mutex

	// pathToHandler is a map of exactly matching request to its handler
	pathToHandler map[string]http.Handler

	// this has to be sorted by most slashes then by length
	prefixHandler map[string]http.Handler

	// notFoundHandler is the handler to use for satisfying requests with no other match
	notFoundHandler http.Handler
}

func NewPathHandler(name string) *PathHandler {
	return &PathHandler{
		muxName:         name,
		pathToHandler:   make(map[string]http.Handler),
		prefixHandler:   make(map[string]http.Handler),
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
	if !strings.HasSuffix(path, "/") {
		panic(fmt.Sprintf("%q must end in a trailing slash", path))
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	m.prefixHandler[path] = handler
}

// ServeHTTP makes it an http.Handler
func (h *PathHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if exactHandler, ok := h.pathToHandler[r.URL.Path]; ok {
		fmt.Printf("%v: %q satisfied by exact match\n", h.muxName, r.URL.Path)
		exactHandler.ServeHTTP(w, r)
		return
	}

	for prefix, handler := range h.prefixHandler {
		if strings.HasPrefix(r.URL.Path, prefix) {
			fmt.Printf("%v: %q satisfied by prefix %v\n", h.muxName, r.URL.Path, prefix)
			handler.ServeHTTP(w, r)
			return
		}
	}

	fmt.Printf("%v: %q satisfied by NotFoundHandler\n", h.muxName, r.URL.Path)
	h.notFoundHandler.ServeHTTP(w, r)
}
