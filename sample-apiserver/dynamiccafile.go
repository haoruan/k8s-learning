package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"sync/atomic"

	"github.com/fsnotify/fsnotify"
	"k8s.io/client-go/util/workqueue"
)

// DynamicFileCAContent provides a CAContentProvider that can dynamically react to new file content
// It also fulfills the authenticator interface to provide verifyoptions
type DynamicFileCAContent struct {
	name string

	// filename is the name the file to read.
	filename string

	// caBundle is a caBundleAndVerifier that contains the last read, non-zero length content of the file
	caBundle atomic.Value

	listeners []Listener

	// queue only ever has one item, but it has nice error handling backoff/retry semantics
	queue *workqueue.Type
}

func NewDynamicFileCAContent(name, filename string) *DynamicFileCAContent {
	return &DynamicFileCAContent{
		name:     name,
		filename: filename,
		queue:    workqueue.New(),
	}
}

func (c *DynamicFileCAContent) Run(stopCh <-chan struct{}) {
	defer c.queue.ShutDown()

	c.RunOnce()

	go func() {
	loop:
		for {
			select {
			case <-stopCh:
				break loop
			default:
				c.runWorker()
			}
		}
	}()

	go func() {
	loop:
		for {
			select {
			case <-stopCh:
				break loop
			default:
				c.watchCAFile(stopCh)
			}
		}
	}()
}

func (c *DynamicFileCAContent) watchCAFile(stopCh <-chan struct{}) error {
	// Trigger a check here to ensure the content will be checked periodically even if the following watch fails.
	c.queue.Add(workItemKey)

	w, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("error creating fsnotify watcher: %v", err)
	}
	defer w.Close()

	// Trigger a check in case the file is updated before the watch starts.
	c.queue.Add(workItemKey)

	for {
		select {
		case e := <-w.Events:
			if err := c.handleWatchEvent(e, w); err != nil {
				return err
			}
		case err := <-w.Errors:
			return fmt.Errorf("received fsnotify error: %v", err)
		case <-stopCh:
			return nil
		}
	}
}

func (c *DynamicFileCAContent) handleWatchEvent(e fsnotify.Event, w *fsnotify.Watcher) error {
	// This should be executed after restarting the watch (if applicable) to ensure no file event will be missing.
	defer c.queue.Add(workItemKey)
	if e.Op&(fsnotify.Remove|fsnotify.Rename) == 0 {
		return nil
	}
	if err := w.Remove(e.Name); err != nil {
		fmt.Printf("Failed to remove file watch, it may have been deleted: %s, %v\n", e.Name, err)
	}
	if err := w.Add(e.Name); err != nil {
		return fmt.Errorf("error adding watch for file %s: %v", e.Name, err)
	}
	return nil
}

func (c *DynamicFileCAContent) processNextItem() bool {
	obj, shutdown := c.queue.Get()
	if shutdown {
		return false
	}

	defer c.queue.Done(obj)

	c.loadCAFile()

	for _, listener := range c.listeners {
		listener.Enqueue()
	}

	return true
}

func (c *DynamicFileCAContent) runWorker() {
	for c.processNextItem() {

	}
}

func (c *DynamicFileCAContent) RunOnce() {
	c.loadCAFile()
}

// Name is just an identifier
func (c *DynamicFileCAContent) Name() string {
	return c.name
}

func (c *DynamicFileCAContent) loadCAFile() error {
	caBundle, err := ioutil.ReadFile(c.filename)
	if err != nil {
		return err
	}

	if len(caBundle) == 0 {
		return fmt.Errorf("missing content for ca cert %q", c.Name())
	}

	newCaBundleContent := &caBundleContent{caBundle}

	existing, ok := c.caBundle.Load().(*caBundleContent)
	if ok && existing != nil && bytes.Equal(newCaBundleContent.caBundle, existing.caBundle) {
		return nil
	}

	c.caBundle.Store(newCaBundleContent)
	return nil
}

func (c *DynamicFileCAContent) AddListener(l Listener) {
	c.listeners = append(c.listeners, l)
}

func (c *DynamicFileCAContent) CurrentCAContent() []byte {
	caBundleContent := c.caBundle.Load().(*caBundleContent)
	return caBundleContent.caBundle
}
