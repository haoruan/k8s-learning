package main

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"sync/atomic"

	"github.com/fsnotify/fsnotify"
	"k8s.io/client-go/util/workqueue"
)

// DynamicCertKeyPairContent provides a CertKeyContentProvider that can dynamically react to new file content
type DynamicCertKeyPairContent struct {
	name string

	// certFile is the name of the certificate file to read.
	certFile string
	// keyFile is the name of the key file to read.
	keyFile string

	// certKeyPair is a certKeyContent that contains the last read, non-zero length content of the key and cert
	certKeyPair atomic.Value

	listeners []Listener

	// queue only ever has one item, but it has nice error handling backoff/retry semantics
	queue *workqueue.Type
}

func NewDynamicCertKeyPairContent(name, certFile, keyFile string) *DynamicCertKeyPairContent {
	return &DynamicCertKeyPairContent{
		name:     name,
		certFile: certFile,
		keyFile:  keyFile,
		queue:    workqueue.New(),
	}
}

// Name is just an identifier
func (c *DynamicCertKeyPairContent) Name() string {
	return c.name
}

func (c *DynamicCertKeyPairContent) RunOnce() {
	c.loadCertKeyPair()
}

func (c *DynamicCertKeyPairContent) runWorker() {
	for c.processNextItem() {

	}
}

func (c *DynamicCertKeyPairContent) processNextItem() bool {
	key, shutdown := c.queue.Get()
	if shutdown {
		return false
	}

	defer c.queue.Done(key)

	c.loadCertKeyPair()

	for _, listener := range c.listeners {
		listener.Enqueue()
	}

	return true
}

func (c *DynamicCertKeyPairContent) Run(stopCh <-chan struct{}) {
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
				c.watchCertKeyFile(stopCh)
			}
		}
	}()
}

func (c *DynamicCertKeyPairContent) watchCertKeyFile(stopCh <-chan struct{}) error {
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

// handleWatchEvent triggers reloading the cert and key file, and restarts a new watch if it's a Remove or Rename event.
// If one file is updated before the other, the loadCertKeyPair method will catch the mismatch and will not apply the
// change. When an event of the other file is received, it will trigger reloading the files again and the new content
// will be loaded and used.
func (c *DynamicCertKeyPairContent) handleWatchEvent(e fsnotify.Event, w *fsnotify.Watcher) error {
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

// loadCertKeyPair determines the next set of content for the file.
func (c *DynamicCertKeyPairContent) loadCertKeyPair() error {
	cert, err := ioutil.ReadFile(c.certFile)
	if err != nil {
		return err
	}
	key, err := ioutil.ReadFile(c.keyFile)
	if err != nil {
		return err
	}
	if len(cert) == 0 || len(key) == 0 {
		return fmt.Errorf("missing content for serving cert %q", c.Name())
	}

	newCertKey := &certKeyContent{
		cert: cert,
		key:  key,
	}

	// check to see if we have a change. If the values are the same, do nothing.
	existing, ok := c.certKeyPair.Load().(*certKeyContent)
	if ok && existing != nil && bytes.Equal(existing.cert, newCertKey.cert) && bytes.Equal(existing.key, newCertKey.key) {
		return nil
	}

	// Ensure that the key matches the cert and both are valid
	_, err = tls.X509KeyPair(cert, key)
	if err != nil {
		return err
	}

	c.certKeyPair.Store(newCertKey)
	fmt.Printf("Loaded a new cert/key pair: %s\n", c.Name())

	for _, listener := range c.listeners {
		listener.Enqueue()
	}

	return nil
}

func (c *DynamicCertKeyPairContent) AddListener(l Listener) {
	c.listeners = append(c.listeners, l)
}

// CurrentCertKeyContent provides cert and key byte content
func (c *DynamicCertKeyPairContent) CurrentCertKeyContent() ([]byte, []byte) {
	certKeyContent := c.certKeyPair.Load().(*certKeyContent)
	return certKeyContent.cert, certKeyContent.key
}
