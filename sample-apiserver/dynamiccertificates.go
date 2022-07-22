package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"sync/atomic"
	"time"

	"k8s.io/client-go/util/cert"
	"k8s.io/client-go/util/workqueue"
)

const workItemKey = "key"

// Listener is an interface to use to notify interested parties of a change.
type Listener interface {
	// Enqueue should be called when an input may have changed
	Enqueue()
}

// DynamicServingCertificateController dynamically loads certificates and provides a golang tls compatible dynamic GetCertificate func.
type DynamicServingCertificateController struct {
	// baseTLSConfig is the static portion of the tlsConfig for serving to clients.  It is copied and the copy is mutated
	// based on the dynamic cert state.
	baseTLSConfig *tls.Config

	// clientCA provides the very latest content of the ca bundle
	clientCA *DynamicFileCAContent
	// servingCert provides the very latest content of the default serving certificate
	servingCert *DynamicCertKeyPairContent
	// sniCerts are a list of CertKeyContentProvider with associated names used for SNI
	// sniCerts []SNICertKeyContentProvider

	// currentlyServedContent holds the original bytes that we are serving. This is used to decide if we need to set a
	// new atomic value. The types used for efficient TLSConfig preclude using the processed value.
	currentlyServedContent *dynamicCertificateContent
	// currentServingTLSConfig holds a *tls.Config that will be used to serve requests
	currentServingTLSConfig atomic.Value

	// queue only ever has one item, but it has nice error handling backoff/retry semantics
	queue *workqueue.Type
	// eventRecorder events.EventRecorder
}

// dynamicCertificateContent holds the content that overrides the baseTLSConfig
type dynamicCertificateContent struct {
	// clientCA holds the content for the clientCA bundle
	clientCA    caBundleContent
	servingCert certKeyContent
	// sniCerts    []sniCertKeyContent
}

// caBundleContent holds the content for the clientCA bundle.  Wrapping the bytes makes the Equals work nicely with the
// method receiver.
type caBundleContent struct {
	caBundle []byte
}

// certKeyContent holds the content for the cert and key
type certKeyContent struct {
	cert []byte
	key  []byte
}

func NewDynamicServingCertificateController(
	baseTLSConfig *tls.Config,
	clientCA *DynamicFileCAContent,
	servingCert *DynamicCertKeyPairContent,
) *DynamicServingCertificateController {
	return &DynamicServingCertificateController{
		baseTLSConfig: baseTLSConfig,
		clientCA:      clientCA,
		servingCert:   servingCert,
		queue:         workqueue.New(),
	}
}

// GetConfigForClient is an implementation of tls.Config.GetConfigForClient
func (c *DynamicServingCertificateController) GetConfigForClient(clientHello *tls.ClientHelloInfo) (*tls.Config, error) {
	obj := c.currentServingTLSConfig.Load()
	if obj == nil {
		return nil, fmt.Errorf("dynamiccertificates: configuration not ready")
	}
	tlsConfig, ok := obj.(*tls.Config)
	if !ok {
		return nil, fmt.Errorf("dynamiccertificates: unexpected config type")
	}

	return tlsConfig, nil
}

// newTLSContent determines the next set of content for overriding the baseTLSConfig.
func (c *DynamicServingCertificateController) newTLSContent() (*dynamicCertificateContent, error) {
	newContent := &dynamicCertificateContent{}

	currClientCABundle := c.clientCA.CurrentCAContent()
	// we allow removing all client ca bundles because the server is still secure when this happens. it just means
	// that there isn't a hint to clients about which client-cert to used.  this happens when there is no client-ca
	// yet known for authentication, which can happen in aggregated apiservers and some kube-apiserver deployment modes.
	newContent.clientCA = caBundleContent{currClientCABundle}

	cert, key := c.servingCert.CurrentCertKeyContent()
	if len(cert) == 0 || len(key) == 0 {
		return nil, fmt.Errorf("not loading an empty serving certificate from %s", c.servingCert.Name())
	}

	newContent.servingCert = certKeyContent{cert, key}

	return newContent, nil
}

func (c *DynamicServingCertificateController) syncCert() error {
	newContent, err := c.newTLSContent()
	if err != nil {
		return err
	}

	// skip if tlscontent is same
	if c.currentlyServedContent != nil &&
		bytes.Equal(newContent.servingCert.cert, c.currentlyServedContent.servingCert.cert) &&
		bytes.Equal(newContent.servingCert.key, c.currentlyServedContent.servingCert.key) {
		return nil
	}

	newTLSConfigCopy := c.baseTLSConfig.Clone()

	newClientCAPool := x509.NewCertPool()
	newClientCAs, err := cert.ParseCertsPEM(newContent.clientCA.caBundle)
	if err != nil {
		return fmt.Errorf("unable to load client CA file %q: %v", string(newContent.clientCA.caBundle), err)
	}
	for _, cert := range newClientCAs {
		newClientCAPool.AddCert(cert)
	}

	newTLSConfigCopy.ClientCAs = newClientCAPool

	cert, err := tls.X509KeyPair(newContent.servingCert.cert, newContent.servingCert.key)
	if err != nil {
		return fmt.Errorf("invalid serving cert keypair: %v", err)
	}

	_, err = x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return fmt.Errorf("invalid serving cert: %v", err)
	}

	newTLSConfigCopy.Certificates = []tls.Certificate{cert}

	c.currentServingTLSConfig.Store(newTLSConfigCopy)
	c.currentlyServedContent = newContent

	return nil
}

func (c *DynamicServingCertificateController) RunOnce() {
	c.syncCert()
}

func (c *DynamicServingCertificateController) runWorker() {
	for c.processNextItem() {

	}
}

func (c *DynamicServingCertificateController) processNextItem() bool {
	key, shutdown := c.queue.Get()

	if shutdown {
		return false
	}

	defer c.queue.Done(key)

	c.syncCert()

	return true
}

func (c *DynamicServingCertificateController) Run(stopCh <-chan struct{}) {
	defer c.queue.ShutDown()

	go func() {
	loop:
		for {
			select {
			case <-stopCh:
				break loop
			default:
				c.queue.Add(workItemKey)
				time.Sleep(time.Minute)
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
				c.runWorker()
			}
		}
	}()

	<-stopCh
}

// Enqueue a method to allow separate control loops to cause the certificate controller to trigger and read content.
func (c *DynamicServingCertificateController) Enqueue() {
	c.queue.Add(workItemKey)
}
