package main

import "net/http"

type Aggregator struct {
	genericAPIServer *GenericAPIServer
	delegateHanlder  http.Handler
}

type proxyHandler struct {
	localDelegate http.Handler
}

func (p *proxyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.localDelegate.ServeHTTP(w, r)
}

func (c completedConfig) NewAggregator(delegationTarget DelegationTarget) (*Aggregator, error) {
	s, err := c.NewServer("sample-aggregator")
	if err != nil {
		return nil, err
	}

	aggregator := &Aggregator{
		genericAPIServer: s,
		delegateHanlder:  delegationTarget.UnprotectedHanlder(),
	}

	aggregator.addAPIService()

	return aggregator, nil
}

func (a *Aggregator) addAPIService() error {
	proxyPath := "/api"
	proxyHandler := &proxyHandler{
		localDelegate: a.delegateHanlder,
	}

	a.genericAPIServer.Handler.NonGoRestfulMux.Handle(proxyPath, proxyHandler)
	a.genericAPIServer.Handler.NonGoRestfulMux.HandlePrefix(proxyPath+"/", proxyHandler)

	return nil
}
