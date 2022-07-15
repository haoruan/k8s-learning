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

func (c completedConfig) NewAggregator() (*Aggregator, error) {
	s, err := c.NewServer("sample-aggregator")
	if err != nil {
		return nil, err
	}

	aggregator := &Aggregator{
		genericAPIServer: s,
		delegateHanlder:  s.Handler.NonGoRestfulMux,
	}

	aggregator.addAPIService()

	ws := installLegacyAPI()

	s.Handler.GoRestfulContainer.Add(ws)

	return aggregator, nil
}

func (s *Aggregator) addAPIService() error {
	proxyPath := "/api"
	proxyHandler := &proxyHandler{
		localDelegate: s.delegateHanlder,
	}

	s.genericAPIServer.Handler.NonGoRestfulMux.Handle(proxyPath, proxyHandler)

	return nil
}
