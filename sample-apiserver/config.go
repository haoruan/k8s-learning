package main

import "net/http"

// GenericAPIServer contains state for a Kubernetes cluster api server.
type GenericAPIServer struct {
	// "Outputs"
	// Handler holds the handlers being used by this API server
	Handler *APIServerHandler
}

type Config struct {
	// BuildHandlerChainFunc allows you to build custom handler chains by decorating the apiHandler.
	BuildHandlerChainFunc func(apiHandler http.Handler, c *Config) (secure http.Handler)
}

type completedConfig struct {
	*Config
}

type CompletedConfig struct {
	// Embed a private pointer that cannot be instantiated outside of this package.
	*completedConfig
}

func (c *Config) Complete() CompletedConfig {
	return CompletedConfig{
		&completedConfig{c},
	}
}

func DefaultBuildHandlerChain(apiHandler http.Handler, c *Config) http.Handler {
	handler := WithAuthorization(apiHandler)
	handler = WithLogging(handler)

	return handler
}

func NewConfig() *Config {
	return &Config{
		BuildHandlerChainFunc: DefaultBuildHandlerChain,
	}
}

func installAPI(s *GenericAPIServer) {
	Version{"1", "0"}.Install(s.Handler.GoRestfulContainer)
}

func (c *completedConfig) NewServer(name string) (*GenericAPIServer, error) {
	handlerChainBuilder := func(handler http.Handler) http.Handler {
		return c.BuildHandlerChainFunc(handler, c.Config)
	}

	apiServerHandler := NewAPIServerHandler(name, handlerChainBuilder)

	s := &GenericAPIServer{
		Handler: apiServerHandler,
	}

	installAPI(s)

	return s, nil
}
