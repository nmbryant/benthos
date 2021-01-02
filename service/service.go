package service

import (
	"github.com/Jeffail/benthos/v3/internal/service"
)

// Type defines an executable Benthos environment, including a registry of all
// available components and plugins, and provides methods for executing that
// environment as a service.
type Type struct {
	name string

	// TODO: Add component registrar, includes plugins if nil after opts then we
	// use standard set.
	mgr *service.Manager
}

// OptFunc is a function signature used to define options for the service.Type
// constructor.
type OptFunc func(t *Type)

// New constructs a new Benthos service environment.
func New(opts ...OptFunc) (*Type, error) {
	t := &Type{
		name: "benthos",
	}
	for _, opt := range opts {
		opt(t)
	}
	return t, nil
}

//------------------------------------------------------------------------------

// RunCLI executes Benthos as a CLI service, allowing users to specify a
// configuration via flags. This is how a standard distribution of Benthos
// operates.
//
// This call blocks until either the pipeline shuts down or a termination signal
// is received.
func (t *Type) RunCLI() {
	panic("not implemented")
}

//------------------------------------------------------------------------------

// RegisterCache attempts to register a new cache plugin by providing an
// optional constructor for a configuration struct for the plugin as well as a
// constructor for the cache itself. The constructor will be called for each
// instantiation of the component within a config.
func (t *Type) RegisterCache(name string, confCtor ConfigConstructor, cCtor CacheConstructor) error {
	panic("not implemented")
}

// RegisterRateLimit attempts to register a new rate limit plugin by providing
// an optional constructor for a configuration struct for the plugin as well as
// a constructor for the rate limit itself. The constructor will be called for
// each instantiation of the component within a config.
func (t *Type) RegisterRateLimit(name string, confCtor ConfigConstructor, cCtor RateLimitConstructor) error {
	panic("not implemented")
}

// RegisterProcessorFunc attempts to register a new processor plugin by
// providing a func to operate on each message. Depending on a configuration
// this func could be called by multiple parallel components, and therefore
// should be thread safe.
func (t *Type) RegisterProcessorFunc(name string, fn ProcessorFunc) error {
	panic("not implemented")
}

// RegisterProcessor attempts to register a new processor plugin by providing an
// optional constructor for a configuration struct for the plugin as well as a
// constructor for the processor itself. The constructor will be called for each
// instantiation of the component within a config.
func (t *Type) RegisterProcessor(name string, confCtor ConfigConstructor, cCtor SimpleProcessorConstructor) error {
	panic("not implemented")
}

// RegisterBatchProcessorFunc attempts to register a new batch processor plugin
// by providing a func to operate on each message. Depending on a configuration
// this func could be called by multiple parallel components, and therefore
// should be thread safe.
func (t *Type) RegisterBatchProcessorFunc(name string, fn BatchProcessorFunc) error {
	panic("not implemented")
}

// RegisterBatchProcessor attempts to register a new processor plugin by
// providing an optional constructor for a configuration struct for the plugin
// as well as a constructor for the processor itself. The constructor will be
// called for each instantiation of the component within a config.
func (t *Type) RegisterBatchProcessor(name string, confCtor ConfigConstructor, cCtor BatchProcessorConstructor) error {
	panic("not implemented")
}
