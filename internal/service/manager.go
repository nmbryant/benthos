package service

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

// ErrResourceNotFound represents an error where a named resource could not be
// accessed because it was not found by the manager.
type ErrResourceNotFound string

// Error implements the standard error interface.
func (e ErrResourceNotFound) Error() string {
	return fmt.Sprintf("unable to locate resource: %v", string(e))
}

//------------------------------------------------------------------------------

// APIReg is an interface representing an API builder.
type APIReg interface {
	RegisterEndpoint(path, desc string, h http.HandlerFunc)
}

//------------------------------------------------------------------------------

// Manager is shared with all components within a Benthos pipeline in order to
// allow them to access constructors and initialized resources of all Benthos
// component types.
//
// Manager also provides access to observability components and allows
// components to register endpoints on the service-wide HTTP server.
type Manager struct {
	apiReg APIReg

	inputs       map[string]types.Input
	caches       map[string]types.Cache
	processors   map[string]types.Processor
	outputs      map[string]types.OutputWriter
	rateLimits   map[string]types.RateLimit
	plugins      map[string]interface{}
	resourceLock sync.RWMutex

	logger log.Modular
	stats  metrics.Type

	pipes    map[string]<-chan types.Transaction
	pipeLock sync.RWMutex
}

// NewManager returns an instance of a Manager which can be shared amongst
// Benthos types.
func NewManager(apiReg APIReg, log log.Modular, stats metrics.Type) (*Manager, error) {
	t := &Manager{
		apiReg:     apiReg,
		inputs:     map[string]types.Input{},
		caches:     map[string]types.Cache{},
		processors: map[string]types.Processor{},
		outputs:    map[string]types.OutputWriter{},
		rateLimits: map[string]types.RateLimit{},
		plugins:    map[string]interface{}{},
		pipes:      map[string]<-chan types.Transaction{},
	}

	return t, nil
}

//------------------------------------------------------------------------------

// RegisterEndpoint registers a server wide HTTP endpoint.
func (t *Manager) RegisterEndpoint(path, desc string, h http.HandlerFunc) {
	t.apiReg.RegisterEndpoint(path, desc, h)
}

//------------------------------------------------------------------------------

// GetPipe attempts to obtain and return a named output Pipe
func (t *Manager) GetPipe(name string) (<-chan types.Transaction, error) {
	t.pipeLock.RLock()
	pipe, exists := t.pipes[name]
	t.pipeLock.RUnlock()
	if exists {
		return pipe, nil
	}
	return nil, types.ErrPipeNotFound
}

// SetPipe registers a new transaction chan to a named pipe.
func (t *Manager) SetPipe(name string, tran <-chan types.Transaction) {
	t.pipeLock.Lock()
	t.pipes[name] = tran
	t.pipeLock.Unlock()
}

// UnsetPipe removes a named pipe transaction chan.
func (t *Manager) UnsetPipe(name string, tran <-chan types.Transaction) {
	t.pipeLock.Lock()
	if otran, exists := t.pipes[name]; exists && otran == tran {
		delete(t.pipes, name)
	}
	t.pipeLock.Unlock()
}

//------------------------------------------------------------------------------

// AccessInput attempts to access an input resource by a unique identifier and
// executes a closure function with the input as an argument. Returns an error
// if the input does not exist (or is otherwise inaccessible).
//
// During the execution of the provided closure it is guaranteed that the
// resource will not be closed or removed. However, it is possible for the
// resource to be accessed by any number of components in parallel.
func (t *Manager) AccessInput(name string, fn func(i types.Input)) error {
	t.resourceLock.RLock()
	defer t.resourceLock.RUnlock()
	i, ok := t.inputs[name]
	if !ok {
		return ErrResourceNotFound(name)
	}
	fn(i)
	return nil
}

// NewInput constructs a new input from a config.
func (t *Manager) NewInput(label string, conf input.Config) (types.Input, error) {
	return input.New(conf, nil, t.logger, t.stats) // TODO
}

// StoreInput attempts to store a new input resource. If an existing resource
// has the same name it is closed and removed _after_ the new one is
// successfully initialized.
func (t *Manager) StoreInput(ctx context.Context, name string, conf input.Config) error {
	t.resourceLock.Lock()
	defer t.resourceLock.Unlock()

	newInput, err := t.NewInput("resource."+name, conf)
	if err != nil {
		return err
	}

	i, ok := t.inputs[name]
	if ok {
		i.CloseAsync()
		_ = i.WaitForClose(time.Second) // TODO
	}

	t.inputs[name] = newInput
	return nil
}

//------------------------------------------------------------------------------

// CloseAsync triggers the shut down of all resource types that implement the
// lifetime interface types.Closable.
func (t *Manager) CloseAsync() {
	t.resourceLock.Lock()
	defer t.resourceLock.Unlock()

	for _, c := range t.inputs {
		c.CloseAsync()
	}
	for _, c := range t.caches {
		c.CloseAsync()
	}
	for _, p := range t.processors {
		p.CloseAsync()
	}
	for _, c := range t.plugins {
		if closer, ok := c.(types.Closable); ok {
			closer.CloseAsync()
		}
	}
	for _, c := range t.rateLimits {
		c.CloseAsync()
	}
	for _, c := range t.outputs {
		c.CloseAsync()
	}
}

// WaitForClose blocks until either all closable resource types are shut down or
// a timeout occurs.
func (t *Manager) WaitForClose(timeout time.Duration) error {
	t.resourceLock.Lock()
	defer t.resourceLock.Unlock()

	timesOut := time.Now().Add(timeout)
	for k, c := range t.inputs {
		if err := c.WaitForClose(time.Until(timesOut)); err != nil {
			return fmt.Errorf("resource '%s' failed to cleanly shutdown: %v", k, err)
		}
		delete(t.inputs, k)
	}
	for k, c := range t.caches {
		if err := c.WaitForClose(time.Until(timesOut)); err != nil {
			return fmt.Errorf("resource '%s' failed to cleanly shutdown: %v", k, err)
		}
		delete(t.caches, k)
	}
	for k, p := range t.processors {
		if err := p.WaitForClose(time.Until(timesOut)); err != nil {
			return fmt.Errorf("resource '%s' failed to cleanly shutdown: %v", k, err)
		}
		delete(t.processors, k)
	}
	for k, c := range t.rateLimits {
		if err := c.WaitForClose(time.Until(timesOut)); err != nil {
			return fmt.Errorf("resource '%s' failed to cleanly shutdown: %v", k, err)
		}
		delete(t.rateLimits, k)
	}
	for k, c := range t.outputs {
		if err := c.WaitForClose(time.Until(timesOut)); err != nil {
			return fmt.Errorf("resource '%s' failed to cleanly shutdown: %v", k, err)
		}
		delete(t.outputs, k)
	}
	for k, c := range t.plugins {
		if closer, ok := c.(types.Closable); ok {
			if err := closer.WaitForClose(time.Until(timesOut)); err != nil {
				return fmt.Errorf("resource '%s' failed to cleanly shutdown: %v", k, err)
			}
		}
		delete(t.plugins, k)
	}
	return nil
}

//------------------------------------------------------------------------------
