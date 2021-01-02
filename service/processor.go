package service

import (
	"context"
)

// ProcessorFunc is a minimal implementation of a Benthos processor, able to
// mutate a message or return an error if the message could not be processed. If
// both the returned Message and error are nil then the message is filtered.
type ProcessorFunc func(context.Context, Message) (Message, error)

// SimpleProcessor is a Benthos processor implementation that works against
// single messages and includes methods for cleanly shutting down.
type SimpleProcessor interface {
	// Process a message or return an error if the message could not be
	// processed. If both the returned Message and error are nil then the
	// message is filtered.
	Process(context.Context, Message) (Message, error)

	Close(ctx context.Context) error
}

// SimpleProcessorConstructor is a func that's provided a configuration type and
// access to a service manager and must return an instantiation of a processor
// based on the config, or an error.
type SimpleProcessorConstructor func(label string, conf interface{}, mgr Manager) (SimpleProcessor, error)

//------------------------------------------------------------------------------

// BatchProcessorFunc is a minimal implementation of a Benthos batch processor,
// able to mutate a batch of messages or return an error if the messages could
// not be processed.
type BatchProcessorFunc func(context.Context, []Message) ([]Message, error)

// BatchProcessor is a Benthos processor implementation that works against
// batches of messages, which allows windowed processing and includes methods
// for cleanly shutting down.
type BatchProcessor interface {
	// Process a batch of messages or return an error if the messages could not
	// be processed.
	ProcessBatch(context.Context, []Message) ([]Message, error)

	Close(ctx context.Context) error
}

// BatchProcessorConstructor is a func that's provided a configuration type and
// access to a service manager and must return an instantiation of a processor
// based on the config, or an error.
type BatchProcessorConstructor func(label string, conf interface{}, mgr Manager) (BatchProcessor, error)
