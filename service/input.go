package service

import (
	"context"
)

// Input is an interface implemented by Benthos inputs.
type Input interface {
	// TODO
	Close(ctx context.Context) error
}
