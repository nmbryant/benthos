package service

import (
	"context"
)

// Output is an interface implemented by Benthos outputs.
type Output interface {
	// TODO

	Close(ctx context.Context) error
}
