package bloblang

import (
	"fmt"

	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
)

// ArgSpec provides an API for validating and extracting function or method
// arguments by registering them with pointer receivers.
type ArgSpec struct {
	n          int
	validators []func(args []interface{}) error
}

// NewArgSpec creates an argument parser/validator.
func NewArgSpec() *ArgSpec {
	return &ArgSpec{}
}

// Extract the specified typed arguments from a slice of generic arguments.
// Returns an error if the type of an argument is mismatched, or if the number
// of arguments is mismatched.
func (a *ArgSpec) Extract(args []interface{}) error {
	if len(args) != a.n {
		return fmt.Errorf("expected %v arguments, received %v", a.n, len(args))
	}
	for _, v := range a.validators {
		if err := v(args); err != nil {
			return err
		}
	}
	return nil
}

// IntVar creates an int argument to follow the previously created argument.
func (a *ArgSpec) IntVar(i *int) *ArgSpec {
	index := a.n
	a.n++

	a.validators = append(a.validators, func(args []interface{}) error {
		v, err := query.IGetInt(args[index])
		if err != nil {
			return fmt.Errorf("bad argument %v: %w", index, err)
		}
		*i = int(v)
		return nil
	})

	return a
}

// Int64Var creates an int64 argument to follow the previously created argument.
func (a *ArgSpec) Int64Var(i *int64) *ArgSpec {
	index := a.n
	a.n++

	a.validators = append(a.validators, func(args []interface{}) error {
		v, err := query.IGetInt(args[index])
		if err != nil {
			return fmt.Errorf("bad argument %v: %w", index, err)
		}
		*i = v
		return nil
	})

	return a
}

// Float64Var creates a Float64 argument to follow the previously created
// argument.
func (a *ArgSpec) Float64Var(f *float64) *ArgSpec {
	index := a.n
	a.n++

	a.validators = append(a.validators, func(args []interface{}) error {
		v, err := query.IGetNumber(args[index])
		if err != nil {
			return fmt.Errorf("bad argument %v: %w", index, err)
		}
		*f = v
		return nil
	})

	return a
}

// BoolVar creates a boolean argument to follow the previously created argument.
func (a *ArgSpec) BoolVar(b *bool) *ArgSpec {
	index := a.n
	a.n++

	a.validators = append(a.validators, func(args []interface{}) error {
		v, err := query.IGetBool(args[index])
		if err != nil {
			return fmt.Errorf("bad argument %v: %w", index, err)
		}
		*b = v
		return nil
	})

	return a
}

// StringVar creates a string argument to follow the previously created
// argument.
func (a *ArgSpec) StringVar(s *string) *ArgSpec {
	index := a.n
	a.n++

	a.validators = append(a.validators, func(args []interface{}) error {
		v, err := query.IGetString(args[index])
		if err != nil {
			return fmt.Errorf("bad argument %v: %w", index, err)
		}
		*s = v
		return nil
	})

	return a
}

// AnyVar creates an argument to follow the previously created argument that can
// have any value.
func (a *ArgSpec) AnyVar(i *interface{}) *ArgSpec {
	index := a.n
	a.n++

	a.validators = append(a.validators, func(args []interface{}) error {
		*i = args[index]
		return nil
	})

	return a
}
