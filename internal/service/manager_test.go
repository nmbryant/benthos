package service

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//------------------------------------------------------------------------------

func TestManagerPipeErrors(t *testing.T) {
	mgr, err := NewManager(nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	_, err = mgr.GetPipe("does not exist")
	assert.Equal(t, types.ErrPipeNotFound, err)
}

func TestManagerPipeGetSet(t *testing.T) {
	mgr, err := NewManager(nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	var t1 <-chan types.Transaction = make(chan types.Transaction)
	var t2 <-chan types.Transaction = make(chan types.Transaction)
	var t3 <-chan types.Transaction = make(chan types.Transaction)

	mgr.SetPipe("foo", t1)
	mgr.SetPipe("bar", t3)

	p, err := mgr.GetPipe("foo")
	require.NoError(t, err)
	assert.Equal(t, p, t1)

	// Should be a noop
	mgr.UnsetPipe("foo", t2)
	p, err = mgr.GetPipe("foo")
	require.NoError(t, err)
	assert.Equal(t, p, t1)

	p, err = mgr.GetPipe("bar")
	require.NoError(t, err)
	assert.Equal(t, p, t3)

	mgr.UnsetPipe("foo", t1)
	_, err = mgr.GetPipe("foo")
	assert.Equal(t, types.ErrPipeNotFound, err)

	// Back to before
	mgr.SetPipe("foo", t1)
	p, err = mgr.GetPipe("foo")
	require.NoError(t, err)
	assert.Equal(t, p, t1)

	// Now replace pipe
	mgr.SetPipe("foo", t2)
	p, err = mgr.GetPipe("foo")
	require.NoError(t, err)
	assert.Equal(t, p, t2)

	p, err = mgr.GetPipe("bar")
	require.NoError(t, err)
	assert.Equal(t, p, t3)
}

//------------------------------------------------------------------------------
