package service

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMessageCopyAirGap(t *testing.T) {
	p := message.NewPart([]byte("hello world"))
	p.Metadata().Set("foo", "bar")
	g1 := newAirGapMessage(p)
	g2 := CopyMessage(g1)

	b := p.Get()
	v := p.Metadata().Get("foo")
	assert.Equal(t, "hello world", string(b))
	assert.Equal(t, "bar", v)

	b, ok := g1.Bytes()
	v, _ = g1.Metadata().Get("foo")
	require.True(t, ok)
	assert.Equal(t, "hello world", string(b))
	assert.Equal(t, "bar", v)

	b, ok = g2.Bytes()
	v, _ = g2.Metadata().Get("foo")
	require.True(t, ok)
	assert.Equal(t, "hello world", string(b))
	assert.Equal(t, "bar", v)

	g2.SetBytes([]byte("and now this"))
	g2.Metadata().Set("foo", "baz")

	b = p.Get()
	v = p.Metadata().Get("foo")
	assert.Equal(t, "hello world", string(b))
	assert.Equal(t, "bar", v)

	b, ok = g1.Bytes()
	v, _ = g1.Metadata().Get("foo")
	require.True(t, ok)
	assert.Equal(t, "hello world", string(b))
	assert.Equal(t, "bar", v)

	b, ok = g2.Bytes()
	v, _ = g2.Metadata().Get("foo")
	require.True(t, ok)
	assert.Equal(t, "and now this", string(b))
	assert.Equal(t, "baz", v)

	g1.SetBytes([]byte("but not this"))
	g1.Metadata().Set("foo", "buz")

	b = p.Get()
	v = p.Metadata().Get("foo")
	assert.Equal(t, "hello world", string(b))
	assert.Equal(t, "bar", v)

	b, ok = g1.Bytes()
	v, _ = g1.Metadata().Get("foo")
	require.True(t, ok)
	assert.Equal(t, "but not this", string(b))
	assert.Equal(t, "buz", v)

	b, ok = g2.Bytes()
	v, _ = g2.Metadata().Get("foo")
	require.True(t, ok)
	assert.Equal(t, "and now this", string(b))
	assert.Equal(t, "baz", v)
}

func TestMessageCopyAirGapUnknownType(t *testing.T) {
	type notAirGapMsg struct {
		*airGapMessage
	}

	p := message.NewPart([]byte("hello world"))
	p.Metadata().Set("foo", "bar")

	g1 := notAirGapMsg{newAirGapMessage(p).(*airGapMessage)}
	g2 := CopyMessage(g1)

	b := p.Get()
	v := p.Metadata().Get("foo")
	assert.Equal(t, "hello world", string(b))
	assert.Equal(t, "bar", v)

	b, ok := g1.Bytes()
	v, _ = g1.Metadata().Get("foo")
	require.True(t, ok)
	assert.Equal(t, "hello world", string(b))
	assert.Equal(t, "bar", v)

	b, ok = g2.Bytes()
	v, _ = g2.Metadata().Get("foo")
	require.True(t, ok)
	assert.Equal(t, "hello world", string(b))
	assert.Equal(t, "bar", v)

	g2.SetBytes([]byte("and now this"))
	g2.Metadata().Set("foo", "baz")

	b = p.Get()
	v = p.Metadata().Get("foo")
	assert.Equal(t, "hello world", string(b))
	assert.Equal(t, "bar", v)

	b, ok = g1.Bytes()
	v, _ = g1.Metadata().Get("foo")
	require.True(t, ok)
	assert.Equal(t, "hello world", string(b))
	assert.Equal(t, "bar", v)

	b, ok = g2.Bytes()
	v, _ = g2.Metadata().Get("foo")
	require.True(t, ok)
	assert.Equal(t, "and now this", string(b))
	assert.Equal(t, "baz", v)

	g1.SetBytes([]byte("but not this"))
	g1.Metadata().Set("foo", "buz")

	b = p.Get()
	v = p.Metadata().Get("foo")
	assert.Equal(t, "hello world", string(b))
	assert.Equal(t, "bar", v)

	b, ok = g1.Bytes()
	v, _ = g1.Metadata().Get("foo")
	require.True(t, ok)
	assert.Equal(t, "but not this", string(b))
	assert.Equal(t, "buz", v)

	b, ok = g2.Bytes()
	v, _ = g2.Metadata().Get("foo")
	require.True(t, ok)
	assert.Equal(t, "and now this", string(b))
	assert.Equal(t, "baz", v)
}
