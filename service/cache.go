package service

import (
	"context"
	"time"

	"github.com/Jeffail/benthos/v3/lib/types"
)

// Cache is an interface implemented by Benthos caches.
type Cache interface {
	// Get a cache item.
	Get(key string) ([]byte, error)

	// Set a cache item, specifying an optional TTL. It is okay for caches to
	// ignore the ttl parameter if it isn't possible to implement.
	Set(key string, value []byte, ttl *time.Duration) error

	// Add is the same operation as Set except that it returns an error if the
	// key already exists. It is okay for caches to return nil on duplicates if
	// it isn't possible to implement.
	Add(key string, value []byte, ttl *time.Duration) error

	// Delete attempts to remove a key. If the key does not exist then it is
	// considered correct to return an error, however, for cache implementations
	// where it is difficult to determine this then it is acceptable to return
	// nil.
	Delete(key string) error

	Close(ctx context.Context) error
}

// CacheConstructor is a func that's provided a configuration type and access to
// a service manager and must return an instantiation of a cache based on the
// config, or an error.
type CacheConstructor func(label string, conf interface{}, mgr Manager) (Cache, error)

//------------------------------------------------------------------------------

// Implements types.Cache
type airGapCache struct {
	c Cache

	ctx  context.Context
	done func()
}

func newAirGapCache(c Cache) types.Cache {
	ctx, done := context.WithCancel(context.Background())
	return &airGapCache{c, ctx, done}
}

func (a *airGapCache) Get(key string) ([]byte, error) {
	return a.c.Get(key)
}

func (a *airGapCache) Set(key string, value []byte) error {
	return a.c.Set(key, value, nil)
}

func (a *airGapCache) SetWithTTL(key string, value []byte, ttl *time.Duration) error {
	return a.c.Set(key, value, ttl)
}

func (a *airGapCache) SetMulti(items map[string][]byte) error {
	for k, v := range items {
		if err := a.c.Set(k, v, nil); err != nil {
			return err
		}
	}
	return nil
}

func (a *airGapCache) SetMultiWithTTL(items map[string]types.CacheTTLItem) error {
	for k, v := range items {
		if err := a.c.Set(k, v.Value, v.TTL); err != nil {
			return err
		}
	}
	return nil
}

func (a *airGapCache) Add(key string, value []byte) error {
	return a.c.Add(key, value, nil)
}

func (a *airGapCache) AddWithTTL(key string, value []byte, ttl *time.Duration) error {
	return a.c.Add(key, value, ttl)
}

func (a *airGapCache) Delete(key string) error {
	return a.c.Delete(key)
}

func (a *airGapCache) CloseAsync() {
	go func() {
		if err := a.c.Close(context.Background()); err == nil {
			a.done()
		}
	}()
}

func (a *airGapCache) WaitForClose(tout time.Duration) error {
	select {
	case <-a.ctx.Done():
	case <-time.After(tout):
		return types.ErrTimeout
	}
	return nil
}
