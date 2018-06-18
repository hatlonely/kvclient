package kvclient

import (
	"fmt"
	"time"

	"github.com/coocood/freecache"
)

// NewFreecacheBuilder create a new FreecacheBuilder
func NewFreecacheBuilder() *FreecacheBuilder {
	return &FreecacheBuilder{
		MemBytes:   512 * 1024 * 1024,
		Expiration: time.Duration(20) * time.Minute,
	}
}

// FreecacheBuilder builder
type FreecacheBuilder struct {
	MemBytes   int
	Expiration time.Duration
}

// WithMemBytes option
func (b *FreecacheBuilder) WithMemBytes(memBytes int) *FreecacheBuilder {
	b.MemBytes = memBytes
	return b
}

// WithExpiration option
func (b *FreecacheBuilder) WithExpiration(expiration time.Duration) *FreecacheBuilder {
	b.Expiration = expiration
	return b
}

// Build a new Freecache
func (b *FreecacheBuilder) Build() *Freecache {
	cache := freecache.NewCache(b.MemBytes)
	return &Freecache{
		cache:      cache,
		expiration: b.Expiration,
	}
}

// Freecache cache
type Freecache struct {
	cache      *freecache.Cache
	expiration time.Duration
}

// Close cache. nothing to do
func (c *Freecache) Close() error {
	return nil
}

// Get key
func (c *Freecache) Get(key string) ([]byte, error) {
	val, err := c.cache.Get([]byte(key))
	if err == freecache.ErrNotFound {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	return val, err
}

// Set key value
func (c *Freecache) Set(key string, val []byte) error {
	return c.cache.Set([]byte(key), val, int(c.expiration/time.Second))
}

// Del key
func (c *Freecache) Del(key string) error {
	c.cache.Del([]byte(key))
	return nil
}

// SetBatch set keys values
func (c *Freecache) SetBatch(keys []string, vals [][]byte) ([]error, error) {
	if len(keys) != len(vals) {
		return nil, fmt.Errorf("assert len(keys)[%v] == len(vals)[%v] failed", len(keys), len(vals))
	}

	errs := make([]error, len(keys))
	for i := range keys {
		errs[i] = c.Set(keys[i], vals[i])
	}

	return errs, nil
}

// SetEx set with expiration
func (c *Freecache) SetEx(key string, val []byte, expiration time.Duration) error {
	return c.cache.Set([]byte(key), val, int(expiration/time.Second))
}

// SetNx set if not exist
func (c *Freecache) SetNx(key string, val []byte) error {
	val, err := c.Get(key)
	if err != nil {
		return err
	}

	if val != nil {
		return nil
	}

	return c.Set(key, val)
}

// SetExNx set with expiration if not exists
func (c *Freecache) SetExNx(key string, val []byte, expiration time.Duration) error {
	val, err := c.Get(key)
	if err != nil {
		return err
	}

	if val != nil {
		return nil
	}

	return c.SetEx(key, val, expiration)
}
