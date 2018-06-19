package kvclient

import (
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
	BaseCache

	cache      *freecache.Cache
	expiration time.Duration
}

// Get key
func (c *Freecache) Get(key string) ([]byte, error) {
	val, err := c.cache.Get([]byte(key))
	if err == freecache.ErrNotFound {
		return nil, nil
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
	return SetBatch(c, keys, vals)
}

// SetEx set with expiration
func (c *Freecache) SetEx(key string, val []byte, expiration time.Duration) error {
	return c.cache.Set([]byte(key), val, int(expiration/time.Second))
}

// SetNx set if not exist
func (c *Freecache) SetNx(key string, val []byte) error {
	return SetNx(c, key, val)
}

// SetExNx set with expiration if not exists
func (c *Freecache) SetExNx(key string, val []byte, expiration time.Duration) error {
	return SetExNx(c, key, val, expiration)
}
