package kvclient

import (
	"fmt"
	"time"

	"github.com/allegro/bigcache"
)

// NewBigcacheBuilder create a new BigcacheBuilder
func NewBigcacheBuilder() *BigcacheBuilder {
	return &BigcacheBuilder{
		Shards:     1024,
		Expiration: time.Duration(20) * time.Minute,
		MemBytes:   1000 * 10 * 60,
		Size:       500,
	}
}

// BigcacheBuilder builder
type BigcacheBuilder struct {
	Shards     int
	Expiration time.Duration
	MemBytes   int
	Size       int
}

// WithShards option
func (b *BigcacheBuilder) WithShards(shards int) *BigcacheBuilder {
	b.Shards = shards
	return b
}

// WithExpiration option
func (b *BigcacheBuilder) WithExpiration(expiration time.Duration) *BigcacheBuilder {
	b.Expiration = expiration
	return b
}

// WithMemBytes option
func (b *BigcacheBuilder) WithMemBytes(memBytes int) *BigcacheBuilder {
	b.MemBytes = memBytes
	return b
}

// WithSize option
func (b *BigcacheBuilder) WithSize(size int) *BigcacheBuilder {
	b.Size = size
	return b
}

// Build a new Bigcache
func (b *BigcacheBuilder) Build() (*Bigcache, error) {
	option := bigcache.DefaultConfig(b.Expiration)
	option.Shards = b.Shards
	option.MaxEntriesInWindow = b.MemBytes
	option.MaxEntrySize = b.Size
	option.HardMaxCacheSize = b.Size
	cache, err := bigcache.NewBigCache(option)

	if err != nil {
		return nil, err
	}

	return &Bigcache{
		cache: cache,
	}, nil
}

// Bigcache cache
type Bigcache struct {
	cache *bigcache.BigCache
}

// Close cache. nothing to do
func (c *Bigcache) Close() error {
	return nil
}

// Get key
func (c *Bigcache) Get(key string) ([]byte, error) {
	val, err := c.cache.Get(key)
	if err != nil {
		switch err.(type) {
		case *bigcache.EntryNotFoundError:
			return nil, nil
		}
		return nil, err
	}

	return val, nil
}

// Set key value
func (c *Bigcache) Set(key string, val []byte) error {
	return c.cache.Set(key, val)
}

// Del key
func (c *Bigcache) Del(key string) error {
	err := c.cache.Delete(key)
	if err != nil {
		switch err.(type) {
		case *bigcache.EntryNotFoundError:
			return nil
		}
		return err
	}
	return nil
}

// SetBatch set keys values
func (c *Bigcache) SetBatch(keys []string, vals [][]byte) ([]error, error) {
	if len(keys) != len(vals) {
		return nil, fmt.Errorf("assert len(keys)[%v] == len(vals)[%v] failed", len(keys), len(vals))
	}

	errs := make([]error, len(keys))
	for i := range keys {
		errs[i] = c.Set(keys[i], vals[i])
	}

	return errs, nil
}

// SetEx set with expiration. bigcache not support expiration. it use initial expiration for all keys
func (c *Bigcache) SetEx(key string, val []byte, expiration time.Duration) error {
	panic("Unsupport operation SetEx")
}

// SetNx set if not exist
func (c *Bigcache) SetNx(key string, val []byte) error {
	val, err := c.Get(key)
	if err != nil {
		return err
	}

	if val != nil {
		return nil
	}

	return c.Set(key, val)
}

// SetExNx set with expiration if not exists. bigcache not support expiration. it use initial expiration for all keys
func (c *Bigcache) SetExNx(key string, val []byte, expiration time.Duration) error {
	panic("Unsupport operation SetExNx")
}
