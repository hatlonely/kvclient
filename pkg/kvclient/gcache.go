package kvclient

import (
	"time"

	"github.com/bluele/gcache"
)

// NewGcacheBuilder create a new local cache builder
func NewGcacheBuilder() *GcacheBuilder {
	return &GcacheBuilder{
		Size:       4000,
		Expiration: time.Duration(15) * time.Minute,
	}
}

// GcacheBuilder Gcache builder
type GcacheBuilder struct {
	Size       int
	Expiration time.Duration
}

// Build build a new local cache
func (b *GcacheBuilder) Build() *Gcache {
	return &Gcache{
		cache: gcache.New(b.Size).LRU().Expiration(b.Expiration).Build(),
	}
}

// WithSize set size
func (b *GcacheBuilder) WithSize(size int) *GcacheBuilder {
	b.Size = size
	return b
}

// WithExpiration set expire time
func (b *GcacheBuilder) WithExpiration(expiration time.Duration) *GcacheBuilder {
	b.Expiration = expiration
	return b
}

// Gcache localcache implementation with `github.com/bluele/gcache`
type Gcache struct {
	BaseCache

	cache gcache.Cache
}

// Set set a key
func (lc *Gcache) Set(key string, val []byte) error {
	return lc.cache.Set(key, val)
}

// Get get a key
func (lc *Gcache) Get(key string) ([]byte, error) {
	val, err := lc.cache.Get(key)
	if err == gcache.KeyNotFoundError {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	return val.([]byte), nil
}

// Del delete a key
func (lc *Gcache) Del(key string) error {
	lc.cache.Remove(key)
	return nil
}

// SetEx set with expiration
func (lc *Gcache) SetEx(key string, val []byte, expiration time.Duration) error {
	return lc.cache.SetWithExpire(key, val, expiration)
}

// SetNx set if not exists
func (lc *Gcache) SetNx(key string, val []byte) (bool, error) {
	return SetNx(lc, key, val)
}

// SetExNx set if not exists with expiration
func (lc *Gcache) SetExNx(key string, val []byte, expiration time.Duration) (bool, error) {
	return SetExNx(lc, key, val, expiration)
}

// SetBatch keys vals
func (lc *Gcache) SetBatch(keys []string, vals [][]byte) ([]error, error) {
	return SetBatch(lc, keys, vals)
}

// GetBatch keys
func (lc *Gcache) GetBatch(keys []string) ([][]byte, []error, error) {
	return GetBatch(lc, keys)
}
