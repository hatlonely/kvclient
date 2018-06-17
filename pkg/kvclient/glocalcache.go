package kvclient

import (
	"fmt"
	"time"

	"github.com/bluele/gcache"
)

// NewGLocalCacheBuilder create a new local cache builder
func NewGLocalCacheBuilder() *GLocalCacheBuilder {
	return &GLocalCacheBuilder{
		Size:       4000,
		Expiration: time.Duration(15) * time.Minute,
	}
}

// GLocalCacheBuilder glocalcache builder
type GLocalCacheBuilder struct {
	Size       int
	Expiration time.Duration
}

// Build build a new local cache
func (b *GLocalCacheBuilder) Build() *GLocalCache {
	return &GLocalCache{
		cache: gcache.New(b.Size).LRU().Expiration(b.Expiration).Build(),
	}
}

// WithSize set size
func (b *GLocalCacheBuilder) WithSize(size int) *GLocalCacheBuilder {
	b.Size = size
	return b
}

// WithExpiration set expire time
func (b *GLocalCacheBuilder) WithExpiration(expiration time.Duration) *GLocalCacheBuilder {
	b.Expiration = expiration
	return b
}

// GLocalCache localcache implementation with `github.com/bluele/gcache`
type GLocalCache struct {
	cache gcache.Cache
}

// Close cache. nothing to do
func (lc *GLocalCache) Close() error {
	return nil
}

// Set set a key
func (lc *GLocalCache) Set(key string, val []byte) error {
	return lc.cache.Set(key, val)
}

// Get get a key
func (lc *GLocalCache) Get(key string) ([]byte, error) {
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
func (lc *GLocalCache) Del(key string) error {
	lc.cache.Remove(key)
	return nil
}

// SetEx set with expiration
func (lc *GLocalCache) SetEx(key string, val []byte, expiration time.Duration) error {
	return lc.cache.SetWithExpire(key, val, expiration)
}

// SetNx set if not exists
func (lc *GLocalCache) SetNx(key string, val []byte) error {
	val, err := lc.Get(key)
	if err != nil {
		return err
	}

	if val != nil {
		return nil
	}

	return lc.Set(key, val)
}

// SetExNx set if not exists with expiration
func (lc *GLocalCache) SetExNx(key string, val []byte, expiration time.Duration) error {
	val, err := lc.Get(key)
	if err != nil {
		return err
	}

	if val != nil {
		return nil
	}

	return lc.SetEx(key, val, expiration)
}

// SetBatch keys vals
func (lc *GLocalCache) SetBatch(keys []string, vals [][]byte) ([]error, error) {
	if len(keys) != len(vals) {
		return nil, fmt.Errorf("assert len(keys)[%v] == len(vals)[%v] failed", len(keys), len(vals))
	}

	errs := make([]error, len(keys))
	for i := range keys {
		errs[i] = lc.Set(keys[i], vals[i])
	}

	return errs, nil
}
