package kvclient

import (
	"strings"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
)

// NewMemcacheBuilder create a new MemcacheBuilder
func NewMemcacheBuilder() *MemcacheBuilder {
	return &MemcacheBuilder{
		Address: "127.0.0.1:11211",
	}
}

// MemcacheBuilder builder
type MemcacheBuilder struct {
	Address    string
	Expiration time.Duration
	PoolSize   int
	Timeout    time.Duration
}

// WithAddress option
func (b *MemcacheBuilder) WithAddress(address string) *MemcacheBuilder {
	b.Address = address
	return b
}

// WithExpiration option
func (b *MemcacheBuilder) WithExpiration(expiration time.Duration) *MemcacheBuilder {
	b.Expiration = expiration
	return b
}

// WithPoolSize option
func (b *MemcacheBuilder) WithPoolSize(poolSize int) *MemcacheBuilder {
	b.PoolSize = poolSize
	return b
}

// WithTimeout option
func (b *MemcacheBuilder) WithTimeout(timeout time.Duration) *MemcacheBuilder {
	b.Timeout = timeout
	return b
}

// Build a MemcacheBuilder
func (b *MemcacheBuilder) Build() *Memcache {
	client := memcache.New(strings.Split(b.Address, ",")...)
	client.MaxIdleConns = b.PoolSize
	client.Timeout = b.Timeout

	return &Memcache{
		client:     client,
		expiration: b.Expiration,
	}
}

// Memcache cache
type Memcache struct {
	BaseCache

	client     *memcache.Client
	expiration time.Duration
}

// Get key
func (m *Memcache) Get(key string) (val []byte, err error) {
	item, err := m.client.Get(key)
	if err == memcache.ErrCacheMiss {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return item.Value, err
}

// Set key value
func (m *Memcache) Set(key string, val []byte) error {
	return m.client.Set(&memcache.Item{Key: key, Value: val, Expiration: int32(m.expiration / time.Second)})
}

// Del key
func (m *Memcache) Del(key string) error {
	return m.client.Delete(key)
}

// SetBatch set keys values
func (m *Memcache) SetBatch(keys []string, vals [][]byte) ([]error, error) {
	return SetBatch(m, keys, vals)
}

// SetEx set with expiration
func (m *Memcache) SetEx(key string, val []byte, expiration time.Duration) error {
	return m.client.Set(&memcache.Item{Key: key, Value: val, Expiration: int32(expiration / time.Second)})
}

// SetNx set if not exist
func (m *Memcache) SetNx(key string, val []byte) error {
	return SetNx(m, key, val)
}

// SetExNx set if not exists with expiration
func (m *Memcache) SetExNx(key string, val []byte, expiration time.Duration) error {
	return SetExNx(m, key, val, expiration)
}
