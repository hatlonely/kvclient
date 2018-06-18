package kvclient

import (
	"errors"
	"time"
)

// ErrNotFound error
var ErrNotFound = errors.New("key not found")

// Compressor compress key
type Compressor interface {
	Compress(key interface{}) string
}

// Serializer serialize val
type Serializer interface {
	Marshal(val interface{}) ([]byte, error)
	Unmarshal(buf []byte, val interface{}) error
}

// KVClient client for kv storage
type KVClient interface {
	SetCompressor(compressor Compressor)
	SetSerializer(serializer Serializer)
	Get(key interface{}, val interface{}) (bool, error) // return false if key not found
	Set(key interface{}, val interface{}) error         // key will expire with default configuration
	Del(key interface{}) error
	SetBatch(keys []interface{}, vals []interface{}) ([]error, error)
	SetEx(key interface{}, val interface{}, expiration time.Duration) error   // set with expiration
	SetNx(key interface{}, val interface{}) error                             // set if not exists
	SetExNx(key interface{}, val interface{}, expiration time.Duration) error // set if not exists with expiration
	Close() error
	CacheHitRate() []float64
}

// Cache interface
type Cache interface {
	Get(key string) ([]byte, error)   // return nil, ErrNotFound if key not found
	Set(key string, val []byte) error // key will expire with default configuration
	Del(key string) error
	SetBatch(keys []string, vals [][]byte) ([]error, error)
	SetEx(key string, val []byte, expiration time.Duration) error   // set with expiration
	SetNx(key string, val []byte) error                             // set if not exists
	SetExNx(key string, val []byte, expiration time.Duration) error // set if not exists with expiration
	Close() error
}
