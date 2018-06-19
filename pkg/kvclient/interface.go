package kvclient

import (
	"time"
)

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
	// nilValBuf used when we need set a key which has a nil value for local caches
	// if a key set NilValBuf as val, the key will take as not found
	SetNilValBuf(buf []byte)
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
	Get(key string) ([]byte, error) // return nil, nil if key not found
	// do not use nil as key or val. because nil val is not support for remote caches
	// if necessary, consider `[]` instead
	// key will expire with default configuration
	Set(key string, val []byte) error
	Del(key string) error
	SetBatch(keys []string, vals [][]byte) ([]error, error)
	SetEx(key string, val []byte, expiration time.Duration) error   // set with expiration
	SetNx(key string, val []byte) error                             // set if not exists
	SetExNx(key string, val []byte, expiration time.Duration) error // set if not exists with expiration
	Close() error
}
