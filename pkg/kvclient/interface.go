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
	Marshal(val interface{}) (buf []byte, err error)
	Unmarshal(buf []byte, val interface{}) (err error)
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
}

// Cache interface
type Cache interface {
	Get(key string) (val []byte, err error) // return nil, nil if key not found
	Set(key string, val []byte) (err error) // key will expire with default configuration
	Del(key string) (err error)
	SetBatch(keys []string, vals [][]byte) (errs []error, err error)
	SetEx(key string, val []byte, expiration time.Duration) error   // set with expiration
	SetNx(key string, val []byte) error                             // set if not exists
	SetExNx(key string, val []byte, expiration time.Duration) error // set if not exists with expiration
	Close() error
}
