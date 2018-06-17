package kvclient

import (
	"fmt"
	"time"
)

// NewBuilder create a new Builder
func NewBuilder() *Builder {
	return &Builder{}
}

// Builder kvclient builder
type Builder struct {
	caches     []Cache
	compressor Compressor
	serializer Serializer
}

// WithCaches option
func (b *Builder) WithCaches(caches []Cache) *Builder {
	b.caches = caches
	return b
}

// WithCompressor option
func (b *Builder) WithCompressor(compressor Compressor) *Builder {
	b.compressor = compressor
	return b
}

// WithSerializer option
func (b *Builder) WithSerializer(serializer Serializer) *Builder {
	b.serializer = serializer
	return b
}

// Build a KVClient
func (b *Builder) Build() KVClient {
	return &kvClient{
		caches:     b.caches,
		compressor: b.compressor,
		serializer: b.serializer,
	}
}

// kvClient dmp client
type kvClient struct {
	caches     []Cache
	compressor Compressor
	serializer Serializer
}

// SetCompressor set compressor
func (c *kvClient) SetCompressor(compressor Compressor) {
	c.compressor = compressor
}

// SetCompressor set serializer
func (c *kvClient) SetSerializer(serializer Serializer) {
	c.serializer = serializer
}

// Get get a key
func (c *kvClient) Get(key interface{}, val interface{}) (bool, error) {
	keybuf := c.compressor.Compress(key)

	for i, cache := range c.caches {
		buf, err := cache.Get(keybuf)
		if err != nil {
			return false, err
		}

		if buf != nil {
			if err := c.serializer.Unmarshal(buf, val); err != nil {
				return false, err
			}

			for j := 0; j < i; j++ {
				cache.Set(keybuf, buf)
			}

			return true, nil
		}
	}

	return false, nil
}

// Set set a key
func (c *kvClient) Set(key interface{}, val interface{}) error {
	keybuf := c.compressor.Compress(key)
	valbuf, err := c.serializer.Marshal(val)

	if err != nil {
		return err
	}

	for _, cache := range c.caches {
		if seterr := cache.Set(keybuf, valbuf); seterr != nil {
			err = seterr
		}
	}

	return err
}

// Del remove a key
func (c *kvClient) Del(key interface{}) error {
	keybuf := c.compressor.Compress(key)

	var err error
	for _, cache := range c.caches {
		if delerr := cache.Del(keybuf); delerr != nil {
			err = delerr
		}
	}

	return err
}

func (c *kvClient) SetEx(key interface{}, val interface{}, expiration time.Duration) error {
	keybuf := c.compressor.Compress(key)
	valbuf, err := c.serializer.Marshal(val)

	if err != nil {
		return err
	}

	for _, cache := range c.caches {
		if seterr := cache.SetEx(keybuf, valbuf, expiration); seterr != nil {
			err = seterr
		}
	}

	return err
}

func (c *kvClient) SetNx(key interface{}, val interface{}) error {
	keybuf := c.compressor.Compress(key)
	valbuf, err := c.serializer.Marshal(val)

	if err != nil {
		return err
	}

	for _, cache := range c.caches {
		if seterr := cache.SetNx(keybuf, valbuf); seterr != nil {
			err = seterr
		}
	}

	return err
}

func (c *kvClient) SetExNx(key interface{}, val interface{}, expiration time.Duration) error {
	keybuf := c.compressor.Compress(key)
	valbuf, err := c.serializer.Marshal(val)

	if err != nil {
		return err
	}

	for _, cache := range c.caches {
		if seterr := cache.SetExNx(keybuf, valbuf, expiration); seterr != nil {
			err = seterr
		}
	}

	return err
}

// SetBatch set batch
func (c *kvClient) SetBatch(keys []interface{}, vals []interface{}) ([]error, error) {
	if len(keys) != len(vals) {
		return nil, fmt.Errorf("assert len(keys)[%v] == len(vals)[%v] failed", len(keys), len(vals))
	}

	var err error
	var errs []error
	keybufs := make([]string, len(keys))
	valbufs := make([][]byte, len(keys))
	for i := range keys {
		keybufs[i] = c.compressor.Compress(keys[i])
		valbufs[i], err = c.serializer.Marshal(vals[i])
		if err != nil {
			return nil, err
		}
	}

	for _, cache := range c.caches {
		sberrs, sberr := cache.SetBatch(keybufs, valbufs)
		if sberr != nil {
			err = sberr
			errs = sberrs
		}
	}

	return errs, err
}
