package kvclient

import (
	"bytes"
	"fmt"
	"sync/atomic"
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
		getTimes:   make([]int64, len(b.caches)),
		hitTimes:   make([]int64, len(b.caches)),
		compressor: b.compressor,
		serializer: b.serializer,
		nilValBuf:  []byte{},
	}
}

// kvClient dmp client
type kvClient struct {
	caches     []Cache
	getTimes   []int64
	hitTimes   []int64
	compressor Compressor
	serializer Serializer
	nilValBuf  []byte
}

// Close caches
func (c *kvClient) Close() error {
	var err error
	for _, cache := range c.caches {
		if cerr := cache.Close(); cerr != nil {
			err = cerr
		}
	}

	return err
}

// SetCompressor set compressor
func (c *kvClient) SetCompressor(compressor Compressor) {
	c.compressor = compressor
}

// SetCompressor set serializer
func (c *kvClient) SetSerializer(serializer Serializer) {
	c.serializer = serializer
}

// SetNilValBuf set nilValBuf
func (c *kvClient) SetNilValBuf(buf []byte) {
	c.nilValBuf = buf
}

// CacheHitRate cache hit rate
func (c *kvClient) CacheHitRate() []float64 {
	var rate []float64
	for i := range c.caches {
		rate = append(rate, float64(c.hitTimes[i])/float64(c.getTimes[i]))
	}

	return rate
}

// Get key
func (c *kvClient) Get(key interface{}, val interface{}) (bool, error) {
	keybuf := c.compressor.Compress(key)

	var ok bool
	var err error
	var buf []byte
	var idx int
	for i, cache := range c.caches {
		idx = i
		buf, err = cache.Get(keybuf)
		atomic.AddInt64(&(c.getTimes[i]), 1)
		if err != nil {
			return false, err
		}
		if buf != nil {
			if bytes.Equal(buf, c.nilValBuf) {
				ok = false
				break
			}

			atomic.AddInt64(&(c.hitTimes[i]), 1)
			if err = c.serializer.Unmarshal(buf, val); err != nil {
				return false, err
			}
			ok = true
			break
		}
	}

	if ok {
		for i := 0; i < idx; i++ {
			c.caches[i].Set(keybuf, buf)
		}
	} else {
		for i := 0; i < idx; i++ {
			c.caches[i].Set(keybuf, c.nilValBuf)
		}
	}

	return ok, nil
}

// Set key
func (c *kvClient) Set(key interface{}, val interface{}) error {
	keybuf := c.compressor.Compress(key)
	valbuf, err := c.serializer.Marshal(val)

	if err != nil {
		return err
	}

	for _, cache := range c.caches {
		if err := cache.Set(keybuf, valbuf); err != nil {
			return err
		}
	}

	return nil
}

// Del key
func (c *kvClient) Del(key interface{}) error {
	keybuf := c.compressor.Compress(key)

	for _, cache := range c.caches {
		if err := cache.Del(keybuf); err != nil {
			return err
		}
	}

	return nil
}

// SetEx set with expiration
func (c *kvClient) SetEx(key interface{}, val interface{}, expiration time.Duration) error {
	keybuf := c.compressor.Compress(key)
	valbuf, err := c.serializer.Marshal(val)

	if err != nil {
		return err
	}

	for _, cache := range c.caches {
		if err := cache.SetEx(keybuf, valbuf, expiration); err != nil {
			return err
		}
	}

	return nil
}

// SetNx set if not exist
func (c *kvClient) SetNx(key interface{}, val interface{}) (bool, error) {
	keybuf := c.compressor.Compress(key)
	valbuf, err := c.serializer.Marshal(val)

	if err != nil {
		return false, err
	}

	for i := 0; i < len(c.caches)-1; i++ {
		if _, err := c.caches[i].SetNx(keybuf, valbuf); err != nil {
			return false, err
		}
	}

	return c.caches[len(c.caches)-1].SetNx(keybuf, valbuf)
}

// SetExNx set with expiration if not exist
func (c *kvClient) SetExNx(key interface{}, val interface{}, expiration time.Duration) (bool, error) {
	keybuf := c.compressor.Compress(key)
	valbuf, err := c.serializer.Marshal(val)

	if err != nil {
		return false, err
	}

	for i := 0; i < len(c.caches)-1; i++ {
		if _, err := c.caches[i].SetExNx(keybuf, valbuf, expiration); err != nil {
			return false, err
		}
	}

	return c.caches[len(c.caches)-1].SetExNx(keybuf, valbuf, expiration)
}

// SetBatch set batch
func (c *kvClient) SetBatch(keys []interface{}, vals []interface{}) ([]error, error) {
	if len(keys) != len(vals) {
		return nil, fmt.Errorf("assert len(keys)[%v] == len(vals)[%v] failed", len(keys), len(vals))
	}

	var err error
	keybufs := make([]string, len(keys))
	valbufs := make([][]byte, len(keys))
	for i := range keys {
		keybufs[i] = c.compressor.Compress(keys[i])
		valbufs[i], err = c.serializer.Marshal(vals[i])
		if err != nil {
			return nil, err
		}
	}

	for i := 0; i < len(c.caches)-1; i++ {
		if errs, err := c.caches[i].SetBatch(keybufs, valbufs); err != nil {
			return errs, err
		}
	}

	return c.caches[len(c.caches)-1].SetBatch(keybufs, valbufs)
}

// GetBatch get batch
func (c *kvClient) GetBatch(keys []interface{}, vals []interface{}) ([]bool, []error, error) {
	if len(keys) != len(vals) {
		return nil, nil, fmt.Errorf("assert len(keys)[%v] == len(vals)[%v] failed", len(keys), len(vals))
	}

	var err error
	keybufs := make([]string, len(keys))
	for i := range keys {
		keybufs[i] = c.compressor.Compress(keys[i])
	}

	valbufs, errs, err := c.caches[len(c.caches)-1].GetBatch(keybufs)
	if err != nil {
		return nil, nil, err
	}

	oks := make([]bool, len(keys))
	for i := range keys {
		oks[i] = false
		if errs[i] != nil {
			continue
		}
		if valbufs[i] == nil {
			continue
		}
		if err := c.serializer.Unmarshal(valbufs[i], vals[i]); err != nil {
			errs[i] = err
		} else {
			oks[i] = true
		}
	}

	return oks, errs, nil
}
