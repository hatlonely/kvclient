package kvclient

import (
	"fmt"
	"time"
)

// BaseCache cache base not implememt
type BaseCache struct{}

// Close cache
func (c *BaseCache) Close() error {
	return nil
}

// Get key
func (c *BaseCache) Get(key string) ([]byte, error) {
	panic("not implememt")
}

// Set key value
func (c *BaseCache) Set(key string, val []byte) error {
	panic("not implement")
}

// Del key
func (c *BaseCache) Del(key string) error {
	panic("not implement")
}

// SetBatch keys values
func (c *BaseCache) SetBatch(keys []string, vals [][]byte) ([]error, error) {
	panic("not implememt")
}

// SetEx set key with expiration
func (c *BaseCache) SetEx(key string, val []byte, expiration time.Duration) error {
	panic("not implememt")
}

// SetNx set if not exist
func (c *BaseCache) SetNx(key string, val []byte) (bool, error) {
	panic("not implememt")
}

// SetExNx set with expiration if not exist
func (c *BaseCache) SetExNx(key string, val []byte, expiration time.Duration) (bool, error) {
	panic("not implememt")
}

// GetBatch get keys
func GetBatch(c Cache, keys []string) ([][]byte, []error, error) {
	errs := make([]error, len(keys))
	vals := make([][]byte, len(keys))
	var err error
	for i := range keys {
		vals[i], errs[i] = c.Get(keys[i])
		if errs[i] != nil {
			err = errs[i]
		}
	}

	return vals, errs, err
}

// SetBatch set keys values
func SetBatch(c Cache, keys []string, vals [][]byte) ([]error, error) {
	if len(keys) != len(vals) {
		return nil, fmt.Errorf("assert len(keys)[%v] == len(vals)[%v] failed", len(keys), len(vals))
	}

	errs := make([]error, len(keys))
	var err error
	for i := range keys {
		errs[i] = c.Set(keys[i], vals[i])
		if errs[i] != nil {
			err = errs[i]
		}
	}

	return errs, err
}

// SetNx set if not exists
func SetNx(c Cache, key string, val []byte) (bool, error) {
	gval, err := c.Get(key)
	if err != nil {
		return false, err
	}

	if gval != nil {
		return false, nil
	}

	return true, c.Set(key, val)
}

// SetExNx set if not exists with expiration
func SetExNx(c Cache, key string, val []byte, expiration time.Duration) (bool, error) {
	gval, err := c.Get(key)
	if err != nil {
		return false, err
	}

	if gval != nil {
		return false, nil
	}

	return true, c.SetEx(key, val, expiration)
}
