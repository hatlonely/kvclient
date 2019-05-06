package kvclient

import (
	"fmt"
	"time"

	"github.com/go-redis/redis"
)

// NewRedisStringBuilder create a new redis client builder, use redis string type
func NewRedisStringBuilder() *RedisStringBuilder {
	return &RedisStringBuilder{
		Address:    "127.0.0.1:6379",
		Timeout:    time.Duration(1000) * time.Millisecond,
		Retries:    3,
		PoolSize:   20,
		Expiration: time.Duration(24) * time.Hour,
	}
}

// RedisStringBuilder redis cluster builder
type RedisStringBuilder struct {
	Address    string
	Password   string
	Timeout    time.Duration
	Retries    int
	PoolSize   int
	Expiration time.Duration
}

// WithAddress set address
func (b *RedisStringBuilder) WithAddress(address string) *RedisStringBuilder {
	b.Address = address
	return b
}

// WithPassword set password
func (b *RedisStringBuilder) WithPassword(password string) *RedisStringBuilder {
	b.Password = password
	return b
}

// WithRetries set retry times
func (b *RedisStringBuilder) WithRetries(retries int) *RedisStringBuilder {
	b.Retries = retries
	return b
}

// WithTimeout set timeout
func (b *RedisStringBuilder) WithTimeout(timeout time.Duration) *RedisStringBuilder {
	b.Timeout = timeout
	return b
}

// WithPoolSize set connection pool size
func (b *RedisStringBuilder) WithPoolSize(poolsize int) *RedisStringBuilder {
	b.PoolSize = poolsize
	return b
}

// WithExpiration set expire time
func (b *RedisStringBuilder) WithExpiration(expiration time.Duration) *RedisStringBuilder {
	b.Expiration = expiration
	return b
}

// Build build a new redis cluster client
func (b *RedisStringBuilder) Build() (*RedisString, error) {
	rs := &RedisString{}
	rs.client = redis.NewClient(&redis.Options{
		Addr:         b.Address,
		DialTimeout:  b.Timeout,
		ReadTimeout:  b.Timeout,
		WriteTimeout: b.Timeout,
		MaxRetries:   b.Retries,
		PoolSize:     b.PoolSize,
		Password:     b.Password,
	})
	rs.expiration = b.Expiration
	if err := rs.client.Ping().Err(); err != nil {
		return nil, err
	}

	return rs, nil
}

// RedisString redis cluster client
type RedisString struct {
	BaseCache

	client     *redis.Client
	expiration time.Duration
}

// Close redis client
func (rc *RedisString) Close() error {
	return rc.client.Close()
}

// Get get a key
func (rc *RedisString) Get(key string) ([]byte, error) {
	val, err := rc.client.Get(key).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return []byte(val), nil
}

// Set set a key
func (rc *RedisString) Set(key string, val []byte) error {
	return rc.client.Set(key, val, rc.expiration).Err()
}

// Del delete a key
func (rc *RedisString) Del(key string) error {
	return rc.client.Del(key).Err()
}

// SetEx set with expiration
func (rc *RedisString) SetEx(key string, val []byte, expiration time.Duration) error {
	return rc.client.Set(key, val, expiration).Err()
}

// SetNx set if not exists
func (rc *RedisString) SetNx(key string, val []byte) (bool, error) {
	return rc.client.SetNX(key, val, rc.expiration).Result()
}

// SetExNx set if not exists with expiration
func (rc *RedisString) SetExNx(key string, val []byte, expiration time.Duration) (bool, error) {
	return rc.client.SetNX(key, val, expiration).Result()
}

// SetBatch set batch
func (rc *RedisString) SetBatch(keys []string, vals [][]byte) ([]error, error) {
	if len(keys) != len(vals) {
		return nil, fmt.Errorf("assert len(keys)[%v] == len(vals)[%v] failed", len(keys), len(vals))
	}

	pipe := rc.client.Pipeline()
	defer pipe.Close()
	cmds := make([]*redis.StatusCmd, len(keys))

	for i := range keys {
		cmds[i] = pipe.Set(keys[i], vals[i], rc.expiration)
	}

	if _, err := pipe.Exec(); err != nil {
		return nil, err
	}

	errs := make([]error, len(keys))
	for i, cmd := range cmds {
		errs[i] = cmd.Err()
	}

	return errs, nil
}

// GetBatch keys
func (rc *RedisString) GetBatch(keys []string) ([][]byte, []error, error) {
	pipe := rc.client.Pipeline()
	defer pipe.Close()
	cmds := make([]*redis.StringCmd, len(keys))

	for i := range keys {
		cmds[i] = pipe.Get(keys[i])
	}

	if _, err := pipe.Exec(); err != nil && err != redis.Nil {
		return nil, nil, err
	}

	vals := make([][]byte, len(keys))
	errs := make([]error, len(keys))
	for i, cmd := range cmds {
		err := cmd.Err()
		if err == redis.Nil {
			vals[i] = nil
			errs[i] = nil
		} else if err != nil {
			vals[i] = nil
			errs[i] = err
		} else {
			vals[i] = []byte(cmd.Val())
			errs[i] = err
		}
	}

	return vals, errs, nil
}
