package kvclient

import (
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis"
)

// NewRedisClusterStringBuilder create a new redis cluster client builder, use redis string type
func NewRedisClusterStringBuilder() *RedisClusterStringBuilder {
	return &RedisClusterStringBuilder{
		Address:    []string{"127.0.0.1:7000"},
		Timeout:    time.Duration(1000) * time.Millisecond,
		Retries:    3,
		PoolSize:   20,
		Expiration: time.Duration(24) * time.Hour,
	}
}

// RedisClusterStringBuilder redis cluster builder
type RedisClusterStringBuilder struct {
	Address    []string
	Timeout    time.Duration
	Retries    int
	PoolSize   int
	Expiration time.Duration
}

// WithAddress set address
func (b *RedisClusterStringBuilder) WithAddress(address string) *RedisClusterStringBuilder {
	b.Address = strings.Split(address, ",")
	return b
}

// WithRetries set retry times
func (b *RedisClusterStringBuilder) WithRetries(retries int) *RedisClusterStringBuilder {
	b.Retries = retries
	return b
}

// WithTimeout set timeout
func (b *RedisClusterStringBuilder) WithTimeout(timeout time.Duration) *RedisClusterStringBuilder {
	b.Timeout = timeout
	return b
}

// WithPoolSize set connection pool size
func (b *RedisClusterStringBuilder) WithPoolSize(poolsize int) *RedisClusterStringBuilder {
	b.PoolSize = poolsize
	return b
}

// WithExpiration set expire time
func (b *RedisClusterStringBuilder) WithExpiration(expiration time.Duration) *RedisClusterStringBuilder {
	b.Expiration = expiration
	return b
}

// Build build a new redis cluster client
func (b *RedisClusterStringBuilder) Build() (*RedisClusterString, error) {
	redisCluster := &RedisClusterString{}
	redisCluster.client = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:        b.Address,
		DialTimeout:  b.Timeout,
		ReadTimeout:  b.Timeout,
		WriteTimeout: b.Timeout,
		MaxRetries:   b.Retries,
		PoolSize:     b.PoolSize,
	})
	redisCluster.expiration = b.Expiration
	if err := redisCluster.client.Ping().Err(); err != nil {
		return nil, err
	}

	return redisCluster, nil
}

// RedisClusterString redis cluster client
type RedisClusterString struct {
	client     *redis.ClusterClient
	expiration time.Duration
}

// Close redis client
func (rc *RedisClusterString) Close() error {
	return rc.client.Close()
}

// Get get a key
func (rc *RedisClusterString) Get(key string) ([]byte, error) {
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
func (rc *RedisClusterString) Set(key string, val []byte) error {
	return rc.client.Set(key, val, rc.expiration).Err()
}

// Del delete a key
func (rc *RedisClusterString) Del(key string) error {
	return rc.client.Del(key).Err()
}

// SetEx set with expiration
func (rc *RedisClusterString) SetEx(key string, val []byte, expiration time.Duration) error {
	return rc.client.Set(key, val, expiration).Err()
}

// SetNx set if not exists
func (rc *RedisClusterString) SetNx(key string, val []byte) error {
	return rc.client.SetNX(key, val, rc.expiration).Err()
}

// SetExNx set if not exists with expiration
func (rc *RedisClusterString) SetExNx(key string, val []byte, expiration time.Duration) error {
	return rc.client.SetNX(key, val, expiration).Err()
}

// SetBatch set batch
func (rc *RedisClusterString) SetBatch(keys []string, vals [][]byte) ([]error, error) {
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
