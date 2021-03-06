package kvclient

import (
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis"
)

// NewRedisClusterHashBuilder create a new redis cluster client builder, use redis string type
func NewRedisClusterHashBuilder() *RedisClusterHashBuilder {
	return &RedisClusterHashBuilder{
		Address:  []string{"127.0.0.1:7000"},
		Timeout:  time.Duration(1000) * time.Millisecond,
		Retries:  3,
		PoolSize: 20,
		KeyIdx:   8,
		KeyLen:   7,
	}
}

// RedisClusterHashBuilder redis cluster builder
type RedisClusterHashBuilder struct {
	Address  []string
	Timeout  time.Duration
	Retries  int
	PoolSize int
	KeyIdx   int
	KeyLen   int
}

// WithAddress option
func (b *RedisClusterHashBuilder) WithAddress(address string) *RedisClusterHashBuilder {
	b.Address = strings.Split(address, ",")
	return b
}

// WithRetries option
func (b *RedisClusterHashBuilder) WithRetries(retries int) *RedisClusterHashBuilder {
	b.Retries = retries
	return b
}

// WithTimeout option
func (b *RedisClusterHashBuilder) WithTimeout(timeout time.Duration) *RedisClusterHashBuilder {
	b.Timeout = timeout
	return b
}

// WithPoolSize option
func (b *RedisClusterHashBuilder) WithPoolSize(poolsize int) *RedisClusterHashBuilder {
	b.PoolSize = poolsize
	return b
}

// WithKeyIdxLen option
func (b *RedisClusterHashBuilder) WithKeyIdxLen(keyIdx int, keyLen int) *RedisClusterHashBuilder {
	b.KeyIdx = keyIdx
	b.KeyLen = keyLen
	return b
}

// Build build a new redis cluster client
func (b *RedisClusterHashBuilder) Build() (*RedisClusterHash, error) {
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:        b.Address,
		DialTimeout:  b.Timeout,
		ReadTimeout:  b.Timeout,
		WriteTimeout: b.Timeout,
		MaxRetries:   b.Retries,
		PoolSize:     b.PoolSize,
	})
	if err := client.Ping().Err(); err != nil {
		return nil, err
	}

	return &RedisClusterHash{
		client: client,
		keyIdx: b.KeyIdx,
		keyLen: b.KeyLen,
	}, nil
}

// RedisClusterHash redis cluster client
type RedisClusterHash struct {
	BaseCache

	client *redis.ClusterClient
	keyIdx int
	keyLen int
}

// Close redis client
func (rc *RedisClusterHash) Close() error {
	return rc.client.Close()
}

// Get get a key
func (rc *RedisClusterHash) Get(key string) ([]byte, error) {
	k, f := rc.parseKey(key)
	val, err := rc.client.HGet(k, f).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return []byte(val), nil
}

// Set set a key
func (rc *RedisClusterHash) Set(key string, val []byte) error {
	k, f := rc.parseKey(key)
	return rc.client.HSet(k, f, val).Err()
}

// Del delete a key
func (rc *RedisClusterHash) Del(key string) error {
	k, f := rc.parseKey(key)
	return rc.client.HDel(k, f).Err()
}

// SetNx set if not exists
func (rc *RedisClusterHash) SetNx(key string, val []byte) (bool, error) {
	k, f := rc.parseKey(key)
	return rc.client.HSetNX(k, f, val).Result()
}

// SetBatch set batch
func (rc *RedisClusterHash) SetBatch(keys []string, vals [][]byte) ([]error, error) {
	if len(keys) != len(vals) {
		return nil, fmt.Errorf("assert len(keys)[%v] == len(vals)[%v] failed", len(keys), len(vals))
	}

	pipe := rc.client.Pipeline()
	defer pipe.Close()
	cmds := make([]*redis.BoolCmd, len(keys))

	for i := range keys {
		k, f := rc.parseKey(keys[i])
		cmds[i] = pipe.HSet(k, f, vals[i])
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

func (rc *RedisClusterHash) parseKey(key string) (string, string) {
	if len(key) > rc.keyIdx+rc.keyLen {
		return key[rc.keyIdx : rc.keyIdx+rc.keyLen], key[:rc.keyIdx] + key[rc.keyIdx+rc.keyLen:]
	} else if len(key) > rc.keyIdx {
		return key[rc.keyIdx:], key[:rc.keyIdx]
	} else {
		return "", key
	}
}

// GetBatch keys
func (rc *RedisClusterHash) GetBatch(keys []string) ([][]byte, []error, error) {
	pipe := rc.client.Pipeline()
	defer pipe.Close()
	cmds := make([]*redis.StringCmd, len(keys))

	for i := range keys {
		k, f := rc.parseKey(keys[i])
		cmds[i] = pipe.HGet(k, f)
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
