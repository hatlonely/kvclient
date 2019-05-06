package kvclient

import (
	"fmt"
	"time"

	"github.com/go-redis/redis"
)

// NewRedisHashBuilder create a new redis cluster client builder, use redis string type
func NewRedisHashBuilder() *RedisHashBuilder {
	return &RedisHashBuilder{
		Address:  "127.0.0.1:6379",
		Timeout:  time.Duration(1000) * time.Millisecond,
		Retries:  3,
		PoolSize: 20,
		KeyIdx:   8,
		KeyLen:   7,
	}
}

// RedisHashBuilder redis cluster builder
type RedisHashBuilder struct {
	Address  string
	Password string
	Timeout  time.Duration
	Retries  int
	PoolSize int
	KeyIdx   int
	KeyLen   int
}

// WithAddress option
func (b *RedisHashBuilder) WithAddress(address string) *RedisHashBuilder {
	b.Address = address
	return b
}

// WithPassword set password
func (b *RedisHashBuilder) WithPassword(password string) *RedisHashBuilder {
	b.Password = password
	return b
}

// WithRetries option
func (b *RedisHashBuilder) WithRetries(retries int) *RedisHashBuilder {
	b.Retries = retries
	return b
}

// WithTimeout option
func (b *RedisHashBuilder) WithTimeout(timeout time.Duration) *RedisHashBuilder {
	b.Timeout = timeout
	return b
}

// WithPoolSize option
func (b *RedisHashBuilder) WithPoolSize(poolsize int) *RedisHashBuilder {
	b.PoolSize = poolsize
	return b
}

// WithKeyIdxLen option
func (b *RedisHashBuilder) WithKeyIdxLen(keyIdx int, keyLen int) *RedisHashBuilder {
	b.KeyIdx = keyIdx
	b.KeyLen = keyLen
	return b
}

// Build build a new redis cluster client
func (b *RedisHashBuilder) Build() (*RedisHash, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         b.Address,
		DialTimeout:  b.Timeout,
		ReadTimeout:  b.Timeout,
		WriteTimeout: b.Timeout,
		MaxRetries:   b.Retries,
		PoolSize:     b.PoolSize,
		Password:     b.Password,
	})
	if err := client.Ping().Err(); err != nil {
		return nil, err
	}

	return &RedisHash{
		client: client,
		keyIdx: b.KeyIdx,
		keyLen: b.KeyLen,
	}, nil
}

// RedisHash redis cluster client
type RedisHash struct {
	BaseCache

	client *redis.Client
	keyIdx int
	keyLen int
}

// Close redis client
func (rc *RedisHash) Close() error {
	return rc.client.Close()
}

// Get get a key
func (rc *RedisHash) Get(key string) ([]byte, error) {
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
func (rc *RedisHash) Set(key string, val []byte) error {
	k, f := rc.parseKey(key)
	return rc.client.HSet(k, f, val).Err()
}

// Del delete a key
func (rc *RedisHash) Del(key string) error {
	k, f := rc.parseKey(key)
	return rc.client.HDel(k, f).Err()
}

// SetNx set if not exists
func (rc *RedisHash) SetNx(key string, val []byte) (bool, error) {
	k, f := rc.parseKey(key)
	return rc.client.HSetNX(k, f, val).Result()
}

// SetBatch set batch
func (rc *RedisHash) SetBatch(keys []string, vals [][]byte) ([]error, error) {
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

func (rc *RedisHash) parseKey(key string) (string, string) {
	if len(key) > rc.keyIdx+rc.keyLen {
		return key[rc.keyIdx : rc.keyIdx+rc.keyLen], key[:rc.keyIdx] + key[rc.keyIdx+rc.keyLen:]
	} else if len(key) > rc.keyIdx {
		return key[rc.keyIdx:], key[:rc.keyIdx]
	} else {
		return "", key
	}
}

// GetBatch keys
func (rc *RedisHash) GetBatch(keys []string) ([][]byte, []error, error) {
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
