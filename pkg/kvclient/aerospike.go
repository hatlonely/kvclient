package kvclient

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aerospike/aerospike-client-go"
)

// NewAerospikeBuilder create a builder
func NewAerospikeBuilder() *AerospikeBuilder {
	return &AerospikeBuilder{
		Address:   "127.0.0.1:3000",
		Namespace: "dmp",
		Setname:   "dsp",
		Timeout:   time.Duration(1000) * time.Millisecond,
		Retries:   3,
	}
}

// AerospikeBuilder builder
type AerospikeBuilder struct {
	Address    string
	Namespace  string
	Setname    string
	Timeout    time.Duration
	Retries    int
	expiration time.Duration
}

// WithAddress option
func (b *AerospikeBuilder) WithAddress(address string) *AerospikeBuilder {
	b.Address = address
	return b
}

// WithNamespace option
func (b *AerospikeBuilder) WithNamespace(namespace string) *AerospikeBuilder {
	b.Namespace = namespace
	return b
}

// WithSetName option
func (b *AerospikeBuilder) WithSetName(setname string) *AerospikeBuilder {
	b.Setname = setname
	return b
}

// WithTimeout option
func (b *AerospikeBuilder) WithTimeout(timeout time.Duration) *AerospikeBuilder {
	b.Timeout = timeout
	return b
}

// WithRetries option
func (b *AerospikeBuilder) WithRetries(retries int) *AerospikeBuilder {
	b.Retries = retries
	return b
}

// WithExpiration option
func (b *AerospikeBuilder) WithExpiration(expiration time.Duration) *AerospikeBuilder {
	b.expiration = expiration
	return b
}

// Build a new aerospike
func (b *AerospikeBuilder) Build() (*Aerospike, error) {
	var hosts []*aerospike.Host
	for _, addr := range strings.Split(b.Address, ",") {
		hostAndPort := strings.Split(addr, ":")
		if len(hostAndPort) == 1 {
			hosts = append(hosts, aerospike.NewHost(hostAndPort[0], 3000))
		}
		if len(hostAndPort) == 2 {
			port, err := strconv.Atoi(hostAndPort[1])
			if err != nil {
				continue
			}
			hosts = append(hosts, aerospike.NewHost(hostAndPort[0], port))
		}
	}

	rpolicy := aerospike.NewPolicy()
	rpolicy.Timeout = b.Timeout
	rpolicy.MaxRetries = b.Retries

	wpolicy := aerospike.NewWritePolicy(0, uint32(b.expiration)/uint32(time.Second))
	wpolicy.BasePolicy.Timeout = b.Timeout
	wpolicy.BasePolicy.MaxRetries = b.Retries

	client, err := aerospike.NewClientWithPolicyAndHost(nil, hosts...)
	if err != nil {
		return nil, err
	}

	return &Aerospike{
		client:    client,
		rpolicy:   rpolicy,
		wpolicy:   wpolicy,
		namespace: b.Namespace,
		setname:   b.Setname,
	}, nil
}

// Aerospike datasource
type Aerospike struct {
	client    *aerospike.Client
	rpolicy   *aerospike.BasePolicy
	wpolicy   *aerospike.WritePolicy
	namespace string
	setname   string
}

// Get a key
func (as *Aerospike) Get(key string) ([]byte, error) {
	ak, err := aerospike.NewKey(as.namespace, as.setname, key)
	if err != nil {
		return nil, err
	}
	record, err := as.client.Get(as.rpolicy, ak)
	if err != nil {
		return nil, err
	}
	if record != nil && record.Bins[""] != nil {
		if buf, ok := record.Bins[""].([]byte); ok {
			return buf, nil
		}
	}
	return nil, nil
}

// Set a key
func (as *Aerospike) Set(key string, val []byte) error {
	ak, err := aerospike.NewKey(as.namespace, as.setname, key)
	if err != nil {
		return err
	}

	if err := as.client.PutBins(as.wpolicy, ak, aerospike.NewBin("", val)); err != nil {
		return err
	}

	return nil
}

// Del a key
func (as *Aerospike) Del(key string) error {
	ak, err := aerospike.NewKey(as.namespace, as.setname, key)
	if err != nil {
		return err
	}
	if _, err := as.client.Delete(nil, ak); err != nil {
		return err
	}

	return nil
}

// SetEx set with expiration
func (as *Aerospike) SetEx(key string, val []byte, expiration time.Duration) error {
	ak, err := aerospike.NewKey(as.namespace, as.setname, key)
	if err != nil {
		return err
	}

	wpolicy := aerospike.NewWritePolicy(0, uint32(expiration)/uint32(time.Second))
	wpolicy.BasePolicy.Timeout = as.wpolicy.BasePolicy.Timeout
	wpolicy.BasePolicy.MaxRetries = as.wpolicy.BasePolicy.MaxRetries

	if err := as.client.PutBins(wpolicy, ak, aerospike.NewBin("", val)); err != nil {
		return err
	}

	return nil
}

// SetNx set if not exists
func (as *Aerospike) SetNx(key string, val []byte) error {
	val, err := as.Get(key)
	if err != nil {
		return err
	}

	if val != nil {
		return nil
	}

	return as.Set(key, val)
}

// SetExNx set if not exists with expiration
func (as *Aerospike) SetExNx(key string, val []byte, expiration time.Duration) error {
	val, err := as.Get(key)
	if err != nil {
		return err
	}

	if val != nil {
		return nil
	}

	return as.SetEx(key, val, expiration)
}

// SetBatch keys vals
func (as *Aerospike) SetBatch(keys []string, vals [][]byte) ([]error, error) {
	if len(keys) != len(vals) {
		return nil, fmt.Errorf("assert len(keys)[%v] == len(vals)[%v] failed", len(keys), len(vals))
	}

	errs := make([]error, len(keys))
	for i := range keys {
		errs[i] = as.Set(keys[i], vals[i])
	}

	return errs, nil
}
