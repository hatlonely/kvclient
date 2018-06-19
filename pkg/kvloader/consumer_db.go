package kvloader

import (
	"sync"

	"github.com/hatlonely/kvclient/pkg/kvclient"
)

// NewDBKVConsumerBuilder create a new DBKVConsumerBuilder
func NewDBKVConsumerBuilder() *DBKVConsumerBuilder {
	return &DBKVConsumerBuilder{
		ThreadNum: 10,
		Batch:     100,
	}
}

// DBKVConsumerBuilder db kv consumer builder
type DBKVConsumerBuilder struct {
	ThreadNum int
	Batch     int
	Verbose   bool
	kvclient  kvclient.KVClient
}

// WithThreadNum option
func (b *DBKVConsumerBuilder) WithThreadNum(threadNum int) *DBKVConsumerBuilder {
	b.ThreadNum = threadNum
	return b
}

// WithBatch option
func (b *DBKVConsumerBuilder) WithBatch(batch int) *DBKVConsumerBuilder {
	b.Batch = batch
	return b
}

// WithVerbose option
func (b *DBKVConsumerBuilder) WithVerbose(verbose bool) *DBKVConsumerBuilder {
	b.Verbose = verbose
	return b
}

// WithKVClient option
func (b *DBKVConsumerBuilder) WithKVClient(kvclient kvclient.KVClient) *DBKVConsumerBuilder {
	b.kvclient = kvclient
	return b
}

// Build a DBKVConsumer
func (b *DBKVConsumerBuilder) Build() *DBKVConsumer {
	return &DBKVConsumer{
		threadNum: b.ThreadNum,
		batch:     b.Batch,
		verbose:   b.Verbose,
		kvclient:  b.kvclient,
	}
}

// DBKVConsumer consumer for kv db
type DBKVConsumer struct {
	threadNum int
	batch     int
	verbose   bool
	kvclient  kvclient.KVClient
}

// Consume infos
func (c *DBKVConsumer) Consume(wg *sync.WaitGroup, infoChan <-chan *KVInfo) error {
	for i := 0; i < c.threadNum; i++ {
		wg.Add(1)
		go func() {
			var keys []interface{}
			var vals []interface{}
			for info := range infoChan {
				keys = append(keys, info.Key)
				vals = append(vals, info.Val)
				if len(keys) == c.batch {
					c.kvclient.SetBatch(keys, vals)
					keys = keys[:0]
					vals = vals[:0]
				}
			}
			c.kvclient.SetBatch(keys, vals)
			wg.Done()
		}()
	}

	return nil
}
