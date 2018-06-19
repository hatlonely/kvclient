package kvloader

import (
	"encoding/base64"
	"math/rand"
	"sync"

	"github.com/hatlonely/kvclient/pkg/mykv"
)

// NewFakeMyKVProducerBuilder create a new FakeMyKVProducerBuilder
func NewFakeMyKVProducerBuilder() *FakeMyKVProducerBuilder {
	return &FakeMyKVProducerBuilder{
		ThreadNum: 10,
		Total:     20,
		KeyLen:    36,
		ValLen:    23,
	}
}

// FakeMyKVProducerBuilder fake my kv producer builder
type FakeMyKVProducerBuilder struct {
	ThreadNum int
	Total     int
	KeyLen    int
	ValLen    int
}

// WithThreadNum option
func (b *FakeMyKVProducerBuilder) WithThreadNum(threadNum int) *FakeMyKVProducerBuilder {
	b.ThreadNum = threadNum
	return b
}

// WithTotal option
func (b *FakeMyKVProducerBuilder) WithTotal(total int) *FakeMyKVProducerBuilder {
	b.Total = total
	return b
}

// WithKeyLen option
func (b *FakeMyKVProducerBuilder) WithKeyLen(keyLen int) *FakeMyKVProducerBuilder {
	b.KeyLen = keyLen
	return b
}

// WithValLen option
func (b *FakeMyKVProducerBuilder) WithValLen(valLen int) *FakeMyKVProducerBuilder {
	b.ValLen = valLen
	return b
}

// Build a FakeMyKVProducer
func (b *FakeMyKVProducerBuilder) Build() *FakeMyKVProducer {
	return &FakeMyKVProducer{
		threadNum: b.ThreadNum,
		total:     b.Total,
		keyLen:    b.KeyLen,
		valLen:    b.ValLen,
	}
}

// FakeMyKVProducer fake key value pair
type FakeMyKVProducer struct {
	threadNum int
	total     int
	keyLen    int
	valLen    int
}

// Produce fake infos with multi goroutines
func (p *FakeMyKVProducer) Produce(wg *sync.WaitGroup, infoChan chan<- *KVInfo) error {
	emptychan := make(chan struct{}, 1000)
	go func() {
		for i := 0; i < p.total; i++ {
			emptychan <- struct{}{}
		}
		close(emptychan)
	}()

	for i := 0; i < p.threadNum; i++ {
		wg.Add(1)

		go func() {
			for range emptychan {
				infoChan <- p.Fake()
			}
			wg.Done()
		}()
	}

	return nil
}

// Fake a my info
func (p *FakeMyKVProducer) Fake() *KVInfo {
	return &KVInfo{
		Key: &mykv.Key{Message: p.RandBytes(p.keyLen)},
		Val: &mykv.Val{Message: p.RandBytes(p.valLen)},
	}
}

// RandBytes for my key and value
func (p *FakeMyKVProducer) RandBytes(length int) string {
	buf := make([]byte, length)
	rand.Read(buf)
	return base64.StdEncoding.EncodeToString(buf)[:length]
}
