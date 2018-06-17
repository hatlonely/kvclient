package kvloader

import (
	"encoding/base64"
	"math/rand"
	"sync"

	"github.com/hatlonely/kvclient/pkg/kvclient"
)

// NewFakeMyKVProducerBuilder create a new FakeMyKVProducerBuilder
func NewFakeMyKVProducerBuilder() *FakeMyKVProducerBuilder {
	return &FakeMyKVProducerBuilder{
		threadNum: 10,
		total:     20,
		keyLen:    36,
		valLen:    23,
	}
}

// FakeMyKVProducerBuilder fake my kv producer builder
type FakeMyKVProducerBuilder struct {
	threadNum int
	total     int
	keyLen    int
	valLen    int
}

// WithThreadNum option
func (b *FakeMyKVProducerBuilder) WithThreadNum(threadNum int) *FakeMyKVProducerBuilder {
	b.threadNum = threadNum
	return b
}

// WithTotal option
func (b *FakeMyKVProducerBuilder) WithTotal(total int) *FakeMyKVProducerBuilder {
	b.total = total
	return b
}

// WithKeyLen option
func (b *FakeMyKVProducerBuilder) WithKeyLen(keyLen int) *FakeMyKVProducerBuilder {
	b.keyLen = keyLen
	return b
}

// WithValLen option
func (b *FakeMyKVProducerBuilder) WithValLen(valLen int) *FakeMyKVProducerBuilder {
	b.valLen = valLen
	return b
}

// Build a FakeMyKVProducer
func (b *FakeMyKVProducerBuilder) Build() *FakeMyKVProducer {
	return &FakeMyKVProducer{
		threadNum: b.threadNum,
		total:     b.total,
		keyLen:    b.keyLen,
		valLen:    b.valLen,
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
		Key: &kvclient.MyKey{Message: p.RandBytes(p.keyLen)},
		Val: &kvclient.MyVal{Message: p.RandBytes(p.valLen)},
	}
}

// RandBytes for my key and value
func (p *FakeMyKVProducer) RandBytes(length int) string {
	buf := make([]byte, length)
	rand.Read(buf)
	return base64.StdEncoding.EncodeToString(buf)[:length]
}
