package kvloader

import "sync"

// NewMemKVConsumerBuilder create a new MemKVConsumerBuilder
func NewMemKVConsumerBuilder() *MemKVConsumerBuilder {
	return &MemKVConsumerBuilder{}
}

// MemKVConsumerBuilder memory kv consumer builder
type MemKVConsumerBuilder struct{}

// Build a MemKVConsumer
func (b *MemKVConsumerBuilder) Build() *MemKVConsumer {
	return &MemKVConsumer{
		Infos: []*KVInfo{},
	}
}

// MemKVConsumer consumer for kv db
type MemKVConsumer struct {
	Infos []*KVInfo
}

// Consume infos
func (c *MemKVConsumer) Consume(wg *sync.WaitGroup, infoChan <-chan *KVInfo) error {
	wg.Add(1)
	go func() {
		for info := range infoChan {
			c.Infos = append(c.Infos, info)
		}
		wg.Done()
	}()
	return nil
}
