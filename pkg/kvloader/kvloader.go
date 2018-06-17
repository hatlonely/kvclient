package kvloader

import "sync"

// NewBuilder create a new Builder
func NewBuilder() *Builder {
	return &Builder{}
}

// Builder KVLoader builder
type Builder struct {
	producer KVProducer
	consumer KVConsumer
}

// WithProducer option
func (b *Builder) WithProducer(producer KVProducer) *Builder {
	b.producer = producer
	return b
}

// WithConsumer option
func (b *Builder) WithConsumer(consumer KVConsumer) *Builder {
	b.consumer = consumer
	return b
}

// Build a KVLoader
func (b *Builder) Build() KVLoader {
	return &kvLoader{
		producer: b.producer,
		consumer: b.consumer,
	}
}

type kvLoader struct {
	producer KVProducer
	consumer KVConsumer
}

func (l *kvLoader) Load() error {
	infoChan := make(chan *KVInfo, 10000)
	var wgp, wgc sync.WaitGroup

	if err := l.producer.Produce(&wgp, infoChan); err != nil {
		return err
	}

	if err := l.consumer.Consume(&wgc, infoChan); err != nil {
		return err
	}

	wgp.Wait()
	close(infoChan)
	wgc.Wait()

	return nil
}
