package kvloader

import (
	"sync"
)

// KVInfo key value pair
type KVInfo struct {
	Key interface{} `json:"key,omitempty"`
	Val interface{} `json:"val,omitempty"`
}

// KVProducer produce key value info
type KVProducer interface {
	Produce(wg *sync.WaitGroup, infoChan chan<- *KVInfo) error
}

// KVConsumer consume key value info
type KVConsumer interface {
	Consume(wg *sync.WaitGroup, infoChan <-chan *KVInfo) error
}

// KVCoder Encode/Decode key value info from a string
type KVCoder interface {
	Decode(line string) (*KVInfo, error)
	Encode(info *KVInfo) (string, error)
}

// KVLoader load infos
type KVLoader interface {
	Load() error
}
