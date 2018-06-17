package kvloader

import (
	"testing"
)

func TestRandBytes(t *testing.T) {
	p := &FakeMyKVProducer{}
	t.Log(p.RandBytes(24))
}
