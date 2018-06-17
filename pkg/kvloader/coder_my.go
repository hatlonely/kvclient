package kvloader

import (
	"fmt"
	"strings"

	"github.com/hatlonely/kvclient/pkg/kvclient"
)

// NewMyKVCoderBuilder create a new MyKVCoderBuilder
func NewMyKVCoderBuilder() *MyKVCoderBuilder {
	return &MyKVCoderBuilder{}
}

// MyKVCoderBuilder json kv coder for dmp
type MyKVCoderBuilder struct{}

// Build a MyKVCoder
func (b *MyKVCoderBuilder) Build() *MyKVCoder {
	return &MyKVCoder{}
}

// MyKVCoder coder for my
type MyKVCoder struct{}

// Decode decode info from a string
func (c *MyKVCoder) Decode(line string) (*KVInfo, error) {
	kv := strings.Split(line, "\t")
	if len(kv) != 2 {
		return nil, fmt.Errorf("len(kv) [%v] is not 2. line [%v]", len(kv), line)
	}
	key := &kvclient.MyKey{Message: kv[0]}
	val := &kvclient.MyVal{Message: kv[1]}

	return &KVInfo{
		Key: key,
		Val: val,
	}, nil
}

// Encode encode info to a string
func (c *MyKVCoder) Encode(info *KVInfo) (string, error) {
	key, ok1 := info.Key.(*kvclient.MyKey)
	val, ok2 := info.Val.(*kvclient.MyVal)
	if !ok1 {
		return "", fmt.Errorf("key [%v] is not type of kvclient.MyKey", info.Key)
	}
	if !ok2 {
		return "", fmt.Errorf("val [%v] is not type of kvclient.MyVal", info.Val)
	}

	return key.Message + "\t" + val.Message, nil
}
