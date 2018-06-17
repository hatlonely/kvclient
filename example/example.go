package main

import (
	"fmt"

	"github.com/hatlonely/kvclient/pkg/kvcfg"
)

// MyKey example of key
type MyKey struct {
	Message string `json:"message,omitempty"`
}

// MyVal example of value
type MyVal struct {
	Message string `json:"message,omitempty"`
}

// MyCompressor example of compressor
type MyCompressor struct{}

// Compress key
func (k *MyCompressor) Compress(key interface{}) string {
	return key.(*MyKey).Message
}

// MySerializer example of serailizer
type MySerializer struct{}

// Marshal key
func (s *MySerializer) Marshal(val interface{}) (buf []byte, err error) {
	return []byte(val.(*MyVal).Message), nil
}

// Unmarshal key
func (s *MySerializer) Unmarshal(buf []byte, val interface{}) (err error) {
	pv, ok := val.(*MyVal)
	if !ok {
		return fmt.Errorf("val [%v] is not a type of MyVal", val)
	}
	pv.Message = string(buf)
	return nil
}

func main() {
	client, err := kvcfg.NewKVClientWithFile("example.json")
	if err != nil {
		panic(err)
	}
	client.SetCompressor(&MyCompressor{})
	client.SetSerializer(&MySerializer{})

	key1 := &MyKey{Message: "key"}
	val1 := &MyVal{Message: "val"}
	if err := client.Set(key1, val1); err != nil {
		panic(err)
	}

	key2 := key1
	val2 := &MyVal{}
	ok, err := client.Get(key2, val2)
	if err != nil {
		panic(err)
	}
	if !ok {
		fmt.Println("not found")
	} else {
		fmt.Printf("%#v\n", val2)
	}
}
