package kvclient

import "fmt"

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
