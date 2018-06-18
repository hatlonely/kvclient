package mykv

import "fmt"

// Key example of key
type Key struct {
	Message string `json:"message,omitempty"`
}

// Val example of value
type Val struct {
	Message string `json:"message,omitempty"`
}

// Compressor example of compressor
type Compressor struct{}

// Compress key
func (k *Compressor) Compress(key interface{}) string {
	return key.(*Key).Message
}

// Serializer example of serailizer
type Serializer struct{}

// Marshal key
func (s *Serializer) Marshal(val interface{}) (buf []byte, err error) {
	return []byte(val.(*Val).Message), nil
}

// Unmarshal key
func (s *Serializer) Unmarshal(buf []byte, val interface{}) (err error) {
	pv, ok := val.(*Val)
	if !ok {
		return fmt.Errorf("val [%v] is not a type of MyVal", val)
	}
	pv.Message = string(buf)
	return nil
}
