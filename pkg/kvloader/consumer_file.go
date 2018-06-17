package kvloader

import (
	"fmt"
	"os"
	"sync"

	"github.com/sirupsen/logrus"
)

// NewFileKVConsumerBuilder create a new FileKVConsumerBuilder
func NewFileKVConsumerBuilder() *FileKVConsumerBuilder {
	return &FileKVConsumerBuilder{
		filePath: "data",
		fileNum:  10,
	}
}

// FileKVConsumerBuilder memory kv consumer builder
type FileKVConsumerBuilder struct {
	filePath string
	fileNum  int
	coder    KVCoder
}

// WithFilePath option
func (b *FileKVConsumerBuilder) WithFilePath(filePath string) *FileKVConsumerBuilder {
	b.filePath = filePath
	return b
}

// WithFileNum option
func (b *FileKVConsumerBuilder) WithFileNum(fileNum int) *FileKVConsumerBuilder {
	b.fileNum = fileNum
	return b
}

// WithCoder option
func (b *FileKVConsumerBuilder) WithCoder(coder KVCoder) *FileKVConsumerBuilder {
	b.coder = coder
	return b
}

// Build a FileKVConsumer
func (b *FileKVConsumerBuilder) Build() *FileKVConsumer {
	return &FileKVConsumer{
		filePath: b.filePath,
		fileNum:  b.fileNum,
		coder:    b.coder,
	}
}

// FileKVConsumer consumer for kv db
type FileKVConsumer struct {
	filePath string
	fileNum  int
	coder    KVCoder
}

// Consume infos
func (c *FileKVConsumer) Consume(wg *sync.WaitGroup, infoChan <-chan *KVInfo) error {
	for i := 0; i < c.fileNum; i++ {
		wg.Add(1)
		go func(i int) {
			writer, err := os.Create(fmt.Sprintf("%v.%v", c.filePath, i))
			if err != nil {
				panic(err)
			}
			for info := range infoChan {
				line, err := c.coder.Encode(info)
				if err != nil {
					logrus.WithFields(logrus.Fields{"error": err, "type": "FileKVConsumer"}).Warn()
					continue
				}
				writer.WriteString(line)
				writer.WriteString("\n")
			}
			wg.Done()
		}(i)
	}

	return nil
}
