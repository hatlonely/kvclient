package kvloader

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"

	"github.com/sirupsen/logrus"
)

// NewFileKVProducerBuilder create a new FileProducer
func NewFileKVProducerBuilder() *FileKVProducerBuilder {
	return &FileKVProducerBuilder{
		directory: "data",
		threadNum: 10,
		verbose:   true,
	}
}

// FileKVProducerBuilder file kv producer builder
type FileKVProducerBuilder struct {
	directory string
	threadNum int
	verbose   bool
	coder     KVCoder
}

// WithDirectory option
func (b *FileKVProducerBuilder) WithDirectory(directory string) *FileKVProducerBuilder {
	b.directory = directory
	return b
}

// WithThreadNum option
func (b *FileKVProducerBuilder) WithThreadNum(threadNum int) *FileKVProducerBuilder {
	b.threadNum = threadNum
	return b
}

// WithVerbose option
func (b *FileKVProducerBuilder) WithVerbose(verbose bool) *FileKVProducerBuilder {
	b.verbose = verbose
	return b
}

// WithCoder option
func (b *FileKVProducerBuilder) WithCoder(coder KVCoder) *FileKVProducerBuilder {
	b.coder = coder
	return b
}

// Build a DBKVConsumer
func (b *FileKVProducerBuilder) Build() *FileKVProducer {
	return &FileKVProducer{
		directory: b.directory,
		threadNum: b.threadNum,
		verbose:   b.verbose,
		coder:     b.coder,
	}
}

// FileKVProducer produce key value from file
type FileKVProducer struct {
	directory string
	threadNum int
	verbose   bool
	coder     KVCoder
}

// Produce infos with multi goroutines
func (p *FileKVProducer) Produce(wg *sync.WaitGroup, infoChan chan<- *KVInfo) error {
	objs, err := p.List()
	if err != nil {
		return err
	}
	objChan := make(chan string, len(objs))
	go func() {
		for _, obj := range objs {
			objChan <- obj
		}
		close(objChan)
	}()

	for i := 0; i < p.threadNum; i++ {
		wg.Add(1)

		go func() {
			for obj := range objChan {
				fp, err := os.Open(fmt.Sprintf("%v/%v", p.directory, obj))
				defer fp.Close()
				reader := bufio.NewReader(fp)
				if err != nil {
					logrus.WithFields(logrus.Fields{"error": err, "type": "fileloader"}).Error()
					continue
				}

				for {
					line, err := reader.ReadString('\n')
					if err != nil {
						if err != io.EOF && p.verbose {
							logrus.WithFields(logrus.Fields{"error": err, "type": "dmploader"}).Warn()
						}
						break
					}
					info, err := p.coder.Decode(line[:len(line)-1])
					if err != nil {
						if p.verbose {
							logrus.WithFields(logrus.Fields{"error": err, "type": "dmploader"}).Warn()
						}
						continue
					}
					infoChan <- info
				}
			}
			wg.Done()
		}()
	}

	return nil
}

// List all files on the directory
func (p *FileKVProducer) List() ([]string, error) {
	var parts []string

	infos, err := ioutil.ReadDir(p.directory)
	if err != nil {
		return parts, err
	}

	for _, info := range infos {
		if info.IsDir() {
			continue
		}
		parts = append(parts, info.Name())
	}

	return parts, nil
}
