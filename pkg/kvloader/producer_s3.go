package kvloader

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/sirupsen/logrus"
	"github.com/spaolacci/murmur3"
)

// NewS3KVProducerBuilder create a new S3Producer
func NewS3KVProducerBuilder() *S3KVProducerBuilder {
	return &S3KVProducerBuilder{
		S3bucket:  "mob-emr-test",
		S3prefix:  "user/mtech/dmp",
		ThreadNum: 10,
		Verbose:   true,
		Mod:       1,
		Idx:       0,
	}
}

// S3KVProducerBuilder s3 kv producer builder
type S3KVProducerBuilder struct {
	S3bucket  string
	S3prefix  string
	ThreadNum int
	S3suffix  string
	Mod       int
	Idx       int
	Verbose   bool
	coder     KVCoder
}

// WithS3Bucket option
func (b *S3KVProducerBuilder) WithS3Bucket(s3bucket string) *S3KVProducerBuilder {
	b.S3bucket = s3bucket
	return b
}

// WithS3Prefix option
func (b *S3KVProducerBuilder) WithS3Prefix(s3prefix string) *S3KVProducerBuilder {
	b.S3prefix = s3prefix
	return b
}

// WithS3Suffix option
func (b *S3KVProducerBuilder) WithS3Suffix(s3suffix string) *S3KVProducerBuilder {
	b.S3suffix = s3suffix
	return b
}

// WithThreadNum option
func (b *S3KVProducerBuilder) WithThreadNum(threadNum int) *S3KVProducerBuilder {
	b.ThreadNum = threadNum
	return b
}

// WithModIdx option
func (b *S3KVProducerBuilder) WithModIdx(mod int, idx int) *S3KVProducerBuilder {
	b.Mod = mod
	b.Idx = idx
	return b
}

// WithVerbose option
func (b *S3KVProducerBuilder) WithVerbose(verbose bool) *S3KVProducerBuilder {
	b.Verbose = verbose
	return b
}

// WithCoder option
func (b *S3KVProducerBuilder) WithCoder(coder KVCoder) *S3KVProducerBuilder {
	b.coder = coder
	return b
}

// Build a S3KVProducer
func (b *S3KVProducerBuilder) Build() *S3KVProducer {
	retries := 3
	sess := session.Must(session.NewSession(&aws.Config{
		Region:     aws.String(endpoints.UsEast1RegionID),
		MaxRetries: &retries,
	}))

	s3service := s3.New(sess)

	return &S3KVProducer{
		s3bucket:  b.S3bucket,
		s3prefix:  b.S3prefix,
		s3suffix:  b.S3suffix,
		threadNum: b.ThreadNum,
		s3service: s3service,
		verbose:   b.Verbose,
		mod:       b.Mod,
		idx:       b.Idx,
		coder:     b.coder,
	}
}

// S3KVProducer produce key value pair from s3
type S3KVProducer struct {
	s3bucket  string
	s3prefix  string
	s3service *s3.S3
	threadNum int
	s3suffix  string
	mod       int
	idx       int
	verbose   bool
	coder     KVCoder
}

// Produce infos with multi goroutines
func (p *S3KVProducer) Produce(wg *sync.WaitGroup, infoChan chan<- *KVInfo) error {
	objs, ok, err := p.List()
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("miss _SUCCESS file")
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
				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(30)*time.Second)
				out, err := p.s3service.GetObjectWithContext(ctx, &s3.GetObjectInput{
					Bucket: aws.String(p.s3bucket),
					Key:    aws.String(obj),
				})
				if err != nil {
					logrus.WithFields(logrus.Fields{"error": err, "type": "dmploader"}).Error()
					cancel()
					continue
				}
				reader := bufio.NewReader(out.Body)
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
				out.Body.Close()
				cancel()
			}
			wg.Done()
		}()
	}

	return nil
}

// List all objects on s3
func (p *S3KVProducer) List() ([]string, bool, error) {
	var parts []string
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(30)*time.Second)
	defer cancel()

	out, err := p.s3service.ListObjectsWithContext(ctx, &s3.ListObjectsInput{
		Bucket: aws.String(p.s3bucket),
		Prefix: aws.String(fmt.Sprintf("%s/%s", p.s3prefix, p.s3suffix)),
	})

	if err != nil {
		return parts, false, err
	}

	success := false
	for _, content := range out.Contents {
		part := aws.StringValue(content.Key)
		if filepath.Base(part) == "_SUCCESS" {
			success = true
			continue
		}

		val := int(murmur3.Sum64([]byte(part)) >> 1)
		if err != nil {
			continue
		}
		if val%p.mod != p.idx {
			continue
		}
		parts = append(parts, part)
	}

	return parts, success, err
}
