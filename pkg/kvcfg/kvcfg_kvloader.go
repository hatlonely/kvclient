package kvcfg

import (
	"fmt"
	"os"

	"github.com/hatlonely/kvclient/pkg/kvloader"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// NewKVLoaderWithFile create a new kv loader use config file
func NewKVLoaderWithFile(filename string) (kvloader.KVLoader, error) {
	config := viper.New()
	config.SetConfigType("json")
	fp, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	if err := config.ReadConfig(fp); err != nil {
		return nil, err
	}

	config.BindPFlags(pflag.CommandLine)
	return NewKVLoader(config)
}

// NewKVLoader create a new kv loader
func NewKVLoader(config *viper.Viper) (kvloader.KVLoader, error) {
	producer, err := NewKVProducer(config.Sub("producer"))
	if err != nil {
		return nil, err
	}
	consumer, err := NewKVConsumer(config.Sub("consumer"))
	if err != nil {
		return nil, err
	}

	return kvloader.NewBuilder().WithProducer(producer).WithConsumer(consumer).Build(), nil
}

// NewKVCoder create a new kvloader
func NewKVCoder(config *viper.Viper) (kvloader.KVCoder, error) {
	c := config.GetString("class")
	if c == "MyKVCoder" {
		return kvloader.NewMyKVCoderBuilder().Build(), nil
	}

	return nil, fmt.Errorf("no kvcoder named %v", c)
}

// NewKVProducer create a new kv producer
func NewKVProducer(config *viper.Viper) (kvloader.KVProducer, error) {
	c := config.GetString("class")
	if c == "S3KVProducer" {
		// {
		// 	"class": "S3KVProducer",
		// 	"s3bucket": "mob-emr-test",
		// 	"s3prefix": "user/mtech/dmp",
		// 	"threadNum": 10,
		// 	"s3suffix": "20180614",
		// 	"mod": 1,
		// 	"idx": 0,
		// 	"verbose": true,
		// 	"coder": {
		// 		"class": "DMPJSONKVCoder"
		// 	}
		// }
		coder, err := NewKVCoder(config.Sub("coder"))
		if err != nil {
			return nil, err
		}
		builder := kvloader.NewS3KVProducerBuilder()
		if err := config.Unmarshal(builder); err != nil {
			return nil, err
		}
		return builder.WithCoder(coder).Build(), nil
	} else if c == "FileKVProducer" {
		// {
		// 	"class": "FileKVProducer",
		// 	"directory": "data",
		// 	"threadNum": 10,
		// 	"verbose": true,
		// 	"coder": {
		// 		"class": "DMPJSONKVCoder"
		// 	}
		// }
		coder, err := NewKVCoder(config.Sub("coder"))
		if err != nil {
			return nil, err
		}
		builder := kvloader.NewFileKVProducerBuilder()
		if err := config.Unmarshal(builder); err != nil {
			return nil, err
		}
		return builder.WithCoder(coder).Build(), nil
	} else if c == "FakeMyKVProducer" {
		// {
		// 	"class": "FakeMyKVProducer",
		// 	"threadNum": 10,
		// 	"total": 100000,
		// 	"keyLen": 36,
		// 	"valLen": 23
		// }
		builder := kvloader.NewFakeMyKVProducerBuilder()
		if err := config.Unmarshal(builder); err != nil {
			return nil, err
		}
		return builder.Build(), nil
	}

	return nil, fmt.Errorf("no kvproducer named %v", c)
}

// NewKVConsumer create a new kv consumer
func NewKVConsumer(config *viper.Viper) (kvloader.KVConsumer, error) {
	c := config.GetString("class")
	if c == "DBKVConsumer" {
		// {
		// 	"class": "DBKVConsumer",
		// 	"threadNum": 10,
		// 	"batch": 100,
		// 	"verbose": true,
		// 	"kvclient": {
		// 		"caches": [
		// 			"aerospike"
		// 		],
		// 		"compressor": {
		// 			"package": "mykv",
		// 			"class": "Compressor"
		// 		},
		// 		"serializer": {
		// 			"package": "mykv",
		// 			"class": "Serializer"
		// 		},
		// 		"aerospike": {
		// 			"class": "Aerospike",
		// 			"address": "172.31.19.27:3000,172.31.25.40:3000,172.31.23.48:3000",
		// 			"namespace": "test",
		// 			"setname": "test",
		// 			"timeoutMs": 200,
		// 			"expirationS": 604800,
		// 			"retries": 4
		// 		}
		// 	}
		// }
		kvclient, err := NewKVClient(config.Sub("kvclient"))
		if err != nil {
			return nil, err
		}
		builder := kvloader.NewDBKVConsumerBuilder()
		if err := config.Unmarshal(builder); err != nil {
			return nil, err
		}
		return builder.WithKVClient(kvclient).Build(), nil
	} else if c == "FileKVConsumer" {
		// {
		// 	"class": "FileKVConsumer",
		// 	"filePath": "data",
		// 	"fileNum": 10,
		// 	"coder": {
		// 		"class": "MyKVCoder"
		// 	}
		// }
		coder, err := NewKVCoder(config.Sub("coder"))
		if err != nil {
			return nil, err
		}
		builder := kvloader.NewFileKVConsumerBuilder()
		if err := config.Unmarshal(builder); err != nil {
			return nil, err
		}
		return builder.WithCoder(coder).Build(), nil
	} else if c == "MemKVConsumer" {
		return kvloader.NewMemKVConsumerBuilder().Build(), nil
	}

	return nil, fmt.Errorf("no kvconsumer named %v", c)
}
