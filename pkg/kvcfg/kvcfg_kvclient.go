package kvcfg

import (
	"fmt"
	"os"

	"github.com/hatlonely/kvclient/pkg/kvclient"
	"github.com/hatlonely/kvclient/pkg/mykv"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// NewKVClientWithFile create a new kv client use config file
func NewKVClientWithFile(filename string) (kvclient.KVClient, error) {
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
	return NewKVClient(config)
}

// NewKVClient create a new kvclient
func NewKVClient(config *viper.Viper) (kvclient.KVClient, error) {
	var caches []kvclient.Cache
	names := config.GetStringSlice("caches")
	for _, name := range names {
		cf := config.Sub(name)
		if cf == nil {
			return nil, fmt.Errorf("no such cache named [%v]", name)
		}
		cache, err := NewCache(cf)
		if err != nil {
			return nil, err
		}

		caches = append(caches, cache)
	}

	client := kvclient.NewBuilder().WithCaches(caches).Build()

	if config.Sub("compressor") != nil {
		compressor, err := NewCompressor(config.Sub("compressor"))
		if err != nil {
			return nil, err
		}
		client.SetCompressor(compressor)
	}

	if config.Sub("serializer") != nil {
		serializer, err := NewSerializer(config.Sub("serializer"))
		if err != nil {
			return nil, err
		}
		client.SetSerializer(serializer)
	}

	return client, nil
}

// NewCache create a new cache
func NewCache(config *viper.Viper) (kvclient.Cache, error) {
	c := config.GetString("class")
	if c == "RedisClusterString" {
		// {
		// 		"class": "RedisClusterString",
		// 		"address": "127.0.0.1:7000",
		// 		"poolSize": 30,
		// 		"timeoutMs": 1000,
		// 		"retries": 3,
		// 		"expiration": "7d"
		// }
		builder := kvclient.NewRedisClusterStringBuilder()
		if err := config.Unmarshal(builder); err != nil {
			return nil, err
		}

		return builder.Build()
	} else if c == "RedisClusterHash" {
		// {
		//     "class": "RedisClusterHash",
		//     "address": "127.0.0.1:7000",
		//     "poolSize": 30,
		//     "timeoutMs": 1000,
		//     "retries": 3,
		//     "keyIdx": 8,
		//     "keyLen": 7
		// }
		builder := kvclient.NewRedisClusterHashBuilder()
		if err := config.Unmarshal(builder); err != nil {
			return nil, err
		}
		return builder.Build()
	} else if c == "Aerospike" {
		// {
		//     "class": "Aerospike",
		//     "address": "172.31.19.27:3000,172.31.25.40:3000,172.31.23.48:3000",
		//     "namespace": "dmp",
		//     "setname": "dsp",
		//     "timeoutMs": 200,
		//     "expirationS": 604800,
		//     "retries": 4
		// }
		builder := kvclient.NewAerospikeBuilder()
		if err := config.Unmarshal(builder); err != nil {
			return nil, err
		}
		return builder.Build()
	} else if c == "Gcache" {
		// {
		//     "class": "GLocalCache",
		//     "size": 2000,
		//     "expiration": "15m"
		// }
		builder := kvclient.NewGcacheBuilder()
		if err := config.Unmarshal(builder); err != nil {
			return nil, err
		}
		return builder.Build(), nil
	} else if c == "LevelDB" {
		builder := kvclient.NewLevelDBBuilder()
		if err := config.Unmarshal(builder); err != nil {
			return nil, err
		}
		return builder.Build()
	} else if c == "Memcache" {
		builder := kvclient.NewMemcacheBuilder()
		if err := config.Unmarshal(builder); err != nil {
			return nil, err
		}
		return builder.Build(), nil
	} else if c == "Freecache" {
		builder := kvclient.NewFreecacheBuilder()
		if err := config.Unmarshal(builder); err != nil {
			return nil, err
		}
		return builder.Build(), nil
	} else if c == "Bigcache" {
		builder := kvclient.NewBigcacheBuilder()
		if err := config.Unmarshal(builder); err != nil {
			return nil, err
		}
		return builder.Build()
	}

	return nil, fmt.Errorf("no cache named [%v]", c)
}

// NewCompressor create a new compressor
func NewCompressor(config *viper.Viper) (kvclient.Compressor, error) {
	c := config.GetString("class")
	pkg := config.GetString("package")
	if pkg == "mykv" {
		if c == "Compressor" {
			return &mykv.Compressor{}, nil
		}
	}
	return nil, fmt.Errorf("no compressor named %v.%v", pkg, c)
}

// NewSerializer create a new serializer
func NewSerializer(config *viper.Viper) (kvclient.Serializer, error) {
	c := config.GetString("class")
	pkg := config.GetString("package")
	if pkg == "mykv" {
		if c == "Serializer" {
			return &mykv.Serializer{}, nil
		}
	}
	return nil, fmt.Errorf("no serializer named %v.%v", pkg, c)
}
