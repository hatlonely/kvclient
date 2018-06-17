kvclient -- kv storage 
======================

### 安装

使用 glide 依赖管理工具

```
git config --global url."git@gitlab.mobvista.com:".insteadOf "http://gitlab.mobvista.com"
glide get --insecure gitlab.mobvista.com/mtech/kvclient
```

### 使用示例

完整代码参见 [example](example) 目录

#### 自定义数据类型

``` go
package main

import (
    "fmt"

    "github.com/hatlonely/kvclient/pkg/cfg"
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
    client, err := cfg.NewKVClientWithFile("example.json")
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
```

### 支持的数据源与缓存

#### redis hash

使用 redis 的 hash 方式存储，会将 key 拆开成两部分分别作为 key 和 field，不支持 ttl，更节省内存

``` js
{
    "class": "RedisClusterHash",
    "address": "127.0.0.1:7000,127.0.0.1:7001",     // redis 地址
    "poolSize": 30,                                 // 链接池大小
    "timeout": "1s",                                // 超时时间
    "retries": 3,                                   // 重试次数
    "keyIdx": 8,                                    // key 开始的下标
    "keyLen": 7                                     // key 的长度
}
```

#### redis string

使用 redis 的 string 方式存储，支持 ttl，比较耗费内存

``` js
{
    "class": "RedisClusterString",
    "address": "127.0.0.1:7000,127.0.0.1:7001",     // redis 地址
    "poolSize": 30,                                 // 连接池大小
    "timeout": "1s",                                // 超时时间
    "retries": 3,                                   // 重试次数
    "expiration": "24h"                             // 默认的过期时间
}
```

#### aerospike

使用 aerospike 存储，支持 ttl，key 以固定长度存储在内存中，value 存储在 ssd 上

用（m4.xlarge 4核/16G）客户端机器测试，QPS 7.5w 的情况下，平均响应时间 0.4ms, 99.8%的请求在 1ms 内返回

受到客户端性能的影响，服务端（3台 i3.4xlarge，灌入 20亿数据）的极限 QPS 没有准确数据，在 5 台客户端机器同时访问的情况下，总QPS 达到 36w，99% 请求在 1ms 内返回

``` js
{
    "class": "Aerospike",
    "address": "172.31.19.27:3000,172.31.25.40:3000,172.31.23.48:3000", // aerospike 地址
    "namespace": "test",    // 命名空间
    "setname": "test",      // 集合名称
    "timeout": "200ms",     // 超时时间
    "expiration": "24h",    // 默认过期时间
    "retries": 4            // 重试次数
}
```

#### gcache 缓存

支持 ttl 的 LRU 本地内存缓存

``` js
{
    "class": "GLocalCache",
    "size": 2000,               // 缓存最大的容量
    "expiration": "15m"         // 过期时间
}
```

### 数据加载

数据加载模块用于数据更新，数据构造，性能测试等，支持从本地文件，s3目录，或者构造数据到数据源或者文件中

执行 `make build` 后，在 build/kvloader 目录下生成数据加载工具，configfile 指定配置文件，为了方便线上环境定期从 s3 更新数据，提供 producer.s3suffix 用来指定和时间相关的目录

```
bin/kvloader [-f configfile] [--producer.s3suffix yyymmdd]
```

下面这个配置文件表示构造 10000 个 [kvclient.MyKey](pkg/kvclient/mykv.go)/[kvclient.MyVal](pkg/kvclient/mykv.go) 到 aerospike 中

``` js
{
    "producer": {   // 数据生产者
        "class": "FakeMyKVProducer",
        "threadNum": 10,
        "total": 10000,
        "keyLen": 36,
        "valLen": 23
    },
    "consumer": {   // 数据消费者
        "class": "DBKVConsumer",
        "threadNum": 10,
        "batch": 100,
        "verbose": true,
        "kvclient": {
            "caches": [
                "aerospike"
            ],
            "compressor": {
                "package": "kvclient",
                "class": "MyCompressor"
            },
            "serializer": {
                "package": "kvclient",
                "class": "MySerializer"
            },
            "aerospike": {
                "class": "Aerospike",
                "address": "172.31.19.27:3000,172.31.25.40:3000,172.31.23.48:3000",
                "namespace": "test",
                "setname": "test",
                "timeout": "200ms",
                "expiration": "24h",
                "retries": 4
            }
        }
    }
}
```

#### 数据生产者

##### S3KVProducer

从 s3 获取数据

``` js
{
    "class": "S3KVProducer",
    "s3bucket": "mob-emr-test",     // s3 路径
    "s3prefix": "user/mtech/dmp",   // s3 前缀
    "s3suffix": "20180617",         // s3 后缀，一般为日期
    "verbose": true,                // 输出错误信息
    "threadNum": 10,                // 加载协程数
    "mod": 1,                       // 部分加载份数
    "idx": 0                        // 加载的部分
}
```

##### FileKVProducer

从文件中获取数据

``` js
{
    "class": "FileKVProducer",
    "directory": "data",            // 加载的目录
    "threadNum": 10,                // 加载的协程数
    "verbose": true,                // 输出错误信息
    "coder": {                      // 数据解码器
        "class": "MyKVCoder"
    }
}
```

##### FakeMyKVProducer

构造 [kvclient.MyKey](pkg/kvclient/mykv.go)/[kvclient.MyVal](pkg/kvclient/mykv.go) 数据

``` js
{
    "class": "FakeMyKVProducer",
    "threadNum": 10,        // 构造数据的协程数
    "total": 10000,         // 构造的数据量
    "keyLen": 36,           // 构造数据的 key 的长度
    "valLen": 23            // 构造数据的 val 的长度
}
```

#### 数据消费者

##### DBKVConsumer

使用 kv_client 将数据导入到 DB 中，aerospike/redis 等数据源

``` js
{
    "class": "DBKVConsumer",
    "threadNum": 10,        // 协程数
    "batch": 100,           // 数据写入的批量
    "verbose": true,        // 输出错误信息
    "kvclient": {           // 数据源客户端
        "caches": [
            "aerospike"
        ],
        "compressor": {
            "package": "kvclient",
            "class": "MyCompressor"
        },
        "serializer": {
            "package": "kvclient",
            "class": "MySerializer"
        },
        "aerospike": {
            "class": "Aerospike",
            "address": "172.31.19.27:3000,172.31.25.40:3000,172.31.23.48:3000",
            "namespace": "test",
            "setname": "test",
            "timeout": "200ms",
            "expiration": "24h",
            "retries": 4
        }
    }
}
```

##### FileKVConsumer

数据导出到文件中

``` js
{
    "class": "FileKVConsumer",
    "filePath": "data/my",      // 导出文件路径
    "fileNum": 10,              // 导出文件数量
    "coder": {                  // 数据编码器
        "class": "MyKVCoder"
    }
}
```

##### MekvclientConsumer

数据加载到内存中，主要在性能测试中使用，先将数据载入到内存中，在用这些数据测试客户端性能

#### 数据编解码

##### MyCoder

My 格式数据编解码([kvclient.MyKey](pkg/kvclient/mykv.go)/[kvclient.MyVal](pkg/kvclient/mykv.go))

```
gF7L1neVLzDsNtrZsgWQPXD5NixcRGIa+f/F    37z2rkMdbEOyIP53+ah0/YH
nIHfIkOMdu3wjljqIkpbx8JjcAZpGfGaH874    iCswPwL3ny4VUdn4uVTUvU6
```

### 性能测试

执行 `make build` 后，在 build/bench 目录下生成数据加载工具，configfile 指定配置文件

```
bin/bench [-f configfile]
```

``` js
{
    "producer": {   // 数据生产者
        "class": "FileKVProducer",
        "directory": "../kvloader/data",
        "threadNum": 10,
        "verbose": true,
        "coder": {
            "class": "MyKVCoder"
        }
    },
    "timeDistributionThreshold": [  // 耗时占比分布
        "300us",
        "500us",
        "800us",
        "1ms",
        "2ms",
        "5ms"
    ],
    "schedule": [   // 性能测试调度组
        {
            "readerNum": 0,     // Get 协程数
            "writerNum": 8,     // Set 协程数
            "startPercent": 0,  // 使用数据的开始位置
            "endPercent": 25,   // 使用数据的结束位置
            "times": 1          // 重复次数
        },
        {
            "readerNum": 8,
            "writerNum": 0,
            "startPercent": 25,
            "endPercent": 50,
            "times": 1
        },
        {
            "readerNum": 30,
            "writerNum": 0,
            "startPercent": 50,
            "endPercent": 100,
            "times": 10
        }
    ],
    "kvclient": {       // 被测试的数据源
        "caches": [
            "aerospike"
        ],
        "compressor": {
            "package": "kvclient",
            "class": "MyCompressor"
        },
        "serializer": {
            "package": "kvclient",
            "class": "MySerializer"
        },
        "aerospike": {
            "class": "Aerospike",
            "address": "127.0.0.1:3000,172.31.25.40:3000,172.31.23.48:3000",
            "namespace": "test",
            "setname": "test",
            "timeout": "200ms",
            "expiration": "24h",
            "retries": 4
        }
    }
}
```
