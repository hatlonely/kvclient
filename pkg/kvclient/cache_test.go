package kvclient

import (
	"fmt"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

type dsInterface interface {
	Set(key string, val []byte) (err error)
	Get(key string) (val []byte, err error) // return nil, nil if key not found
	Del(key string) (err error)
	SetBatch(keys []string, vals [][]byte) (errs []error, err error)
}

func TestCache_Set_Get_Del(t *testing.T) {
	Convey("test data source set get and del", t, func() {
		redisHash, err := NewRedisClusterHashBuilder().
			WithAddress("127.0.0.1:7002").
			WithRetries(3).
			WithTimeout(time.Duration(240)*time.Millisecond).
			WithPoolSize(15).
			WithKeyIdxLen(8, 7).
			Build()
		So(err, ShouldBeNil)
		redisString, err := NewRedisClusterStringBuilder().
			WithAddress("127.0.0.1:7002").
			WithRetries(3).
			WithTimeout(time.Duration(240) * time.Millisecond).
			WithExpiration(time.Duration(1) * time.Second).
			WithPoolSize(15).
			Build()
		So(err, ShouldBeNil)
		aerospike, err := NewAerospikeBuilder().
			WithAddress("127.0.0.1:3000").
			WithNamespace("dmp").
			WithSetName("dsp").
			WithTimeout(time.Duration(200) * time.Millisecond).
			WithRetries(4).
			WithExpiration(time.Duration(200) * time.Second).
			Build()
		So(err, ShouldBeNil)
		gcache := NewGLocalCacheBuilder().Build()

		for i, ds := range []dsInterface{redisHash, redisString, aerospike, gcache} {
			Convey(fmt.Sprintf("loop-%v: get a key that not exists", i), func() {
				val, err := ds.Get("name")
				So(err, ShouldEqual, nil)
				So(val, ShouldEqual, nil)
			})

			Convey(fmt.Sprintf("loop-%v: set a key", i), func() {
				err := ds.Set("name", []byte("hatlonely"))
				So(err, ShouldBeNil)

				Convey(fmt.Sprintf("loop-%v: then get the key", i), func() {
					val, err := ds.Get("name")
					So(err, ShouldBeNil)
					So(val, ShouldResemble, []byte("hatlonely"))
				})

				Convey(fmt.Sprintf("loop-%v: del the key", i), func() {
					err := ds.Del("name")
					So(err, ShouldBeNil)

					Convey(fmt.Sprintf("loop-%v: get the key again，it's not exists", i), func() {
						val, err := ds.Get("name")
						So(err, ShouldEqual, nil)
						So(val, ShouldEqual, nil)
					})
				})
			})
		}
	})
}

func TestCache_SetBatch(t *testing.T) {
	Convey("test data source set batch", t, func() {
		redisHash, err := NewRedisClusterHashBuilder().
			WithAddress("127.0.0.1:7002").
			WithRetries(3).
			WithTimeout(time.Duration(240) * time.Millisecond).
			WithPoolSize(15).
			Build()
		So(err, ShouldBeNil)
		redisString, err := NewRedisClusterStringBuilder().
			WithAddress("127.0.0.1:7002").
			WithRetries(3).
			WithTimeout(time.Duration(240) * time.Millisecond).
			WithPoolSize(15).
			Build()
		So(err, ShouldBeNil)
		aerospike, err := NewAerospikeBuilder().
			WithAddress("127.0.0.1:3000").
			WithNamespace("dmp").
			WithSetName("dsp").
			WithTimeout(time.Duration(200) * time.Millisecond).
			WithRetries(4).
			WithExpiration(time.Duration(200) * time.Second).
			Build()
		So(err, ShouldBeNil)
		gcache := NewGLocalCacheBuilder().Build()

		for i, ds := range []dsInterface{redisHash, redisString, aerospike, gcache} {
			Convey(fmt.Sprintf("loop-%v: set batch", i), func() {
				kvs := []*struct {
					Key string
					Val []byte
					Err error
				}{
					{"key1", []byte("val1"), nil},
					{"key2", []byte("val2"), nil},
					{"key3", []byte("val3"), nil},
				}
				keys := []string{"key1", "key2", "key3"}
				vals := [][]byte{[]byte("val1"), []byte("val2"), []byte("val3")}
				errs, err := ds.SetBatch(keys, vals)
				So(err, ShouldBeNil)
				So(errs[0], ShouldBeNil)
				So(errs[1], ShouldBeNil)
				So(errs[2], ShouldBeNil)
				So(kvs[0].Err, ShouldEqual, nil)
				So(kvs[1].Err, ShouldEqual, nil)
				So(kvs[2].Err, ShouldEqual, nil)

				Convey(fmt.Sprintf("loop-%v: then get the keys", i), func() {
					val, err := ds.Get("key1")
					So(err, ShouldBeNil)
					So(val, ShouldResemble, []byte("val1"))

					val, err = ds.Get("key2")
					So(err, ShouldBeNil)
					So(val, ShouldResemble, []byte("val2"))

					val, err = ds.Get("key3")
					So(err, ShouldBeNil)
					So(val, ShouldResemble, []byte("val3"))
				})
			})

			Convey(fmt.Sprintf("loop-%v: del those keys", i), func() {
				for _, key := range []string{"key1", "key2", "key3"} {
					err := ds.Del(key)
					So(err, ShouldBeNil)
				}
			})
		}
	})
}
