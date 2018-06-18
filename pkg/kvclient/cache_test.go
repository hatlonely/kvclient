package kvclient

import (
	"fmt"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestCache_All(t *testing.T) {
	Convey("test cache all", t, func() {
		redisHash, err := NewRedisClusterHashBuilder().
			WithAddress("127.0.0.1:7002").
			WithRetries(3).
			WithTimeout(time.Duration(240)*time.Millisecond).
			WithPoolSize(15).
			WithKeyIdxLen(8, 7).
			Build()
		So(err, ShouldBeNil)
		defer redisHash.Close()
		redisString, err := NewRedisClusterStringBuilder().
			WithAddress("127.0.0.1:7002").
			WithRetries(3).
			WithTimeout(time.Duration(240) * time.Millisecond).
			WithExpiration(time.Duration(1) * time.Second).
			WithPoolSize(15).
			Build()
		So(err, ShouldBeNil)
		defer redisString.Close()
		aerospike, err := NewAerospikeBuilder().
			WithAddress("127.0.0.1:3000").
			WithNamespace("dmp").
			WithSetName("dsp").
			WithTimeout(time.Duration(200) * time.Millisecond).
			WithRetries(4).
			WithExpiration(time.Duration(200) * time.Second).
			Build()
		So(err, ShouldBeNil)
		defer aerospike.Close()
		gcache := NewGcacheBuilder().Build()
		defer gcache.Close()
		levelDB, err := NewLevelDBBuilder().Build()
		So(err, ShouldBeNil)
		defer levelDB.Close()
		memcache := NewMemcacheBuilder().Build()
		defer memcache.Close()
		freecache := NewFreecacheBuilder().Build()
		defer freecache.Close()
		bigcache, err := NewBigcacheBuilder().Build()
		So(err, ShouldBeNil)
		defer bigcache.Close()

		var caches []Cache
		caches = append(caches, redisHash)
		caches = append(caches, redisString)
		caches = append(caches, gcache)
		caches = append(caches, levelDB)
		caches = append(caches, memcache)
		caches = append(caches, aerospike)
		caches = append(caches, freecache)
		caches = append(caches, bigcache)
		for i, cache := range caches {
			Convey(fmt.Sprintf("loop-%v: get a key that not exists", i), func() {
				val, err := cache.Get("name")
				So(err, ShouldEqual, ErrNotFound)
				So(val, ShouldEqual, nil)
			})

			Convey(fmt.Sprintf("loop-%v: set a key", i), func() {
				err := cache.Set("name", []byte("hatlonely"))
				So(err, ShouldBeNil)

				Convey(fmt.Sprintf("loop-%v: then get the key", i), func() {
					val, err := cache.Get("name")
					So(err, ShouldBeNil)
					So(val, ShouldResemble, []byte("hatlonely"))
				})

				Convey(fmt.Sprintf("loop-%v: del the key", i), func() {
					err := cache.Del("name")
					So(err, ShouldBeNil)

					Convey(fmt.Sprintf("loop-%v: get the key again，it's not exists", i), func() {
						val, err := cache.Get("name")
						So(err, ShouldEqual, ErrNotFound)
						So(val, ShouldEqual, nil)
					})
				})
			})

			Convey(fmt.Sprintf("loop-%v: set batch", i), func() {
				keys := []string{"key1", "key2", "key3"}
				vals := [][]byte{[]byte("val1"), []byte("val2"), []byte("val3")}
				errs, err := cache.SetBatch(keys, vals)
				So(err, ShouldBeNil)
				So(errs[0], ShouldBeNil)
				So(errs[1], ShouldBeNil)
				So(errs[2], ShouldBeNil)

				Convey(fmt.Sprintf("loop-%v: then get the keys", i), func() {
					val, err := cache.Get("key1")
					So(err, ShouldBeNil)
					So(val, ShouldResemble, []byte("val1"))

					val, err = cache.Get("key2")
					So(err, ShouldBeNil)
					So(val, ShouldResemble, []byte("val2"))

					val, err = cache.Get("key3")
					So(err, ShouldBeNil)
					So(val, ShouldResemble, []byte("val3"))
				})
			})

			Convey(fmt.Sprintf("loop-%v: del those keys", i), func() {
				for _, key := range []string{"key1", "key2", "key3"} {
					err := cache.Del(key)
					So(err, ShouldBeNil)
				}
			})
		}
	})
}
