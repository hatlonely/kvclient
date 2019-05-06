package kvclient

import (
	"fmt"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestCache_All(t *testing.T) {
	Convey("test cache all", t, func() {
		var caches1 []Cache
		var caches2 []Cache

		rch, err := NewRedisClusterHashBuilder().
			WithAddress("127.0.0.1:7002").
			WithRetries(3).
			WithTimeout(time.Duration(240)*time.Millisecond).
			WithPoolSize(15).
			WithKeyIdxLen(8, 7).
			Build()
		So(err, ShouldBeNil)
		defer rch.Close()
		caches1 = append(caches1, rch)

		rcs, err := NewRedisClusterStringBuilder().
			WithAddress("127.0.0.1:7002").
			WithRetries(3).
			WithTimeout(time.Duration(240) * time.Millisecond).
			WithExpiration(time.Duration(1) * time.Second).
			WithPoolSize(15).
			Build()
		So(err, ShouldBeNil)
		defer rcs.Close()
		caches1 = append(caches1, rcs)
		caches2 = append(caches2, rcs)

		rs, err := NewRedisStringBuilder().
			WithAddress("127.0.0.1:6379").
			WithRetries(3).
			WithTimeout(time.Duration(240) * time.Millisecond).
			WithExpiration(time.Duration(1) * time.Second).
			WithPoolSize(15).
			Build()
		So(err, ShouldBeNil)
		defer rs.Close()
		caches1 = append(caches1, rs)
		caches2 = append(caches2, rs)

		rh, err := NewRedisHashBuilder().
			WithAddress("127.0.0.1:6379").
			WithRetries(3).
			WithTimeout(time.Duration(240)*time.Millisecond).
			WithPoolSize(15).
			WithKeyIdxLen(8, 7).
			Build()
		So(err, ShouldBeNil)
		defer rh.Close()
		caches1 = append(caches1, rh)

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
		caches1 = append(caches1, aerospike)
		caches2 = append(caches2, aerospike)

		gcache := NewGcacheBuilder().Build()
		defer gcache.Close()
		caches1 = append(caches1, gcache)
		caches2 = append(caches2, gcache)

		levelDB, err := NewLevelDBBuilder().Build()
		So(err, ShouldBeNil)
		defer levelDB.Close()
		caches1 = append(caches1, levelDB)

		memcache := NewMemcacheBuilder().Build()
		defer memcache.Close()
		caches1 = append(caches1, memcache)
		caches2 = append(caches2, memcache)

		freecache := NewFreecacheBuilder().Build()
		defer freecache.Close()
		caches1 = append(caches1, freecache)
		caches2 = append(caches2, freecache)

		bigcache, err := NewBigcacheBuilder().Build()
		So(err, ShouldBeNil)
		defer bigcache.Close()
		caches1 = append(caches1, bigcache)

		for i, cache := range caches1 {
			Convey(fmt.Sprintf("loop-%v: get a key that not exists", i), func() {
				val, err := cache.Get("name")
				So(err, ShouldEqual, nil)
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
						So(err, ShouldEqual, nil)
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

				Convey(fmt.Sprintf("loop-%v: del those keys", i), func() {
					for _, key := range []string{"key1", "key2", "key3"} {
						err := cache.Del(key)
						So(err, ShouldBeNil)
					}
				})
			})

			Convey(fmt.Sprintf("loop-%v: set if not exists", i), func() {
				So(cache.Del("key4"), ShouldBeNil)

				ok, err := cache.SetNx("key4", []byte("val4"))
				So(ok, ShouldBeTrue)
				So(err, ShouldBeNil)

				ok, err = cache.SetNx("key4", []byte("val4"))
				So(ok, ShouldBeFalse)
				So(err, ShouldBeNil)
			})
		}

		for i, cache := range caches2 {
			Convey(fmt.Sprintf("loop-%v: set if not exists with expiration", i), func() {
				So(cache.Del("key4"), ShouldBeNil)

				ok, err := cache.SetExNx("key4", []byte("val4"), time.Duration(20)*time.Second)
				So(ok, ShouldBeTrue)
				So(err, ShouldBeNil)

				ok, err = cache.SetExNx("key4", []byte("val4"), time.Duration(20)*time.Second)
				So(ok, ShouldBeFalse)
				So(err, ShouldBeNil)
			})
		}
	})
}
