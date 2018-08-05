package kvclient

import (
	"testing"

	"github.com/hatlonely/kvclient/pkg/mykv"
	. "github.com/smartystreets/goconvey/convey"
)

func TestKVClient_All(t *testing.T) {
	Convey("kvclient test", t, func() {
		freecache := NewFreecacheBuilder().Build()
		// redis, err := NewRedisClusterStringBuilder().WithExpiration(time.Duration(120) * time.Second).Build()
		redis, err := NewRedisClusterHashBuilder().Build()
		So(err, ShouldBeNil)
		client := NewBuilder().
			WithCaches([]Cache{freecache, redis}).
			WithCompressor(&mykv.Compressor{}).
			WithSerializer(&mykv.Serializer{}).
			Build()

		err = client.Set(&mykv.Key{Message: "key1"}, &mykv.Val{Message: "val1"})
		err = client.Set(&mykv.Key{Message: "key3"}, &mykv.Val{Message: "val3"})
		So(err, ShouldBeNil)

		var val mykv.Val
		ok, err := client.Get(&mykv.Key{Message: "key1"}, &val)
		So(ok, ShouldBeTrue)
		So(err, ShouldBeNil)
		So(val.Message, ShouldEqual, "val1")

		keys := []interface{}{&mykv.Key{Message: "key1"}, &mykv.Key{Message: "key2"}, &mykv.Key{Message: "key3"}}
		vals := []interface{}{&mykv.Val{}, &mykv.Val{}, &mykv.Val{}}
		oks, errs, err := client.GetBatch(keys, vals)
		So(err, ShouldBeNil)
		So(oks, ShouldResemble, []bool{true, false, true})
		So(vals, ShouldResemble, []interface{}{&mykv.Val{Message: "val1"}, &mykv.Val{}, &mykv.Val{Message: "val3"}})
		So(errs, ShouldResemble, []error{nil, nil, nil})
	})
}
