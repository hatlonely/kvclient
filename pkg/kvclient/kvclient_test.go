package kvclient

import (
	"testing"
	"time"

	"github.com/hatlonely/kvclient/pkg/mykv"
	. "github.com/smartystreets/goconvey/convey"
)

func TestKVClient_All(t *testing.T) {
	Convey("kvclient test", t, func() {
		redis, err := NewRedisClusterStringBuilder().WithExpiration(time.Duration(20) * time.Second).Build()
		So(err, ShouldBeNil)
		client := NewBuilder().
			WithCaches([]Cache{redis}).
			WithCompressor(&mykv.Compressor{}).
			WithSerializer(&mykv.Serializer{}).
			Build()

		err = client.Set(&mykv.Key{Message: "key"}, &mykv.Val{Message: "val"})
		So(err, ShouldBeNil)

		var val mykv.Val
		ok, err := client.Get(&mykv.Key{Message: "key"}, &val)
		So(ok, ShouldBeTrue)
		So(err, ShouldBeNil)
		So(val.Message, ShouldEqual, "val")
	})
}
