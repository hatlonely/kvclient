package kvclient

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestKVClient_All(t *testing.T) {
	Convey("kvclient test", t, func() {
		redis, err := NewRedisClusterStringBuilder().WithExpiration(time.Duration(20) * time.Second).Build()
		So(err, ShouldBeNil)
		client := NewBuilder().
			WithCaches([]Cache{redis}).
			WithCompressor(&MyCompressor{}).
			WithSerializer(&MySerializer{}).
			Build()

		err = client.Set(&MyKey{"key"}, &MyVal{"val"})
		So(err, ShouldBeNil)

		var val MyVal
		ok, err := client.Get(&MyKey{"key"}, &val)
		So(ok, ShouldBeTrue)
		So(err, ShouldBeNil)
		So(val.Message, ShouldEqual, "val")
	})
}
