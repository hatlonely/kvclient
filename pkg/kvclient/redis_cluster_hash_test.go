package kvclient

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestParseKey(t *testing.T) {
	Convey("test parse key", t, func() {
		rc := &RedisClusterHash{
			keyIdx: 8,
			keyLen: 7,
		}
		k, f := rc.parseKey("01234567890123456789")
		So(k, ShouldEqual, "8901234")
		So(f, ShouldEqual, "0123456756789")
		k, f = rc.parseKey("012345678901234")
		So(k, ShouldEqual, "8901234")
		So(f, ShouldEqual, "01234567")
		k, f = rc.parseKey("01234567")
		So(k, ShouldEqual, "")
		So(f, ShouldEqual, "01234567")
		k, f = rc.parseKey("01234")
		So(k, ShouldEqual, "")
		So(f, ShouldEqual, "01234")
	})
}
