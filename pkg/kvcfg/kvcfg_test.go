package kvcfg

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestNewKVClient(t *testing.T) {
	Convey("test new kv client", t, func() {
		client, err := NewKVClientWithFile("../../configs/kvclient/kvclient.json")
		So(err, ShouldBeNil)
		So(client, ShouldNotBeNil)
	})
}
