package pool

import (
	"testing"

	"github.com/serverless/event-gateway-connector/connection"
	. "github.com/smartystreets/goconvey/convey"
)

func TestInitialWorkerConnection(t *testing.T) {
	conns := make(chan *connection.Connection, 100)

	Convey("test out initializing the workers", t, func() {
		So(func() { StartWorkers(conns, 10) }, ShouldNotPanic)
	})
}
