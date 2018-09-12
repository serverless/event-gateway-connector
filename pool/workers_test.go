package pool

import (
	"testing"

	"github.com/serverless/event-gateway-connector/connection"
	. "github.com/smartystreets/goconvey/convey"
)

func TestInitialWorkerConnection(t *testing.T) {
	conns := make(chan *connection.Connection, 100)
	done := make(chan bool)

	Convey("test out initializing the workers", t, func() {
		So(func() { StartWorkers(10, conns, done) }, ShouldNotPanic)
	})
}
