package pool

import (
	"testing"
	"time"

	"github.com/serverless/event-gateway-connector/connection"
	. "github.com/smartystreets/goconvey/convey"
)

func TestInitialWorkerConnection(t *testing.T) {
	conns := make(chan *connection.Connection, 100)
	done := make(chan bool)

	Convey("test out initializing the workers", t, func() {
		So(func() {
			go StartWorkers(10, conns, done)

			time.Sleep(1 * time.Second)
			done <- true
			time.Sleep(1 * time.Second)
		}, ShouldNotPanic)
	})

	Convey("test out sending a conn down the channel", t, func() {
		So(func() { go StartWorkers(10, conns, done) }, ShouldNotPanic)
		a := &connection.Connection{
			Space:     "/test_one",
			ID:        "test_one",
			Target:    "http://localhost:4001/",
			EventType: "awskinesis",
			AWSKinesisSource: &connection.AWSKinesisSource{
				StreamName:         "stream_one",
				Region:             "us-east-1",
				AWSAccessKeyID:     "key_one",
				AWSSecretAccessKey: "secret_key_one",
				AWSSessionToken:    "session_token_one",
			},
		}

		b := &connection.Connection{
			Space:     "/test_two",
			ID:        "test_two",
			Target:    "http://localhost:4001/",
			EventType: "awskinesis",
			AWSKinesisSource: &connection.AWSKinesisSource{
				StreamName:         "stream_two",
				Region:             "us-east-1",
				AWSAccessKeyID:     "key_two",
				AWSSecretAccessKey: "secret_key_two",
				AWSSessionToken:    "session_token_two",
			},
		}

		So(a, ShouldNotBeNil)
		So(b, ShouldNotBeNil)

		So(func() { conns <- a }, ShouldNotPanic)
		So(func() { conns <- b }, ShouldNotPanic)

		time.Sleep(10 * time.Second)
		done <- true
		time.Sleep(1 * time.Second)
	})

}
