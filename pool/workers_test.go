package pool

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/serverless/event-gateway-connector/connection"
	"github.com/serverless/event-gateway-connector/sources/awskinesis"
	"github.com/serverless/event-gateway-connector/sources/kafka"
	. "github.com/smartystreets/goconvey/convey"
)

func TestMarshalJSON(t *testing.T) {
	a := &connection.Connection{
		Space:      "/test_one",
		ID:         "test_one",
		Target:     "http://localhost:4001/",
		SourceType: "awskinesis",
		Source: &awskinesis.AWSKinesis{
			StreamName:         "stream_one",
			Region:             "us-east-1",
			AWSAccessKeyID:     "key_one",
			AWSSecretAccessKey: "secret_key_one",
			AWSSessionToken:    "session_token_one",
		},
	}

	Convey("test marshalling a valid awskinesis source", t, func() {
		data, err := json.Marshal(a)
		So(data, ShouldNotBeNil)
		So(err, ShouldBeNil)

		fmt.Printf("DEBUG -- data is: %s\n", data)
	})
}

func TestUnmarshalJSON(t *testing.T) {
	a := &connection.Connection{
		Space:      "/test_one",
		ID:         "test_one",
		Target:     "http://localhost:4001/",
		SourceType: "awskinesis",
		Source: &awskinesis.AWSKinesis{
			StreamName:         "stream_one",
			Region:             "us-east-1",
			AWSAccessKeyID:     "key_one",
			AWSSecretAccessKey: "secret_key_one",
			AWSSessionToken:    "session_token_one",
		},
	}

	b := &connection.Connection{
		Space:      "/test_one",
		ID:         "test_one",
		Target:     "http://localhost:4001/",
		SourceType: "awskinesis",
		Source:     &kafka.Kafka{Topic: "funfunfun"},
	}

	Convey("test unmarshalling a valid awskinesis source", t, func() {
		data, err := json.Marshal(a)
		So(data, ShouldNotBeNil)
		So(err, ShouldBeNil)

		var src connection.Connection
		err = json.Unmarshal(data, &src)

		So(err, ShouldBeNil)
	})

	Convey("test unmarshalling invalid awskinesis source", t, func() {
		data, err := json.Marshal(b)
		So(data, ShouldNotBeNil)
		So(err, ShouldBeNil)

		var src connection.Connection
		err = json.Unmarshal(data, &src)

		So(err, ShouldNotBeNil)
		fmt.Printf("DEBUG -- error is: %+v\n", err)
	})
}

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
			Space:      "/test_one",
			ID:         "test_one",
			Target:     "http://localhost:4001/",
			SourceType: "awskinesis",
			Source: &awskinesis.AWSKinesis{
				StreamName:         "stream_one",
				Region:             "us-east-1",
				AWSAccessKeyID:     "key_one",
				AWSSecretAccessKey: "secret_key_one",
				AWSSessionToken:    "session_token_one",
			},
		}

		b := &connection.Connection{
			Space:      "/test_two",
			ID:         "test_two",
			Target:     "http://localhost:4001/",
			SourceType: "awskinesis",
			Source: &awskinesis.AWSKinesis{
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
