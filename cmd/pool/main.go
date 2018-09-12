package main

import (
	"log"
	"time"

	"github.com/serverless/event-gateway-connector/connection"
	"github.com/serverless/event-gateway-connector/pool"

	flag "github.com/ogier/pflag"
)

var workers = flag.IntP("workers", "w", 10, "number of worker processes")

func main() {
	flag.Parse()

	conns := make(chan *connection.Connection, 100)
	done := make(chan bool)

	go func() {
		a := &connection.Connection{
			Space:  "/test_one",
			ID:     "test_one",
			Target: "http://localhost:4001/",
			Type:   "awskinesis",
			AWSKinesisSource: &connection.AWSKinesisSource{
				StreamName:         "stream_one",
				Region:             "us-east-1",
				AWSAccessKeyID:     "key_one",
				AWSSecretAccessKey: "secret_key_one",
				AWSSessionToken:    "session_token_one",
			},
		}

		b := &connection.Connection{
			Space:  "/test_two",
			ID:     "test_two",
			Target: "http://localhost:4001/",
			Type:   "awskinesis",
			AWSKinesisSource: &connection.AWSKinesisSource{
				StreamName:         "stream_two",
				Region:             "us-east-1",
				AWSAccessKeyID:     "key_two",
				AWSSecretAccessKey: "secret_key_two",
				AWSSessionToken:    "session_token_two",
			},
		}

		log.Printf("testing out the new conn: %+v\n", a)
		conns <- a
		conns <- b

		time.Sleep(3 * time.Second)
		log.Printf("DONESKI\n")

		done <- true
	}()

	if err := pool.StartWorkers(*workers, conns, done); err != nil {
		log.Fatal(err)
	}
}
