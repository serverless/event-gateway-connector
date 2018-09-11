package main

import (
	"log"

	"github.com/serverless/event-gateway-connector/connection"
	"github.com/serverless/event-gateway-connector/pool"

	flag "github.com/ogier/pflag"
)

var workers = flag.IntP("workers", "w", 10, "number of worker processes")

func main() {
	flag.Parse()

	conns := make(chan *connection.Connection, 100)

	if err := pool.StartWorkers(conns, *workers); err != nil {
		log.Fatal(err)
	}
}
