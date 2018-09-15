package main

import (
	"context"
	"os"
	"os/signal"
	"time"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/coreos/etcd/clientv3/namespace"
	"github.com/serverless/event-gateway-connector/httpapi"
	"github.com/serverless/event-gateway-connector/kv"
	"github.com/serverless/event-gateway-connector/watcher"
	"github.com/serverless/event-gateway-connector/workerpool"
	"go.uber.org/zap"

	flag "github.com/ogier/pflag"

	_ "github.com/serverless/event-gateway-connector/sources/awskinesis"
)

const connectionsPrefix = "serverless-event-gateway-connector/connections"

var maxWorkers = flag.UintP("workers", "w", 10, "Maximum number of workers for the pool.")
var port = flag.IntP("port", "p", 4002, "Port to serve configuration API on")

func main() {
	flag.Parse()

	// logger
	rawLogger, _ := zap.NewDevelopment()
	defer rawLogger.Sync()
	logger := rawLogger.Sugar()

	// etcd client
	client, err := etcd.New(etcd.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 2 * time.Second,
	})
	if err != nil {
		logger.Fatalf("Unable to connect to etcd. Error: %s", err)
	}
	defer client.Close()

	// KV service
	store := &kv.Store{
		Client: namespace.NewKV(client, connectionsPrefix),
		Log:    logger,
	}

	// Watcher

	watch := watcher.New(client, connectionsPrefix, logger.Named("Watcher"))
	events, err := watch.Watch()
	if err != nil {
		logger.Fatalf("Unable to watch changes in etcd. Error: %s", err)
	}
	defer watch.Stop()

	// Initalize the WorkerPool
	session, err := concurrency.NewSession(client)
	if err != nil {
		logger.Fatalf("Unable to create session in etcd. Error: %s", err)
	}
	wp, err := workerpool.New(session, *maxWorkers, events, logger.Named("WorkerPool"))
	if err != nil {
		logger.Fatal("Unable to start worker pool. Error: %s", err)
	}
	go func() {
		logger.Fatal(wp.Start())
	}()
	defer wp.Stop()

	// Server
	srv := httpapi.ConfigAPI(store, *port)
	go func() {
		logger.Debugf("Starting Config API on port: %d", *port)
		logger.Fatal(srv.ListenAndServe())
	}()
	defer srv.Shutdown(context.TODO())

	// Setup signal capturing
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	<-stop

	logger.Debugf("Cleaning up resources...")
}
