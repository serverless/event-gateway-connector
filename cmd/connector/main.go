package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
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

	// Setup signal capturing
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	// Logger
	rawLogger, _ := zap.NewDevelopment()
	defer rawLogger.Sync()
	logger := rawLogger.Sugar()

	// etcd client
	client, err := etcd.New(etcd.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 2 * time.Second,
	})
	if err != nil {
		logger.Fatalf("unable to connect to etcd. Error: %s", err)
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
		logger.Fatalf("unable to watch changes in etcd. Error: %s", err)
	}
	defer watch.Stop()

	// Initalize the WorkerPool
	session, err := concurrency.NewSession(client)
	if err != nil {
		logger.Fatalf("unable to create session in etcd. Error: %s", err)
	}
	wp := workerpool.New(session, *maxWorkers, events, logger.Named("WorkerPool"))
	go wp.Start()
	defer wp.Stop()

	// Server
	srv := httpapi.ConfigAPI(store, *port)
	go func() {
		logger.Debugf("starting Config API on port: %d", *port)
		if err := srv.ListenAndServe(); err != nil {
			if err != http.ErrServerClosed {
				logger.Errorw("unable to start Config API server", "error", err)
			}

			stop <- os.Interrupt
		}
	}()
	defer srv.Shutdown(context.TODO())

	<-stop

	logger.Debugf("cleaning up resources...")
}
