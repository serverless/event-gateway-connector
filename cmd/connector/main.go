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
	"github.com/serverless/event-gateway-connector/workerpool"
	"go.uber.org/zap"

	flag "github.com/ogier/pflag"

	_ "github.com/serverless/event-gateway-connector/sources/amqp"
	_ "github.com/serverless/event-gateway-connector/sources/awskinesis"
)

const prefix = "serverless-event-gateway-connector/"
const connectionsPrefix = prefix + "connections/"
const locksPrefix = prefix + "locks/jobs/"

const jobsBucketSize = 5

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

	// Watcher
	watch := kv.NewWatcher(
		namespace.NewKV(client, connectionsPrefix),
		namespace.NewWatcher(client, connectionsPrefix),
		namespace.NewKV(client, locksPrefix),
		logger.Named("KV.Watcher"))
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
	wp := workerpool.New(&workerpool.Config{
		MaxWorkers:  *maxWorkers,
		LocksPrefix: locksPrefix,
		Session:     session,
		Events:      events,
		Log:         logger.Named("WorkerPool"),
	})
	wp.Start()
	defer wp.Stop()
	logger.Debugw("started worker pool", "maxWorkers", maxWorkers)

	// KV store service
	store := kv.NewStore(namespace.NewKV(client, connectionsPrefix), jobsBucketSize, logger.Named("KV.Store"))

	// Server
	srv := httpapi.NewConfigAPI(store, *port)
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
