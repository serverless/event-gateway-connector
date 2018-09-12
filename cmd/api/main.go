package main

import (
	"context"
	"os"
	"os/signal"
	"time"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/namespace"
	"github.com/serverless/event-gateway-connector/httpapi"
	"github.com/serverless/event-gateway-connector/kv"
	"github.com/serverless/event-gateway-connector/watcher"
	"go.uber.org/zap"
)

const prefix = "serverless-event-gateway-connector/"

func main() {
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
		Client: namespace.NewKV(client, prefix),
		Log:    logger,
	}

	// Watcher
	watcher := watcher.New(client, prefix, logger)
	defer watcher.Stop()
	events, err := watcher.Watch()
	if err != nil {
		logger.Fatalf("Unable to watch changes in etcd. Error: %s", err)
	}
	go func() {
		for {
			event := <-events
			if event != nil {
				logger.Debugw("Configuration change detected.", "ID", event.ID, "Connection", event.Connection, "type", event.Type)
			}
		}
	}()

	// Server
	srv := httpapi.ConfigAPI(store)
	go func() {
		logger.Debugf("Starting Config API on port: 4002")
		logger.Fatal(srv.ListenAndServe())
	}()
	defer srv.Shutdown(context.TODO())

	// Setup signal capturing
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	<-stop

	logger.Debugf("Cleaning up resources...")
}
