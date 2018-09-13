package main

import (
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
	events, err := watcher.Watch(client, prefix, nil)
	if err != nil {
		logger.Fatalf("Unable to watch changes in etcd. Error: %s", err)
	}
	go func() {
		for {
			event := <-events
			logger.Debugw("Change detected.", "key", event.Key, "value", string(event.Value), "type", event.Type)
		}
	}()

	// Server
	logger.Debugf("Starting Config API on port: 4002")
	logger.Fatal(httpapi.StartConfigAPI(store))
}
