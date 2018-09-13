package watcher

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/coreos/etcd/mvcc/mvccpb"

	"github.com/serverless/event-gateway-connector/connection"
	"go.uber.org/zap"

	"github.com/coreos/etcd/clientv3/namespace"

	etcd "github.com/coreos/etcd/clientv3"
)

// Watcher watches etcd directory and emits events when Connection configuration was added or deleted.
type Watcher struct {
	kvClient    etcd.KV
	watchClient etcd.Watcher
	stopCh      chan struct{}
	log         *zap.SugaredLogger
}

// New creates new Watcher instance
func New(client *etcd.Client, prefix string, log *zap.SugaredLogger) *Watcher {
	return &Watcher{
		kvClient:    namespace.NewKV(client.KV, prefix),
		watchClient: namespace.NewWatcher(client.Watcher, prefix),
		stopCh:      make(chan struct{}),
		log:         log,
	}
}

// Watch function also emits events for pre-existing key/value pairs.
func (w *Watcher) Watch() (<-chan *Event, error) {
	eventsCh := make(chan *Event)

	existingValues, err := w.list()
	if err != nil {
		return nil, fmt.Errorf("listing existing values faield: %s", err)
	}

	go func() {
		defer w.watchClient.Close()
		defer close(eventsCh)

		// populate channel with events about existing values
		for _, existingValue := range existingValues {
			eventsCh <- existingValue
		}

		watchCh := w.watchClient.Watch(context.TODO(), "", etcd.WithPrefix())
		for resp := range watchCh {
			select {
			case <-w.stopCh:
				return
			default:
			}

			for _, watchEvent := range resp.Events {
				event := &Event{ID: extractIDFromKey(watchEvent.Kv.Key)}

				if watchEvent.Type == mvccpb.PUT {
					event.Type = Created

					conn := &connection.Connection{}
					if err := json.Unmarshal(watchEvent.Kv.Value, conn); err != nil {
						w.log.Errorf("unmarshaling payload failed: %s", err)
					}

					event.Connection = conn
				} else {
					event.Type = Deleted
				}

				eventsCh <- event
			}
		}
	}()

	return eventsCh, nil
}

// Stop watching changes in etcd.
func (w *Watcher) Stop() {
	close(w.stopCh)
}

const (
	// Created happens when Conneciton was added to configuration.
	Created int = iota
	// Deleted happens when Connection was deleted.
	Deleted
)

// Event represents event happened in Connections configuration
type Event struct {
	Type       int
	ID         connection.ID
	Connection *connection.Connection
}

// list retruns existing key/value pairs as events.
func (w *Watcher) list() ([]*Event, error) {
	resp, err := w.kvClient.Get(context.TODO(), "\x00", etcd.WithFromKey())
	if err != nil {
		return nil, err
	}

	list := []*Event{}
	for _, kv := range resp.Kvs {
		conn := &connection.Connection{}
		if err := json.Unmarshal(kv.Value, conn); err != nil {
			return nil, err
		}

		list = append(list, &Event{
			Type:       Created,
			ID:         conn.ID,
			Connection: conn,
		})
	}

	return list, nil
}

func extractIDFromKey(key []byte) connection.ID {
	return connection.ID(strings.Split(string(key), "/")[1])
}
