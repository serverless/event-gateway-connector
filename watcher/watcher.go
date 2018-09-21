package watcher

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/coreos/etcd/mvcc/mvccpb"

	"github.com/serverless/event-gateway-connector/connection"
	"go.uber.org/zap"

	"github.com/coreos/etcd/clientv3/namespace"

	etcd "github.com/coreos/etcd/clientv3"
)

// Watcher watches etcd directory and emits events when Connection configuration was added or deleted.
type Watcher struct {
	kvClient      etcd.KV
	locksKVClient etcd.KV
	watchClient   etcd.Watcher
	stopCh        chan struct{}
	log           *zap.SugaredLogger
}

// New creates new Watcher instance
func New(client *etcd.Client, connectionsPrefix, locksPrefix string, log *zap.SugaredLogger) *Watcher {
	return &Watcher{
		kvClient:      namespace.NewKV(client.KV, connectionsPrefix),
		locksKVClient: namespace.NewKV(client.KV, locksPrefix),
		watchClient:   namespace.NewWatcher(client.Watcher, connectionsPrefix),
		stopCh:        make(chan struct{}),
		log:           log,
	}
}

// Watch function also emits events for pre-existing key/value pairs.
func (w *Watcher) Watch() (<-chan *Event, error) {
	eventsCh := make(chan *Event)

	// perioducally populate channel with events about existing connections without locks
	go func() {
		for {
			existingValues, err := w.list()
			if err != nil {
				w.log.Errorf("listing existing values failed: %s", err)
			}

			for _, existingValue := range existingValues {
				eventsCh <- existingValue
			}

			time.Sleep(time.Second * 3)
		}
	}()

	go func() {
		defer w.watchClient.Close()
		defer close(eventsCh)

		watchCh := w.watchClient.Watch(context.TODO(), "", etcd.WithPrefix())
		for resp := range watchCh {
			select {
			case <-w.stopCh:
				return
			default:
			}

			for _, watchEvent := range resp.Events {
				connectionID := connection.ID(string(watchEvent.Kv.Key))

				switch watchEvent.Type {
				case mvccpb.PUT:
					if watchEvent.Kv.CreateRevision != watchEvent.Kv.ModRevision {
						// connection was updated. Emit Delete event first and then Created event.
						eventsCh <- &Event{Type: Deleted, ID: connectionID}
					}

					conn := &connection.Connection{}
					if err := json.Unmarshal(watchEvent.Kv.Value, conn); err != nil {
						w.log.Errorf("unmarshaling payload failed: %s", err)
						continue
					}

					eventsCh <- &Event{Type: Created, ID: connectionID, Connection: conn}
				case mvccpb.DELETE:
					eventsCh <- &Event{Type: Deleted, ID: connectionID}
				}
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
	connections, err := w.kvClient.Get(context.TODO(), "\x00", etcd.WithFromKey())
	if err != nil {
		return nil, err
	}

	locks, err := w.locksKVClient.Get(context.TODO(), "\x00", etcd.WithFromKey(), etcd.WithKeysOnly())
	if err != nil {
		return nil, err
	}

	connectionsWithLocks := map[connection.ID]bool{}
	for _, kv := range locks.Kvs {
		connectionID := connection.ID(strings.Split(string(kv.Key), "/")[0])
		connectionsWithLocks[connectionID] = true
	}

	list := []*Event{}
	for _, kv := range connections.Kvs {
		conn := &connection.Connection{}
		if err := json.Unmarshal(kv.Value, conn); err != nil {
			return nil, err
		}

		if _, exists := connectionsWithLocks[conn.ID]; !exists {
			list = append(list, &Event{
				Type:       Created,
				ID:         conn.ID,
				Connection: conn,
			})
		}
	}

	return list, nil
}
