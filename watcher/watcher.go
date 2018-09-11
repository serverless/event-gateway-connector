package watcher

import (
	"context"
	"fmt"

	"github.com/coreos/etcd/clientv3/namespace"

	etcd "github.com/coreos/etcd/clientv3"
)

// Watch watches etcd directory and emits events when key/value pair was added, deleted or changed.
// Watch function also emits events for pre-existing key/value pairs.
func Watch(client *etcd.Client, prefix string, stopCh <-chan struct{}) (<-chan *Event, error) {
	watcherClient := namespace.NewWatcher(client.Watcher, prefix)
	kvClient := namespace.NewKV(client.KV, prefix)

	eventsCh := make(chan *Event)

	existingValues, err := list(kvClient)
	if err != nil {
		return nil, fmt.Errorf("listing existing values faield: %s", err)
	}

	go func() {
		defer watcherClient.Close()
		defer close(eventsCh)

		// populate channel with events about existing values
		for _, existingValue := range existingValues {
			eventsCh <- existingValue
		}

		watchCh := watcherClient.Watch(context.TODO(), "", etcd.WithPrefix())
		for resp := range watchCh {
			select {
			case <-stopCh:
				return
			default:
			}

			for _, watchEvent := range resp.Events {
				event := &Event{
					Type:  EventCreatedOrChanged,
					Key:   string(watchEvent.Kv.Key),
					Value: []byte(watchEvent.Kv.Value),
				}
				if watchEvent.Kv.Value == nil {
					event.Type = EventDeleted
				}

				eventsCh <- event
			}
		}
	}()

	return eventsCh, nil
}

// list retruns existing key/value pairs as events.
func list(client etcd.KV) ([]*Event, error) {
	resp, err := client.Get(context.TODO(), "\x00", etcd.WithFromKey())
	if err != nil {
		return nil, err
	}

	list := []*Event{}
	for _, kv := range resp.Kvs {
		list = append(list, &Event{
			Type:  EventCreatedOrChanged,
			Key:   string(kv.Key),
			Value: kv.Value,
		})
	}

	return list, nil
}

const (
	// EventCreatedOrChanged happens when key/value pair was added or changed.
	EventCreatedOrChanged int = iota
	// EventDeleted happens when key/value pair was deleted.
	EventDeleted
)

// Event represents event happened in etcd KV store
type Event struct {
	Type  int
	Key   string
	Value []byte
}
