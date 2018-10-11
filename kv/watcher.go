package kv

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/coreos/etcd/mvcc/mvccpb"

	"github.com/serverless/event-gateway-connector/connection"
	"go.uber.org/zap"

	etcd "github.com/coreos/etcd/clientv3"
)

// Watcher watches etcd directory and emits events when Job configuration was added, changed or deleted.
type Watcher struct {
	connectionsKVClient etcd.KV
	jobsWatchClient     etcd.Watcher
	locksKVClient       etcd.KV
	stopCh              chan struct{}
	log                 *zap.SugaredLogger
}

// NewWatcher creates a new Watcher instance.
func NewWatcher(connectionsKVClient etcd.KV, jobsWatcher etcd.Watcher, locksKVClient etcd.KV, log *zap.SugaredLogger) *Watcher {
	return &Watcher{
		connectionsKVClient: connectionsKVClient,
		jobsWatchClient:     jobsWatcher,
		locksKVClient:       locksKVClient,
		stopCh:              make(chan struct{}),
		log:                 log,
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

	// listen for new events
	go func() {
		defer w.jobsWatchClient.Close()
		defer close(eventsCh)

		watchCh := w.jobsWatchClient.Watch(context.TODO(), "", etcd.WithPrefix())
		for resp := range watchCh {
			select {
			case <-w.stopCh:
				return
			default:
			}

			for _, watchEvent := range resp.Events {
				if !strings.Contains(string(watchEvent.Kv.Key), jobsDir) {
					continue
				}

				jobID := connection.JobID(strings.Split(string(watchEvent.Kv.Key), "/")[2])

				switch watchEvent.Type {
				case mvccpb.PUT:
					if watchEvent.Kv.CreateRevision != watchEvent.Kv.ModRevision {
						// connection was updated. Emit Delete event first and then Created event.
						// TODO handle update
						eventsCh <- &Event{Type: Deleted, JobID: jobID}
					}

					job := &connection.Job{}
					if err := json.Unmarshal(watchEvent.Kv.Value, job); err != nil {
						w.log.Errorw("unmarshaling payload failed", "err", err)
						continue
					}

					eventsCh <- &Event{Type: Created, JobID: jobID, Job: job}
				case mvccpb.DELETE:
					eventsCh <- &Event{Type: Deleted, JobID: jobID}
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
	Type  int
	JobID connection.JobID
	Job   *connection.Job
}

// list retruns existing key/value pairs as events.
func (w *Watcher) list() ([]*Event, error) {
	connectionsAndJobs, err := w.connectionsKVClient.Get(context.TODO(), "\x00", etcd.WithFromKey())
	if err != nil {
		return nil, err
	}

	locks, err := w.locksKVClient.Get(context.TODO(), "\x00", etcd.WithFromKey(), etcd.WithKeysOnly())
	if err != nil {
		return nil, err
	}

	jobsWithLocks := map[connection.JobID]bool{}
	for _, kv := range locks.Kvs {
		segs := strings.Split(string(kv.Key), "/") // extract job ID from lock key (format <job ID>/<lock ID>)
		jobID := connection.JobID(segs[0])
		jobsWithLocks[jobID] = true
	}

	// filter out connection.Connection values
	jobs := []*mvccpb.KeyValue{}
	for _, pair := range connectionsAndJobs.Kvs {
		if strings.Contains(string(pair.Key), jobsDir) {
			jobs = append(jobs, pair)
		}
	}

	list := []*Event{}
	for _, kv := range jobs {
		job := &connection.Job{}
		if err := json.Unmarshal(kv.Value, job); err != nil {
			return nil, err
		}

		if _, exists := jobsWithLocks[job.ID]; !exists {
			list = append(list, &Event{
				Type:  Created,
				JobID: job.ID,
				Job:   job,
			})
		}
	}

	return list, nil
}
