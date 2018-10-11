package workerpool

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3/concurrency"

	"github.com/serverless/event-gateway-connector/connection"
	"github.com/serverless/event-gateway-connector/kv"
	"github.com/serverless/event-gateway/event"
	"go.uber.org/zap"
)

// WorkerPool is a pool of workers grouped into jobs. Each Connection is split into one or more jobs
// (configured by connection.Job struct). Each job runs one or more workers. Each worker processes
// one shard/partition from the connection. Worker pool run jobs based on kv.Event channel.
type WorkerPool struct {
	maxWorkers  uint
	numWorkers  uint
	locksPrefix string
	session     *concurrency.Session
	events      <-chan *kv.Event
	jobs        map[connection.JobID]*job // map of job handlers assigned to each connection.ID
	jobsMutex   sync.RWMutex
	log         *zap.SugaredLogger
}

// Config is a configuration of the worker pool.
type Config struct {
	MaxWorkers  uint
	LocksPrefix string
	Session     *concurrency.Session
	Events      <-chan *kv.Event
	Log         *zap.SugaredLogger
}

// New creates and configures new worker pool. Worker pool has to be explicitly started with Start() method.
func New(config *Config) *WorkerPool {
	return &WorkerPool{
		maxWorkers:  config.MaxWorkers,
		locksPrefix: config.LocksPrefix,
		session:     config.Session,
		events:      config.Events,
		jobs:        make(map[connection.JobID]*job),
		log:         config.Log,
	}
}

// Start listens on kv.Event channel, tries to create a lock, and start a job.
func (pool *WorkerPool) Start() {
	go func() {
		for {
			event, more := <-pool.events
			if more {
				pool.log.Debugw("event received", "jobID", event.JobID, "type", event.Type)

				switch event.Type {
				case kv.Created:
					if pool.numWorkers+event.Job.NumberOfWorkers > pool.maxWorkers {
						pool.log.Debugw("creating new job skipped, workers limit exceeded", "jobID", event.JobID)
						continue
					}

					// In some edge case Watcher will emit the same Created event for the same job twice.
					// It prevents from creating the same job again.
					if _, exists := pool.jobs[event.JobID]; exists {
						continue
					}

					job, err := newJob(pool.session, event.Job, pool.locksPrefix, pool.log.Named("job"))
					if err != nil {
						pool.log.Debugw("creating new job failed", "error", err, "jobID", event.JobID)
						continue
					}

					job.start()

					pool.jobsMutex.Lock()
					pool.jobs[event.JobID] = job
					pool.jobsMutex.Unlock()
					pool.numWorkers += event.Job.NumberOfWorkers
				case kv.Deleted:
					if job, exists := pool.jobs[event.JobID]; exists {
						job.stop()
						delete(pool.jobs, event.JobID)
						pool.numWorkers -= job.numWorkers
					}
				}
			}
		}
	}()
}

// Stop the worker pool. It's a blocking function waiting for all jobs (and workers) to gracefully shutdown.
func (pool *WorkerPool) Stop() {
	pool.jobsMutex.RLock()
	for _, job := range pool.jobs {
		job.stop()
	}
	pool.jobsMutex.RUnlock()

	pool.log.Debugf("all jobs stopped")
}

// job is a group of worker handling part of connection's shards.
type job struct {
	id         connection.JobID
	bucketSize uint
	numWorkers uint
	connection *connection.Connection
	mutex      *concurrency.Mutex
	workers    map[uint]*worker
	waitGroup  *sync.WaitGroup
	log        *zap.SugaredLogger
}

// newJob creates a lock in etcd and returns created job.
func newJob(session *concurrency.Session, config *connection.Job, locksPrefix string, log *zap.SugaredLogger) (*job, error) {
	mutex := concurrency.NewMutex(session, locksPrefix+string(config.ID))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	if err := mutex.Lock(ctx); err != nil {
		return nil, err
	}
	log.Debugw("lock acquired", "jobID", config.ID)

	return &job{
		id:         config.ID,
		bucketSize: config.BucketSize,
		numWorkers: config.NumberOfWorkers,
		connection: config.Connection,
		mutex:      mutex,
		workers:    make(map[uint]*worker),
		waitGroup:  &sync.WaitGroup{},
		log:        log,
	}, nil
}

func (j *job) start() {
	for i := uint(0); i < j.numWorkers; i++ {
		id := uint(j.id.JobNumber()*j.bucketSize) + i
		worker := newWorker(id, *j.connection, j.waitGroup, j.log.Named("worker"))
		go worker.run()

		j.workers[id] = worker
		j.waitGroup.Add(1)
	}
}

func (j *job) stop() {
	for _, worker := range j.workers {
		worker.done <- true
	}
	j.waitGroup.Wait()

	if err := j.mutex.Unlock(context.TODO()); err != nil {
		j.log.Errorw("unable to unlock connection", "error", err, "jobID", j.id)
	}
	j.log.Debugw("lock released", "jobID", j.id)
}

// worker is the internal representation of the worker process
type worker struct {
	id           uint
	connection   connection.Connection
	eventGateway *http.Client
	done         chan bool
	waitGroup    *sync.WaitGroup
	log          *zap.SugaredLogger
}

func newWorker(id uint, conn connection.Connection, wg *sync.WaitGroup, log *zap.SugaredLogger) *worker {
	w := &worker{
		id:         id,
		connection: conn,
		done:       make(chan bool),
		waitGroup:  wg,
		log:        log,
		eventGateway: &http.Client{
			Timeout: 2 * time.Second,
		},
	}
	return w
}

func (w *worker) run() {
	w.log.Debugw("kicked off worker", "workerID", w.id)
	defer w.waitGroup.Done()
	defer close(w.done)

	var data = &connection.Records{}
	var err error

	for {
		select {
		case <-w.done:
			w.log.Debugw("trapped done signal", "workerID", w.id)
			if err := w.connection.Source.Close(); err != nil {
				w.log.Errorw("closing source failed", "workerID", w.id, "error", err.Error())
			}
			return
		case <-time.After(1 * time.Second):
			// This case is needed to unblock Fetch call below. Otherwise, it can block indefinitely
			// if the source is using blocking call to the source message queue.
		default:
			data, err = w.connection.Source.Fetch(w.id, data.LastSequence)
			if err != nil {
				w.log.Errorw("worker failed", "workerID", w.id, "error", err.Error())
				return
			}

			err = w.sendToEventGateway(data)
			if err != nil {
				w.log.Errorw("sending worker data to Event Gateway",
					"workerID", w.id,
					"error", err.Error(),
					"space", w.connection.Space,
					"target", w.connection.Target,
				)
			}
		}
	}
}

// sendToEventGateway takes the provided set of data payload events from a given source
// and sends them to the specified space at the Event Gateway
func (w *worker) sendToEventGateway(data *connection.Records) error {
	for _, payload := range data.Data {
		now := time.Now()
		cloudEvent := &event.Event{
			EventType:          event.TypeName(w.connection.EventType),
			CloudEventsVersion: "0.1",
			Source:             "/eventgateway/connector",
			EventID:            "unique",
			EventTime:          &now,
			ContentType:        "text/plain",
			Data:               string(payload),
		}

		buffer := &bytes.Buffer{}
		err := json.NewEncoder(buffer).Encode(cloudEvent)
		if err != nil {
			return err
		}

		resp, err := w.eventGateway.Post(w.connection.Target, "application/cloudevents+json", buffer)
		if err != nil {
			w.log.Errorw("sending message failed", "error", err, "workerID", w.id, "connectionID", w.connection.ID)
			continue
		}
		if resp.StatusCode >= 400 {
			body, _ := ioutil.ReadAll(resp.Body)
			w.log.Errorw("event rejected by Event Gateway", "workerID", w.id, "connectionID", w.connection.ID, "statusCode", resp.StatusCode, "body", string(body))
			continue
		}

		w.log.Debugw("message sent to Event Gateway", "workerID", w.id, "connectionID", w.connection.ID)

		messagesProcessed.WithLabelValues(w.connection.Space, string(w.connection.ID)).Inc()
		bytesProcessed.WithLabelValues(w.connection.Space, string(w.connection.ID)).Add(float64(len(payload)))
	}

	return nil
}
