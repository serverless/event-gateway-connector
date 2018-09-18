package workerpool

import (
	"context"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3/concurrency"

	"github.com/serverless/event-gateway-connector/connection"
	"github.com/serverless/event-gateway-connector/watcher"
	"go.uber.org/zap"
)

// WorkerPool is the default struct for our worker pool, containing mostly private values
// including the maximum workers eligible, current count of workers, etc.
type WorkerPool struct {
	maxWorkers uint
	numWorkers uint
	session    *concurrency.Session
	log        *zap.SugaredLogger
	jobs       map[connection.ID]*job // map of job handlers assigned to each connection.ID
	events     <-chan *watcher.Event
	done       chan bool // signal channel to stop all worker processes
}

// New will accept a few initializer variables in order to stand up the new worker
// pool of goroutines. These workers will listen for *watcher.Events and handle the
// internal *Connection to manage data.
func New(session *concurrency.Session, maxWorkers uint, events <-chan *watcher.Event, log *zap.SugaredLogger) (*WorkerPool, error) {
	pool := &WorkerPool{
		maxWorkers: maxWorkers,
		session:    session,
		log:        log,
		jobs:       make(map[connection.ID]*job),
		events:     events,
	}

	return pool, nil
}

// Start listens on events channel and tries to start a job and workers for every connection.
func (pool *WorkerPool) Start() error {
	for {
		event := <-pool.events
		if event.Type == watcher.Created {
			job, err := newJob(pool.session, event.Connection, pool.log.Named("job"))
			if err != nil {
				pool.log.Debugw("creating new job failed", "error", err, "connectionID", event.ID)
				continue
			}

			job.start()

			pool.jobs[event.ID] = job
			pool.numWorkers += event.Connection.Source.NumberOfWorkers()
		}
	}
}

// Stop is a blocking function waiting for all jobs (and workers) to stop
func (pool *WorkerPool) Stop() {
	pool.log.Debugf("stopping all jobs")

	for _, job := range pool.jobs {
		job.stop()
	}
}

const lockPrefix = "serverless-event-gateway-connector/locks/connections/"

// job is the interim struct to manage the specific worker for a give connectionID
type job struct {
	connection *connection.Connection
	mutex      *concurrency.Mutex
	workers    map[uint]*worker
	done       chan bool // signal channel to stop all worker processes
	waitGroup  *sync.WaitGroup
	log        *zap.SugaredLogger
}

// newJob creates new job an tries to lock the connection in etcd
func newJob(session *concurrency.Session, conn *connection.Connection, log *zap.SugaredLogger) (*job, error) {
	mutex := concurrency.NewMutex(session, lockPrefix+string(conn.ID))

	log.Debugw("locking connection...", "connectionID", conn.ID)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	if err := mutex.Lock(ctx); err != nil {
		return nil, err
	}
	log.Debugw("connection lock acquired", "connectionID", conn.ID)

	return &job{
		connection: conn,
		mutex:      mutex,
		workers:    make(map[uint]*worker),
		done:       make(chan bool),
		waitGroup:  &sync.WaitGroup{},
		log:        log,
	}, nil
}

func (j *job) start() {
	for id := uint(0); id < j.connection.Source.NumberOfWorkers(); id++ {
		j.workers[id] = newWorker(id, j.connection, j.done, j.waitGroup, j.log.Named("worker"))
		go j.workers[id].run()
		j.waitGroup.Add(1)
	}
}

func (j *job) stop() {
	j.log.Debugw("stopping a job", "connectionID", j.connection.ID)

	j.done <- true
	j.waitGroup.Wait()

	if err := j.mutex.Unlock(context.TODO()); err != nil {
		j.log.Errorw("unable to unlock connection", "error", err, "connectionID", j.connection.ID)
	}
	j.log.Debugw("connection unlocked", "connectionID", j.connection.ID)
}

// worker is the internal representation of the worker process
type worker struct {
	id         uint
	connection *connection.Connection
	done       <-chan bool
	waitGroup  *sync.WaitGroup
	log        *zap.SugaredLogger
}

func newWorker(id uint, conn *connection.Connection, done <-chan bool, wg *sync.WaitGroup, log *zap.SugaredLogger) *worker {
	w := &worker{
		id:         id,
		connection: conn,
		done:       done,
		waitGroup:  wg,
		log:        log,
	}
	return w
}

func (w *worker) run() {
	w.log.Debugw("kicked off worker", "workerID", w.id)
	for {
		select {
		case <-w.done:
			w.log.Debugw("trapped done signal", "workerID", w.id)
			w.waitGroup.Done()
			return
		default:
			// perform the actual connection here
			w.log.Debugw("would be handling the stuff here", "workerID", w.id, "connectionID", w.connection.ID)
			time.Sleep(3 * time.Second)
		}
	}
}
