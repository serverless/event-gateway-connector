package workerpool

import (
	"context"
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
		done:       make(chan bool),
	}

	return pool, nil
}

// Stop sends the done signal to the WorkerPool to clean up all Connections
func (pool *WorkerPool) Stop() {
	pool.done <- true
}

// Start receives the number of worker goroutines from the main process
func (pool *WorkerPool) Start() error {
	// errors channel for workers to send back errors
	// this will be used by the master to address any fails that come from a worker
	errors := make(chan workerError)

	// close channel for workers to send back normal exit
	// simply dump the worker ID back on the channel
	close := make(chan uint)

	for {
		select {
		case <-pool.done:
			// block & wait for the done signal
			pool.log.Debugf("received the done signal")
			for _, job := range pool.jobs {
				for _, worker := range job.workers {
					worker.done <- true
				}

				if err := job.mutex.Unlock(context.TODO()); err != nil {
					pool.log.Errorf("unable to unlock", "error", err, "connectionID", job.connection.ID)
				}
				pool.log.Debugf("unlocked", "connectionID", job.connection.ID)
			}
			return nil
		case event := <-pool.events:
			if event.Type == watcher.Created {
				// acquire lock
				mutex := concurrency.NewMutex(pool.session, lockPrefix)
				pool.log.Debugw("locking...", "connectionID", event.ID)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
				err := mutex.Lock(ctx)
				cancel()
				if err != nil {
					pool.log.Debugw("unable to lock", "error", err, "connectionID", event.ID)
					continue
				}
				pool.log.Debugw("lock acquired", "ID", event.ID)

				// stark job
				count := event.Connection.Source.NumberOfWorkers()
				if _, ok := pool.jobs[event.Connection.ID]; !ok {
					pool.jobs[event.Connection.ID] = &job{
						connection: event.Connection,
						workers:    make(map[uint]*worker),
						mutex:      mutex,
					}
				}

				for id := uint(0); id < count; id++ {
					pool.assignWorker(id, event.Connection, errors, close)
					pool.numWorkers++
				}
			}
		case workerErr := <-errors:
			// worker thread errored
			// would need to figure out retry logic here
			pool.log.Warnf("received an error from worker %d, error: %s, total: %d", workerErr.id, workerErr.err.Error(), pool.numWorkers)
			pool.numWorkers--
			pool.removeWorker(workerErr)

			pool.log.Debugf("restarting worker %d from connection %s", workerErr.id, workerErr.connectionID)
			pool.assignWorker(workerErr.id, pool.jobs[workerErr.connectionID].connection, errors, close)
			pool.numWorkers++
		case workerID := <-close:
			// worker thread closed normally
			pool.log.Debugw("closing worker", "workerID", workerID)
		}
	}
}

const lockPrefix = "serverless-event-gateway-connector/__locks"

// job is the interim struct to manage the specific worker for a give connectionID
type job struct {
	workers    map[uint]*worker
	connection *connection.Connection
	mutex      *concurrency.Mutex
}

// removeWorker deducts the specified worker from the job, allowing it to be reassigned
func (pool *WorkerPool) removeWorker(w workerError) {
	delete(pool.jobs[w.connectionID].workers, w.id)
}

// assignWorker processes the new event and signals the requisite worker goroutines
func (pool *WorkerPool) assignWorker(id uint, conn *connection.Connection, errors chan<- workerError, close chan<- uint) {
	pool.jobs[conn.ID].workers[id] = newWorker(id, conn, errors, close, pool.log)
}

func newWorker(id uint, conn *connection.Connection, errors chan<- workerError, close chan<- uint, log *zap.SugaredLogger) *worker {
	w := &worker{
		id:         id,
		done:       make(chan bool),
		connection: conn,
		errors:     errors,
		close:      close,
		log:        log,
	}
	go w.run()
	return w
}

func (w *worker) run() {
	w.log.Debugw("kicked off worker", "workerID", w.id)
	for {
		select {
		case <-w.done:
			w.log.Debugw("trapped done signal", "workerID", w.id)
			return

		default:
			// perform the actual connection here
			for i := 0; i < 3; i++ {
				w.log.Debugf("would be handling the stuff here: %d, %+v", w.id, w.connection)
				time.Sleep(3 * time.Second)
			}

			// in case of error:
			// w.errors <- workerError{id: w.id, connectionID: c.ID, err: err}
			w.log.Debugw("worker finished job", "workerID", w.id, "connectionID", w.connection.ID)
			w.close <- w.id
			return
		}
	}
}

// worker is the internal representation of the worker process
type worker struct {
	id         uint
	connection *connection.Connection
	errors     chan<- workerError
	close      chan<- uint
	done       chan bool
	log        *zap.SugaredLogger
}

// workerError for cases where the worker ends up failing for a specific reason
type workerError struct {
	id           uint
	connectionID connection.ID
	err          error
}
