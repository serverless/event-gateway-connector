package workers

import (
	"time"

	"github.com/serverless/event-gateway-connector/connection"
	"github.com/serverless/event-gateway-connector/utils"
	"github.com/serverless/event-gateway-connector/watcher"
	"go.uber.org/zap"
)

// worker is the internal representation of the worker process
type worker struct {
	id     uint
	recv   chan *connection.Connection
	errors chan<- workerError
	close  chan<- uint
	log    *zap.SugaredLogger
	done   chan bool
}

// WorkerPool is the default struct for our worker pool, containing mostly private values
// including the maximum workers eligible, current count of workers, etc.
type WorkerPool struct {
	maxWorkers uint
	numWorkers uint
	log        *zap.SugaredLogger
	jobs       map[connection.ID]*job // map of job handlers assigned to each connection.ID
	events     <-chan *watcher.Event
	done       chan bool // signal channel to stop all worker processes
	stack      *utils.Stack
}

// job is the interim struct to manage the specific worker for a give connectionID
type job struct {
	workers map[uint]*worker
	conn    *connection.Connection
}

// define the map of workers to manage
var workerMap = make(map[uint]*worker)

// workerError for cases where the worker ends up failing for a specific reason
type workerError struct {
	id     uint
	connID connection.ID
	err    error
}

// NewPool will accept a few initializer variables in order to stand up the new worker
// pool of goroutines. These workers will listen for *watcher.Events and handle the
// internal *Connection to manage data.
func NewPool(log *zap.SugaredLogger, maxWorkers uint, events <-chan *watcher.Event) (*WorkerPool, error) {
	w := &WorkerPool{
		maxWorkers: maxWorkers,
		log:        log,
		jobs:       make(map[connection.ID]*job),
		events:     events,
		done:       make(chan bool),
	}

	// This stack is a helper to know which worker threads are still available to allocate.
	// Though this is moot in the initial kickoff of workers, this stack becomes helpful
	// if/when workers either die or are cancelled. Instead of combing the map to find
	// empty spaces, we can just pop a value off the top and use that
	w.stack = utils.NewStack()
	return w, nil
}

// NumWorkers returns the maximum number of workers eligible for configuration in the WorkerPool (set at initialization time)
func (wp *WorkerPool) NumWorkers() uint {
	return wp.numWorkers
}

// Close sends the done signal to the WorkerPool to clean up all Connections
func (wp *WorkerPool) Close() {
	wp.close()
}

func (wp *WorkerPool) close() {
	wp.done <- true
}

// StartWorkers receives the number of worker goroutines from the main process
func (wp *WorkerPool) StartWorkers() error {

	// errors channel for workers to send back errors
	// this will be used by the master to address any fails that come from a worker
	errors := make(chan workerError)

	// close channel for workers to send back normal exit
	// simply dump the worker ID back on the channel
	close := make(chan uint)

	// fork off the goroutines, at this point each goroutine is unconfigured
	for i := uint(0); i < wp.maxWorkers; i++ {
		workerMap[i] = newWorker(i, wp.log, errors, close)
		wp.stack.Push(i)
	}

	for {
		select {
		case <-wp.done:
			// block & wait for the done signal
			wp.log.Debugf("received the done signal!")
			for x := range workerMap {
				workerMap[x].done <- true
			}
			return nil
		case e := <-wp.events:
			count := e.Connection.Source.NumWorkers()
			if _, ok := wp.jobs[e.Connection.ID]; !ok {
				wp.jobs[e.Connection.ID] = &job{
					conn:    e.Connection,
					workers: make(map[uint]*worker),
				}
			}

			for a := uint(0); a < count; a++ {
				wp.assignWorker(e.Connection)
			}
		case w := <-errors:
			// worker thread errored
			// would need to figure out retry logic here
			wp.log.Warnf("received an error from worker %d, error: %s, total: %d", w.id, w.err.Error(), wp.stack.Length())
			wp.removeWorker(w)
			wp.assignWorker(wp.jobs[w.connID].conn)
		case c := <-close:
			// worker thread closed normally
			wp.log.Debugf("closing worker %d", c)
			wp.stack.Push(c)
			delete(workerMap, c)
		}
	}
}

// removeWorker deducts the specified worker from both the job and workerMap, allowing it to be reassigned
func (wp *WorkerPool) removeWorker(w workerError) {
	wp.stack.Push(w.id)
	delete(workerMap, w.id)
	delete(wp.jobs[w.connID].workers, w.id)
}

// handleEvent processes the new event and signals the requisite worker goroutines
func (wp *WorkerPool) assignWorker(c *connection.Connection) {
	// receive a connection from the API
	n, ok := wp.stack.Pop()
	if !ok {
		wp.log.Errorf("too many worker threads already assigned (%d of %d)", len(workerMap), wp.numWorkers)
	}

	workerMap[n].recv <- c
	wp.jobs[c.ID].workers[n] = workerMap[n]
}

func newWorker(id uint, log *zap.SugaredLogger, errors chan<- workerError, close chan<- uint) *worker {
	w := &worker{
		id:     id,
		done:   make(chan bool),
		recv:   make(chan *connection.Connection),
		errors: errors,
		close:  close,
		log:    log,
	}
	go w.run()
	return w
}

func (w *worker) run() {
	w.log.Debugf("kicked off worker #%02d...", w.id)
	for {
		select {
		case <-w.done:
			w.log.Debugf("trapped done signal for worker %d...", w.id)
			return
		case c := <-w.recv:
			w.log.Debugf("worker %d started job:  %s", w.id, c.ID)
			if err := w.handleConnection(c); err != nil {
				w.errors <- workerError{id: w.id, connID: c.ID, err: err}
				return
			}

			w.log.Debugf("worker %d finished job: %s", w.id, c.ID)
			w.close <- w.id
			return
		}
	}
}

// handleConnection will actually spin up and handle the connection
func (w *worker) handleConnection(c *connection.Connection) error {
	// perform the actual connection here
	for i := 0; i < 3; i++ {
		w.log.Debugf("would be handling the stuff here: %d, %+v", w.id, c)
		time.Sleep(3 * time.Second)
	}

	return nil
}
