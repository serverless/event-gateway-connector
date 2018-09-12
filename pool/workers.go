package pool

import (
	"fmt"
	"time"

	"github.com/serverless/event-gateway-connector/connection"
	"go.uber.org/zap"
)

type workerMap map[int]*worker

// worker is the internal representation of the worker process
type worker struct {
	id     int
	recv   chan *connection.Connection
	errors chan workerError
	close  chan int
	conn   *connection.Connection
	log    *zap.SugaredLogger
	done   chan bool
	inUse  bool
}

// workerError for cases where the worker ends up failing for a specific reason
type workerError struct {
	id  int
	err error
}

// StartWorkers receives the number of worker goroutines from the main process
func StartWorkers(numWorkers int, conns chan *connection.Connection, done <-chan bool) error {
	// initialize the logger for the pool
	rawLogger, _ := zap.NewDevelopment()
	defer rawLogger.Sync()
	log := rawLogger.Sugar()

	m := make(workerMap)

	// errors channel for workers to send back errors
	// this will be used by the master to address any fails that come from a worker
	errors := make(chan workerError)

	// close channel for workers to send back normal exit
	// simply dump the worker ID back on the channel
	close := make(chan int)

	// fork off the goroutines, at this point each goroutine is unconfigured
	for i := 0; i < numWorkers; i++ {
		m[i] = newWorker(i, log, errors, close)
	}

	var tasks int
	for {
		select {
		case <-done:
			// block & wait for the done signal
			log.Debugf("received the done signal!")
			for x := range m {
				m[x].done <- true
			}
			return nil
		case c := <-conns:
			m[tasks].recv <- c
			tasks++
		case w := <-errors:
			log.Warnf("received an error from worker %d, error: %s", w.id, w.err.Error())
			// would need to figure out retry logic here
		case c := <-close:
			log.Infof("closing worker %d", c)
		}
	}
}

func newWorker(id int, log *zap.SugaredLogger, errors chan workerError, close chan int) *worker {
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
			// defer w.conn.Close()
			return
		case w.conn = <-w.recv:
			w.inUse = true
			w.log.Infof("worker %d started job:  %s", w.id, w.conn.ID)
			if err := w.handleConnection(); err != nil {
				w.errors <- workerError{id: w.id, err: err}
				continue
			}
			w.log.Infof("worker %d finished job: %s", w.id, w.conn.ID)
			w.inUse = false
		}
	}
}

// handleConnection will actually spin up and handle the connection
func (w *worker) handleConnection() error {
	if w.id == 1 {
		return fmt.Errorf("testing an error failure in worker 1")
	}

	// perform the actual connection here
	for i := 0; i < 3; i++ {
		w.log.Infof("would be handling the stuff here: %d, %s, %s", w.id, w.conn.Space, w.conn.SourceConfig)
		time.Sleep(3 * time.Second)
	}

	return nil
}
