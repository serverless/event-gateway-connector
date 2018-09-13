package pool

import (
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

	// define the map of workers to manage
	m := make(workerMap)

	// errors channel for workers to send back errors
	// this will be used by the master to address any fails that come from a worker
	errors := make(chan workerError)

	// close channel for workers to send back normal exit
	// simply dump the worker ID back on the channel
	close := make(chan int)

	s := NewStack()

	for a := 0; a < numWorkers; a++ {
		s.Push(a)
	}

	// fork off the goroutines, at this point each goroutine is unconfigured
	for i := 0; i < numWorkers; i++ {
		m[i] = newWorker(i, log, errors, close)
	}

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
			// receive a connection from the API
			n, ok := s.Pop()
			if !ok {
				log.Errorf("too many worker threads already assigned (%d)", len(m))
				continue
			}
			m[n].recv <- c
		case w := <-errors:
			// worker thread errored
			// would need to figure out retry logic here
			log.Warnf("received an error from worker %d, error: %s, total: %d", w.id, w.err.Error(), s.Length())
			s.Push(w.id)
			delete(m, w.id)
		case c := <-close:
			// worker thread closed normally
			log.Infof("closing worker %d", c)
			s.Push(c)
			delete(m, c)
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
			return
		case c := <-w.recv:
			w.conn = c

			w.log.Infof("worker %d started job:  %s", w.id, w.conn.ID)
			if err := w.handleConnection(); err != nil {
				w.errors <- workerError{id: w.id, err: err}
				return
			}

			w.log.Infof("worker %d finished job: %s", w.id, w.conn.ID)
			w.close <- w.id
			return
		}
	}
}

// handleConnection will actually spin up and handle the connection
func (w *worker) handleConnection() error {
	// perform the actual connection here
	for i := 0; i < 3; i++ {
		w.log.Infof("would be handling the stuff here: %d, %+v", w.id, w.conn)
		time.Sleep(3 * time.Second)
	}

	return nil
}
