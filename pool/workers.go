package pool

import (
	"github.com/serverless/event-gateway-connector/connection"
	"go.uber.org/zap"
)

type workerMap map[int]*worker

// worker is the internal representation of the worker process
type worker struct {
	id    int
	recv  chan *connection.Connection
	conn  *connection.Connection
	log   *zap.SugaredLogger
	done  chan bool
	inUse bool
}

// StartWorkers receives the number of worker goroutines from the main process
func StartWorkers(numWorkers int, conns chan *connection.Connection, done <-chan bool) error {
	// initialize the logger for the pool
	rawLogger, _ := zap.NewDevelopment()
	defer rawLogger.Sync()
	log := rawLogger.Sugar()

	m := make(workerMap)

	// fork off the goroutines, at this point each goroutine is unconfigured
	for i := 0; i < numWorkers; i++ {
		m[i] = newWorker(i, log)
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
			if !checkConnection(c) {
				log.Errorf("malformed conn payload: %s", c.ID)
				continue
			}
			m[tasks].recv <- c
			tasks++
		}
	}
}

func checkConnection(conn *connection.Connection) bool {
	if conn.Type == "kafka" && conn.KafkaSource == nil {
		return false
	}

	if conn.Type == "awskinesis" && conn.AWSKinesisSource == nil {
		return false
	}

	return true
}

func newWorker(id int, log *zap.SugaredLogger) *worker {
	w := &worker{
		id:   id,
		done: make(chan bool),
		recv: make(chan *connection.Connection),
		log:  log,
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
				w.log.Errorf("handle connection: %s", err.Error())
			}
			w.log.Infof("worker %d finished job: %s", w.id, w.conn.ID)
			w.inUse = false
		}
	}
}

// handleConnection will actually spin up and handle the connection
func (w *worker) handleConnection() error {
	// perform the actual connection here
	w.log.Infof("would be handling the stuff here: %d, %s", w.id, w.conn.Space)

	return nil
}
