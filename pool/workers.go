package pool

import (
	"time"

	"github.com/serverless/event-gateway-connector/connection"
	"go.uber.org/zap"
)

// worker is the internal representation of the worker process
type worker struct {
	log *zap.SugaredLogger
}

// StartWorkers receives the number of worker goroutines from the main process
func StartWorkers(conns chan *connection.Connection, numWorkers int) error {
	// initialize the logger for the pool
	rawLogger, _ := zap.NewDevelopment()
	defer rawLogger.Sync()
	logger := rawLogger.Sugar()

	done := make(chan bool, 1)
	w := &worker{logger}

	// fork off the goroutines
	for i := 0; i < numWorkers; i++ {
		go w.start(i, conns, done)
	}

	time.Sleep(1 * time.Second)

	return nil
}

func (w *worker) start(id int, conns <-chan *connection.Connection, done <-chan bool) {
	select {
	case <-done:
		w.log.Debugf("trapped done signal for worker %d...", id)
		return
	case c := <-conns:
		w.log.Infof("worker %d started job:  %s", id, c.ID)
		time.Sleep(time.Second)
		w.log.Infof("worker %d finished job: %s", id, c.ID)
	default:
		w.log.Debugf("%02d. worker still waiting...", id)
	}
}
