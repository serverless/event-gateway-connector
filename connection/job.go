package connection

import (
	"fmt"
	"strconv"
	"strings"
)

type JobID string

// NewJobID create new JobID
func NewJobID(connID ID, workerID uint) JobID {
	return JobID(fmt.Sprintf("%s-%d", connID, workerID))
}

func (id JobID) ConnectionID() ID {
	segs := strings.Split(string(id), "-")
	return ID(segs[0])
}

func (id JobID) WorkerID() uint {
	segs := strings.Split(string(id), "-")
	workerID, _ := strconv.Atoi(segs[1])
	return uint(workerID)
}

type Job struct {
	ID              JobID       `json:"jobId"`
	Connection      *Connection `json:"connection"`
	BucketSize      uint        `json:"bucketSize"`
	NumberOfWorkers uint        `json:"numberOfWorkers"`
}
