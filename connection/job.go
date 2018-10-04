package connection

import (
	"fmt"
	"strconv"
	"strings"
)

// JobID identifies job in the connection. It contains parent connection ID and a job number.
type JobID string

// NewJobID create new Job ID.
func NewJobID(connID ID, workerID uint) JobID {
	return JobID(fmt.Sprintf("%s-%d", connID, workerID))
}

// ConnectionID returns parent Connection ID.
func (id JobID) ConnectionID() ID {
	segs := strings.Split(string(id), "-")
	return ID(segs[0])
}

// JobNumber indicates an offset in a slice of shards returned by a source. Job Number
// indicates which part for the slice the job will handle. The number of shards that
// a single job handles is defined by bucket size.
func (id JobID) JobNumber() uint {
	segs := strings.Split(string(id), "-")
	workerID, _ := strconv.Atoi(segs[1])
	return uint(workerID)
}

// Job represents set of workers that process part of the connection shards.
type Job struct {
	ID              JobID       `json:"jobId"`
	Connection      *Connection `json:"connection"`
	BucketSize      uint        `json:"bucketSize"`
	NumberOfWorkers uint        `json:"numberOfWorkers"`
	// TODO we should also store Shard IDs handled by this Job. To have single source of truth about connection (shards) -> jobs mapping.
}
