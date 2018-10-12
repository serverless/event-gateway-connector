package kv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"go.uber.org/zap"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/segmentio/ksuid"
	"github.com/serverless/event-gateway-connector/connection"
)

const jobsDir = "jobs/"
const connectionsPrefix = "connections/"
const locksPrefix = "locks/jobs/"
const checkpointPrefix = "workers/"

//const workerDir = "workers"

// Store implements connection.Service using etcd KV as a backend.
type Store struct {
	client         etcd.KV
	jobsBucketSize uint
	log            *zap.SugaredLogger
}

// NewStore returns new Store instance.
func NewStore(client etcd.KV, jobsBucketSize uint, log *zap.SugaredLogger) *Store {
	return &Store{
		client:         client,
		jobsBucketSize: jobsBucketSize,
		log:            log,
	}
}

// RetrieveCheckpoint returns the existing checkpoint for a given workerID, or an error if not found
func (store Store) RetrieveCheckpoint(key string) (string, error) {
	checkpoint, err := store.client.Get(context.TODO(), key+"/")
	if checkpoint.Count == 0 {
		return "", ErrKeyNotFound
	}
	if err != nil {
		return "", err
	}

	return string(checkpoint.Kvs[0].Value), nil
}

// UpdateCheckpoint updates the current checkpoint information for a given workerID
func (store Store) UpdateCheckpoint(key, value string) error {
	_, err := store.client.Put(context.TODO(), key+"/", value)
	return err
}

// DeleteCheckpoint removes the checkpoint altogether from the store (usually after removing a connection)
func (store Store) DeleteCheckpoint(key string) error {
	_, err := store.client.Delete(context.TODO(), key+"/")
	return err
}

// CreateConnection creates connection in etcd.
func (store Store) CreateConnection(conn *connection.Connection) (*connection.Connection, error) {
	id, err := ksuid.NewRandom()
	if err != nil {
		return nil, err
	}
	conn.ID = connection.ID(id.String())

	value, err := json.Marshal(conn)
	if err != nil {
		return nil, err
	}

	createConnection := etcd.OpPut(string(conn.ID), string(value))
	createJobs, err := store.createJobsOps(conn)
	if err != nil {
		return nil, err
	}
	ops := append(createJobs, createConnection)
	_, err = store.client.Txn(context.TODO()).Then(ops...).Commit()
	if err != nil {
		return nil, err
	}

	store.log.Debugw("Connection created.", "space", conn.Space, "connectionId", conn.ID)

	return conn, nil
}

// UpdateConnection udpates connection in etcd.
func (store Store) UpdateConnection(conn *connection.Connection) (*connection.Connection, error) {
	connectionValue, err := json.Marshal(conn)
	if err != nil {
		return nil, err
	}

	existingPairs, err := store.client.Get(context.TODO(), string(conn.ID)+"/", etcd.WithPrefix())
	if existingPairs.Count == 0 {
		return nil, ErrKeyNotFound
	}
	if err != nil {
		return nil, err
	}

	ops := []etcd.Op{}
	// update existing connection
	ops = append(ops, etcd.OpPut(string(conn.ID), string(connectionValue)))
	// update existing jobs. We cannot just delete all jobs and create new ones because etcd transactions
	// doesn't allow deleting and created same key in one transaction
	createJobs, err := store.createJobsOps(conn)
	if err != nil {
		return nil, err
	}
	ops = append(ops, createJobs...)
	// delete remaining jobs from the store
	if len(createJobs) < len(existingPairs.Kvs) {
		for i := len(existingPairs.Kvs) - 1; i < len(existingPairs.Kvs); i++ {
			ops = append(ops, etcd.OpDelete(string(existingPairs.Kvs[i].Key)))
		}
	}

	_, err = store.client.
		Txn(context.TODO()).
		If(etcd.Compare(etcd.ModRevision(string(conn.ID)), "=", existingPairs.Kvs[0].ModRevision)).
		Then(ops...).
		Commit()
	if err != nil {
		return nil, err
	}

	store.log.Debugw("Connection updated.", "space", conn.Space, "connectionId", conn.ID)

	return conn, nil
}

// DeleteConnection deletes connection from etcd.
func (store Store) DeleteConnection(space string, id connection.ID) error {
	deleteConnection := etcd.OpDelete(string(id))
	deleteJobs := etcd.OpDelete(fmt.Sprintf("%s/%s", id, jobsDir), etcd.WithPrefix())
	deleteWorkers := etcd.OpDelete(fmt.Sprintf("%s%s", checkpointPrefix, id), etcd.WithPrefix())
	resp, err := store.client.Txn(context.TODO()).Then(deleteConnection, deleteJobs, deleteWorkers).Commit()
	if resp.Responses[0].GetResponseDeleteRange().Deleted == 0 {
		return ErrKeyNotFound
	}
	if err != nil {
		return err
	}

	store.log.Debugw("Connection deleted.", "space", space, "connectionId", string(id))

	return nil
}

func (store Store) createJobsOps(conn *connection.Connection) ([]etcd.Op, error) {
	ops := []etcd.Op{}

	numWorkersLeft := conn.Source.NumberOfWorkers()
	numBuckets := int(math.Ceil(float64(conn.Source.NumberOfWorkers()) / float64(store.jobsBucketSize)))
	for i := 0; i < numBuckets; i++ {
		job := &connection.Job{
			ID:              connection.NewJobID(conn.ID, uint(i)),
			Connection:      conn,
			BucketSize:      store.jobsBucketSize,
			NumberOfWorkers: uint(math.Min(float64(store.jobsBucketSize), float64(numWorkersLeft))),
		}
		numWorkersLeft -= job.NumberOfWorkers
		value, err := json.Marshal(job)
		if err != nil {
			return []etcd.Op{}, err
		}

		ops = append(ops, etcd.OpPut(fmt.Sprintf("%s/%s%s", conn.ID, jobsDir, job.ID), string(value)))
	}

	return ops, nil
}

// GetLocksPrefix returns the prefix for the locks entry in the kv
func GetLocksPrefix() string {
	return locksPrefix
}

// ErrKeyNotFound is thrown when the key is not found in the store during a Get operation
var ErrKeyNotFound = errors.New("Key not found in store")

var _ connection.Service = (*Store)(nil)
