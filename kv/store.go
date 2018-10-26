package kv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"

	"go.uber.org/zap"

	etcd "github.com/coreos/etcd/clientv3"
	namespace "github.com/coreos/etcd/clientv3/namespace"
	"github.com/segmentio/ksuid"
	"github.com/serverless/event-gateway-connector/connection"
)

const (
	// Prefix is the default key-value store prefix for all connectors, jobs, workers, etc
	Prefix = "serverless-event-gateway-connector/"

	// ConnectionsPrefix is the path for the connections underneath PREFIX
	ConnectionsPrefix = "connections/"

	// LocksPrefix is the path for the separate key-value store for locks only
	LocksPrefix = "locks/jobs/"

	// CheckpointPrefix is the path for the separate key-value store for workers only
	CheckpointPrefix = "checkpoints/"

	jobsDir = "jobs/"
)

// Store implements connection.Service using etcd KV as a backend.
type Store struct {
	client         etcd.KV
	jobsBucketSize uint
	log            *zap.SugaredLogger
}

// NewStore returns new Store instance.
func NewStore(client *etcd.Client, jobsBucketSize uint, log *zap.SugaredLogger) *Store {
	return &Store{
		client:         namespace.NewKV(client, Prefix),
		jobsBucketSize: jobsBucketSize,
		log:            log,
	}
}

// RetrieveCheckpoint returns the existing checkpoint for a given workerID, or an error if not found
func (store Store) RetrieveCheckpoint(key string) (string, error) {
	checkpoint, err := store.client.Get(context.TODO(), fmt.Sprintf("%s%s/", CheckpointPrefix, key))
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
	_, err := store.client.Put(context.TODO(), fmt.Sprintf("%s%s/", CheckpointPrefix, key), value)
	return err
}

// ListConnections returns list of connections
func (store Store) ListConnections(space string) ([]*connection.Connection, error) {
	conns := []*connection.Connection{}

	kvs, err := store.client.Get(context.TODO(), ConnectionsPrefix, etcd.WithPrefix())
	if err != nil && err != ErrKeyNotFound {
		return nil, err
	}

	for _, kv := range kvs.Kvs {
		// ignore jobs KVs
		if strings.Contains(string(kv.Key), jobsDir) {
			continue
		}

		conn := &connection.Connection{}
		if err = json.Unmarshal(kv.Value, conn); err != nil {
			return nil, err
		}

		// filter out connections from a different space
		if conn.Space == space {
			conns = append(conns, conn)
		}
	}

	return conns, nil
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

	createConnection := etcd.OpPut(fmt.Sprintf("%s%s", ConnectionsPrefix, string(conn.ID)), string(value))
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
	connectionKey := ConnectionsPrefix + string(conn.ID)

	connectionValue, err := json.Marshal(conn)
	if err != nil {
		return nil, err
	}

	existingJobs, err := store.client.Get(context.TODO(), connectionKey+"/", etcd.WithPrefix())
	if existingJobs.Count == 0 {
		return nil, ErrKeyNotFound
	}
	if err != nil {
		return nil, err
	}

	ops := []etcd.Op{}
	// update existing jobs. We cannot just delete all jobs and create new ones because etcd transactions
	// doesn't allow deleting and created same key in one transaction
	createJobs, err := store.createJobsOps(conn)
	if err != nil {
		return nil, err
	}
	ops = append(ops, createJobs...)
	// delete remaining jobs from the store
	if len(createJobs) < len(existingJobs.Kvs) {
		for i := len(existingJobs.Kvs) - 1; i < len(existingJobs.Kvs); i++ {
			ops = append(ops, etcd.OpDelete(string(existingJobs.Kvs[i].Key)))
		}
	}

	// update existing connection
	ops = append(ops, etcd.OpPut(connectionKey, string(connectionValue)))

	_, err = store.client.
		Txn(context.TODO()).
		If(etcd.Compare(etcd.ModRevision(connectionKey), "=", existingJobs.Kvs[0].ModRevision)).
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
	deleteConnection := etcd.OpDelete(fmt.Sprintf("%s%s", ConnectionsPrefix, string(id)))
	deleteJobs := etcd.OpDelete(fmt.Sprintf("%s%s/%s", ConnectionsPrefix, id, jobsDir), etcd.WithPrefix())
	deleteWorkers := etcd.OpDelete(fmt.Sprintf("%s%s", CheckpointPrefix, id), etcd.WithPrefix())
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

		ops = append(ops, etcd.OpPut(fmt.Sprintf("%s%s/%s%s", ConnectionsPrefix, conn.ID, jobsDir, job.ID), string(value)))
	}

	return ops, nil
}

// ErrKeyNotFound is thrown when the key is not found in the store during a Get operation
var ErrKeyNotFound = errors.New("Key not found in store")

var _ connection.Service = (*Store)(nil)
