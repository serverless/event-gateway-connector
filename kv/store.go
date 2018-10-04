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
	createJobs, err := store.createJobs(conn)
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
	_, err := json.Marshal(conn)
	if err != nil {
		return nil, err
	}

	getResp, err := store.client.Get(context.TODO(), string(conn.ID))
	if getResp.Count == 0 {
		return nil, ErrKeyNotFound
	}
	if err != nil {
		return nil, err
	}

	// TODO it doesn't work
	// ops := []etcd.Op{}
	// ops = append(ops, store.deleteJobs(conn.ID))
	// ops = append(ops, etcd.OpPut(store.ConnectionsPrefix+string(conn.ID), string(value)))
	// ops = append(ops, store.createJobs(conn)...)
	// _, err = store.Client.
	// 	Txn(context.TODO()).
	// 	If(etcd.Compare(etcd.ModRevision(store.ConnectionsPrefix+string(conn.ID)), "=", getResp.Kvs[0].ModRevision)).
	// 	Then(ops...).
	// 	Commit()
	// if err != nil {
	// 	return nil, err
	// }

	store.log.Debugw("Connection updated.", "space", conn.Space, "connectionId", conn.ID)

	return conn, nil
}

// DeleteConnection deletes connection from etcd.
func (store Store) DeleteConnection(space string, id connection.ID) error {
	deleteConnection := etcd.OpDelete(string(id))
	deleteJobs := store.deleteJobs(id)
	resp, err := store.client.Txn(context.TODO()).Then(deleteConnection, deleteJobs).Commit()
	if resp.Responses[0].GetResponseDeleteRange().Deleted == 0 {
		return ErrKeyNotFound
	}
	if err != nil {
		return err
	}

	store.log.Debugw("Connection deleted.", "space", space, "connectionId", string(id))

	return nil
}

func (store Store) createJobs(conn *connection.Connection) ([]etcd.Op, error) {
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

func (store Store) deleteJobs(id connection.ID) etcd.Op {
	return etcd.OpDelete(fmt.Sprintf("%s/%s", id, jobsDir), etcd.WithPrefix())
}

// ErrKeyNotFound is thrown when the key is not found in the store during a Get operation
var ErrKeyNotFound = errors.New("Key not found in store")

var _ connection.Service = (*Store)(nil)
