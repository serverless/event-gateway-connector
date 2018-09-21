package kv

import (
	"context"
	"encoding/json"
	"errors"

	"go.uber.org/zap"

	"github.com/coreos/etcd/clientv3"
	"github.com/segmentio/ksuid"
	"github.com/serverless/event-gateway-connector/connection"
)

// Store implements connection.Service using etcd KV as a backend.
type Store struct {
	Client clientv3.KV
	Log    *zap.SugaredLogger
}

var _ connection.Service = (*Store)(nil)

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

	_, err = store.Client.Put(context.TODO(), string(conn.ID), string(value))
	if err != nil {
		return nil, err
	}

	store.Log.Debugw("Connection created.", "space", conn.Space, "connectionId", conn.ID)

	return conn, nil
}

// UpdateConnection udpates connection in etcd.
func (store Store) UpdateConnection(conn *connection.Connection) (*connection.Connection, error) {
	value, err := json.Marshal(conn)
	if err != nil {
		return nil, err
	}

	resp, err := store.Client.Get(context.TODO(), string(conn.ID))
	if resp.Count == 0 {
		return nil, ErrKeyNotFound
	}
	if err != nil {
		return nil, err
	}

	_, err = store.Client.Put(context.TODO(), string(conn.ID), string(value))
	if err != nil {
		return nil, err
	}

	store.Log.Debugw("Connection updated.", "space", conn.Space, "connectionId", conn.ID)

	return conn, nil
}

// DeleteConnection deletes connection from etcd.
func (store Store) DeleteConnection(space string, id connection.ID) error {
	resp, err := store.Client.Delete(context.TODO(), string(id))
	if resp.Deleted == 0 {
		return ErrKeyNotFound
	}
	if err != nil {
		return err
	}

	store.Log.Debugw("Connection deleted.", "space", space, "connectionId", string(id))

	return nil
}

// ErrKeyNotFound is thrown when the key is not found in the store during a Get operation
var ErrKeyNotFound = errors.New("Key not found in store")
