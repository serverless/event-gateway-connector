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

	_, err = store.Client.Put(context.TODO(), connectionKey{Space: conn.Space, ID: conn.ID}.String(), string(value))
	if err != nil {
		return nil, err
	}

	store.Log.Debugw("Connection created.", "space", conn.Space, "connectionId", conn.ID)

	return conn, nil
}

// DeleteConnection deletes connection from etcd.
func (store Store) DeleteConnection(space string, id connection.ID) error {
	resp, err := store.Client.Delete(context.TODO(), connectionKey{Space: space, ID: id}.String())
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

type connectionKey struct {
	Space string
	ID    connection.ID
}

func (ck connectionKey) String() string {
	return "connections/" + string(ck.ID)
}
