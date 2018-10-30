package connection

import "github.com/serverless/event-gateway/metadata"

// Service for managing connections.
type Service interface {
	ListConnections(space string, filters ...metadata.Filter) ([]*Connection, error)
	CreateConnection(conn *Connection) (*Connection, error)
	UpdateConnection(conn *Connection) (*Connection, error)
	DeleteConnection(space string, id ID) error
}
