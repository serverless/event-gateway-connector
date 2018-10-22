package connection

// Service for managing connections.
type Service interface {
	ListConnections(space string) ([]*Connection, error)
	CreateConnection(conn *Connection) (*Connection, error)
	UpdateConnection(conn *Connection) (*Connection, error)
	DeleteConnection(space string, id ID) error
}
