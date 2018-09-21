package connection

// Service for managing connections.
type Service interface {
	CreateConnection(conn *Connection) (*Connection, error)
	UpdateConnection(conn *Connection) (*Connection, error)
	DeleteConnection(space string, id ID) error
}
