package connection

import (
	"encoding/json"
	"fmt"
)

// ID uniquely identifies a conneciton.
type ID string

// Connection is a configuration used by the connector to transport data from source to the EG (target).
type Connection struct {
	Space        string           `json:"space" validate:"required,min=3,space"`
	ID           ID               `json:"connectionId" validate:"required,connectionid"`
	Target       string           `json:"target"`
	Type         SourceType       `json:"type"`
	SourceConfig *json.RawMessage `json:"source"`
	Source       Source           `json:"-" validate:"-"`
}

// UnmarshalJSON ...
func (c *Connection) UnmarshalJSON(data []byte) error {
	type connectionJSON Connection

	rawConn := connectionJSON{}
	if err := json.Unmarshal(data, &rawConn); err != nil {
		return err
	}

	if rawConn.Type == "" {
		return fmt.Errorf("source type not set")
	}

	c.ID = rawConn.ID
	c.Space = rawConn.Space
	c.Target = rawConn.Target
	c.Type = rawConn.Type

	if loader, ok := sources[rawConn.Type]; ok {
		src, err := loader.Load(*rawConn.SourceConfig)
		if err != nil {
			return err
		}

		c.Source = src
		return nil
	}

	return fmt.Errorf("unsupported source type: %s", rawConn.Type)
}

// MarshalJSON ...
func (c *Connection) MarshalJSON() ([]byte, error) {
	type connectionJSON Connection

	conf, err := json.Marshal(c.Source)
	if err != nil {
		return nil, err
	}

	rawConfig := json.RawMessage(conf)
	conn := connectionJSON{
		Space:        c.Space,
		ID:           c.ID,
		Target:       c.Target,
		Type:         c.Type,
		SourceConfig: &rawConfig,
	}

	return json.Marshal(conn)
}
