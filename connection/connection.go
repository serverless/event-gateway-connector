package connection

import (
	"encoding/json"
	"fmt"
)

// ID uniquely identifies a conneciton.
type ID string

// Connection is a configuration used by the connector to transport data from source to the EG (target).
type Connection struct {
	Space        string           `json:"space"`
	ID           ID               `json:"connectionId"`
	Target       string           `json:"target"`
	EventType    string           `json:"eventType"`
	SourceType   SourceType       `json:"type"`
	SourceConfig *json.RawMessage `json:"source"`
	Source       Source           `json:"-"`
}

// UnmarshalJSON is a custom unmarshaller for the Controller struct in order to handle the various
// Source payloads that could arrive. Since each Source will likely require a unique configuration, this
// custom unmarhsaller handles those sepcific payload portions based on their source definition (including
// validation)
func (c *Connection) UnmarshalJSON(data []byte) error {
	type connectionJSON Connection

	rawConn := connectionJSON{}
	if err := json.Unmarshal(data, &rawConn); err != nil {
		return err
	}

	if rawConn.SourceType == "" {
		return fmt.Errorf("source type not set")
	}

	c.ID = rawConn.ID
	c.Space = rawConn.Space
	c.Target = rawConn.Target
	c.EventType = rawConn.EventType
	c.SourceType = rawConn.SourceType

	if loader, ok := sources[rawConn.SourceType]; ok {
		src, err := loader.Load(*rawConn.SourceConfig)
		if err != nil {
			return err
		}

		c.Source = src
		return nil
	}

	return fmt.Errorf("unsupported source type: %s", rawConn.SourceType)
}

// MarshalJSON is a custom JSON marshaller for the Controller struct in order to help build the SourceConfig
// payload dependent on each unique SourceType. Since each Source will likely require a unique configuration,
// this custom marshaller handles those specific payload portions based on their source definition.
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
		EventType:    c.EventType,
		SourceType:   c.SourceType,
		SourceConfig: &rawConfig,
	}

	return json.Marshal(conn)
}
