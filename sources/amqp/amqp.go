package amqp

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/streadway/amqp"

	"github.com/serverless/event-gateway-connector/connection"
	validator "gopkg.in/go-playground/validator.v9"
)

// AMQP is a configuration used to configure AMQP as a source.
type AMQP struct {
	URL       string `json:"url" validate:"required,url"`
	QueueName string `json:"queueName" validate:"required"`

	connection *amqp.Connection
	channel    *amqp.Channel
	deliveryCh <-chan amqp.Delivery
}

func init() {
	connection.RegisterSource(connection.SourceType("amqp"), Load)
}

// Load returns configured AMQP service based on input JSON blob.
func Load(data []byte) (connection.Source, error) {
	mq := &AMQP{}
	err := json.Unmarshal(data, &mq)
	if err != nil {
		return nil, fmt.Errorf("unable to load amqp source config: %s", err.Error())
	}

	if err := validator.New().Struct(mq); err != nil {
		return nil, fmt.Errorf("missing required fields for amqp source: %s", err.Error())
	}

	return mq, nil
}

// Fetch lazy loads AMQP connection and channel and blocks until there is another message
// in the delivery channel.
func (a *AMQP) Fetch(ctx context.Context, shardID uint, lastSeq string) (*connection.Records, error) {
	if a.deliveryCh == nil {
		connection, err := amqp.Dial(a.URL)
		if err != nil {
			return nil, err
		}
		a.connection = connection

		channel, err := connection.Channel()
		if err != nil {
			return nil, err
		}
		a.channel = channel

		ch, err := channel.Consume(a.QueueName, "serverless/event-gateway-consumer", false, true, false, false, nil)
		if err != nil {
			return nil, err
		}

		a.deliveryCh = ch
	}

	select {
	case <-ctx.Done():
		return &connection.Records{}, nil
	case delivery := <-a.deliveryCh:
		defer delivery.Ack(false)
		return &connection.Records{Data: [][]byte{delivery.Body}}, nil
	}
}

// NumberOfWorkers returns number of shards to handle by the pool
func (a *AMQP) NumberOfWorkers() uint {
	return 1
}

// Close closes channel and connection to AMQP server.
func (a *AMQP) Close(_ uint) error {
	if err := a.connection.Close(); err != nil {
		return nil
	}
	return nil
}
