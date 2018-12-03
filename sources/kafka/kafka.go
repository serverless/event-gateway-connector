package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	kafkalib "github.com/confluentinc/confluent-kafka-go/kafka"
	uuid "github.com/satori/go.uuid"
	"github.com/serverless/event-gateway-connector/connection"
	validator "gopkg.in/go-playground/validator.v9"
)

// Kafka is a configuration used to configure Kafka topic as a source.
type Kafka struct {
	BoostrapServers      string `json:"bootstrapServers" validate:"required"`
	Offset               string `json:"offset" validate:"eq=earliest|eq=latest"`
	Topic                string `json:"topic" validate:"required"`
	ConfluentCloudKey    string `json:"confluentCloudKey,omitempty"`
	ConfluentCloudSecret string `json:"confluentCloudSecret,omitempty"`

	consumers  []*kafkalib.Consumer
	partitions []kafkalib.TopicPartition
}

func init() {
	connection.RegisterSource(connection.SourceType("kafka"), Load)
}

// Load decodes the provided JSON data into valid Kafka format and creates Kafka
// Consumer instance. Provided the connection is successful, we return an instance
// of the connection.Source, othewise an error.
func Load(data []byte) (connection.Source, error) {
	var src Kafka
	err := json.Unmarshal(data, &src)
	if err != nil {
		return nil, fmt.Errorf("unable to load kafka source config: %s", err.Error())
	}

	if err := validator.New().Struct(src); err != nil {
		return nil, fmt.Errorf("missing required fields for kafka source: %s", err.Error())
	}

	config := kafkalib.ConfigMap{
		"bootstrap.servers":               src.BoostrapServers,
		"group.id":                        uuid.NewV4(),
		"session.timeout.ms":              5000,
		"go.application.rebalance.enable": true, // delegate Assign() responsibility to the source
		"default.topic.config":            kafkalib.ConfigMap{"auto.offset.reset": src.Offset}}
	if src.ConfluentCloudKey != "" && src.ConfluentCloudSecret != "" {
		config["broker.version.fallback"] = "0.10.0.0"
		config["api.version.fallback.ms"] = 0
		config["sasl.mechanisms"] = "PLAIN"
		config["security.protocol"] = "SASL_SSL"
		config["sasl.username"] = src.ConfluentCloudKey
		config["sasl.password"] = src.ConfluentCloudSecret
	}

	metadata, err := src.fetchPartitions(src.Topic, config)
	if err != nil {
		return nil, fmt.Errorf("unable to fetch topic partitions: %s", err.Error())
	}

	// create consumers for every partition
	consumers := make([]*kafkalib.Consumer, len(metadata))
	partitions := make([]kafkalib.TopicPartition, len(metadata))
	for i, meta := range metadata {
		consumer, err := kafkalib.NewConsumer(&config)
		if err != nil {
			return nil, fmt.Errorf("unable to create consumer: %s", err.Error())
		}

		partition := kafkalib.TopicPartition{
			Topic:     &src.Topic,
			Partition: meta.ID,
		}
		err = consumer.Assign([]kafkalib.TopicPartition{partition})
		if err != nil {
			return nil, fmt.Errorf("Kafka consumer couldn't assign partition: %s", err.Error())
		}

		consumers[i] = consumer
		partitions[i] = partition
	}

	src.consumers = consumers
	src.partitions = partitions

	return src, nil
}

// Fetch retrieves the next document from the awskinesis source
func (k Kafka) Fetch(ctx context.Context, partitionIndex uint, savedOffset string) (*connection.Records, error) {
	consumer := k.consumers[partitionIndex]

	partition := k.partitions[partitionIndex]
	if parsed, err := strconv.Atoi(savedOffset); err == nil {
		partition.Offset = kafkalib.Offset(int64(parsed) + 1)
	}

	err := consumer.Assign([]kafkalib.TopicPartition{partition})
	if err != nil {
		return nil, fmt.Errorf("Kafka consumer couldn't assign partition: %s", err.Error())
	}

	if err := ctx.Err(); err != nil { // check if context is already canceled
		return nil, err
	}
	timeout := 0 * time.Millisecond
	if deadline, ok := ctx.Deadline(); ok {
		timeout = time.Until(deadline)
	}
	msg, err := consumer.ReadMessage(timeout)
	if err != nil {
		if err.(kafkalib.Error).Code() == kafkalib.ErrTimedOut {
			return &connection.Records{}, nil
		}
		return nil, fmt.Errorf("Kafka consumer returned error: %s", err.Error())
	}

	return &connection.Records{
		Data:         [][]byte{msg.Value},
		LastSequence: msg.TopicPartition.Offset.String(),
	}, nil
}

// NumberOfWorkers returns number of shards to handle by the pool
func (k Kafka) NumberOfWorkers() uint {
	return uint(len(k.consumers))
}

// Close consumer.
func (k Kafka) Close(partitionIndex uint) error {
	if err := k.consumers[partitionIndex].Close(); err != nil {
		return err
	}
	return nil
}

func (k Kafka) fetchPartitions(topic string, config kafkalib.ConfigMap) ([]kafkalib.PartitionMetadata, error) {
	consumer, err := kafkalib.NewConsumer(&config)
	if err != nil {
		return nil, err
	}
	defer consumer.Close()

	metadata, err := consumer.GetMetadata(&topic, false, 100000)
	if err != nil {
		return nil, err
	}
	return metadata.Topics[topic].Partitions, nil
}
