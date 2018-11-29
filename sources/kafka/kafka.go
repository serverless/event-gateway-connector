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
	BoostrapServers string `json:"bootstrapServers" validate:"required"`
	Offset          string `json:"offset" validate:"eq=earliest|eq=latest"`
	Topic           string `json:"topic" validate:"required"`

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

	// fetch partition metadata
	metadataConsumer, err := kafkalib.NewConsumer(&kafkalib.ConfigMap{
		"bootstrap.servers":               src.BoostrapServers,
		"group.id":                        uuid.NewV4(),
		"session.timeout.ms":              6000,
		"go.application.rebalance.enable": true, // delegate Assign() responsibility to the source
		"default.topic.config":            kafkalib.ConfigMap{"auto.offset.reset": src.Offset}})
	if err != nil {
		return nil, fmt.Errorf("unable to create consumer: %s", err.Error())
	}

	metadata, err := metadataConsumer.GetMetadata(&src.Topic, false, 100000)
	if err != nil {
		return nil, fmt.Errorf("unable to create consumer: %s", err.Error())
	}

	// create consumers for every partition
	numPartitions := len(metadata.Topics[src.Topic].Partitions)
	consumers := make([]*kafkalib.Consumer, numPartitions)
	partitions := make([]kafkalib.TopicPartition, numPartitions)
	for i, partition := range metadata.Topics[src.Topic].Partitions {
		consumer, err := kafkalib.NewConsumer(&kafkalib.ConfigMap{
			"bootstrap.servers":               src.BoostrapServers,
			"group.id":                        uuid.NewV4(),
			"session.timeout.ms":              6000,
			"go.application.rebalance.enable": true, // delegate Assign() responsibility to the source
			"default.topic.config":            kafkalib.ConfigMap{"auto.offset.reset": src.Offset}})
		if err != nil {
			return nil, fmt.Errorf("unable to create consumer: %s", err.Error())
		}

		partition := kafkalib.TopicPartition{
			Topic:     &src.Topic,
			Partition: partition.ID,
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
	partition := k.partitions[partitionIndex]

	if parsed, err := strconv.Atoi(savedOffset); err == nil {
		partition.Offset = kafkalib.Offset(int64(parsed) + 1)
	}

	consumer := k.consumers[partitionIndex]

	err := consumer.Assign([]kafkalib.TopicPartition{partition})
	if err != nil {
		return nil, fmt.Errorf("Kafka consumer couldn't assign partition: %s", err.Error())
	}

	var timeout time.Duration
	if d, ok := ctx.Deadline(); ok {
		timeout = d.Sub(time.Now())
	}

	msg, err := consumer.ReadMessage(timeout)
	if err != nil {
		if err.(kafkalib.Error).Code() == kafkalib.ErrTimedOut {
			return &connection.Records{}, nil
		}
		return nil, fmt.Errorf("Kafka consumer returned error: %s", err.Error())
	}

	fmt.Printf("%% Message on %s:\n%s\n", msg.TopicPartition, string(msg.Value))

	fmt.Println(fmt.Sprintf("------------ RETURNED OFFSET %+v", msg.TopicPartition.Offset))

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
func (k Kafka) Close() error {
	for _, customer := range k.consumers {
		if err := customer.Close(); err != nil {
			return err
		}
	}
	return nil
}
