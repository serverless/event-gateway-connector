package connection

// ID uniquely identifies a conneciton.
type ID string

// Connection is a configuration used by the connector to transport data from source to the EG (target).
type Connection struct {
	Space            string            `json:"space"`
	ID               ID                `json:"connectionId"`
	Target           string            `json:"target"`
	Type             string            `json:"type"`
	AWSKinesisSource *AWSKinesisSource `json:"AWSKinesisSource,omitempty"`
	KafkaSource      *KafkaSource      `json:"KafkaSource,omitempty"`
}

// AWSKinesisSource is a configuration used to configure AWS Kinesis stream as a source.
type AWSKinesisSource struct {
	StreamName         string `json:"streamName"`
	Region             string `json:"region"`
	AWSAccessKeyID     string `json:"awsAccessKeyId,omitempty"`
	AWSSecretAccessKey string `json:"awsSecretAccessKey,omitempty"`
	AWSSessionToken    string `json:"awsSessionToken,omitempty"`
}

// KafkaSource is the configuration struct to leverage when defining a Kafka resource
// TODO: add tls config portion
type KafkaSource struct {
	Topic      string   `json:"topic"`
	Partitions int      `json:"partitions"`
	Brokers    []string `json:"hosts"`
}
