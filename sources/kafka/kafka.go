package kafka

// Kafka is the configuration struct to leverage when defining a Kafka resource
// TODO: add tls config portion
type Kafka struct {
	Topic      string   `json:"topic"`
	Partitions int      `json:"partitions"`
	Brokers    []string `json:"hosts"`
}
