package kafka

// Kafka is the configuration struct to leverage when defining a Kafka resource
// TODO: add tls config portion
type Kafka struct {
	Topic      string   `json:"topic"`
	Partitions int      `json:"partitions"`
	Brokers    []string `json:"hosts"`
}

// Fetch retrieves the next document from the kafka data source
func (k Kafka) Fetch() ([]byte, error) {
	// getrecord here
	return nil, nil
}
