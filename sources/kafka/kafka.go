package kafka

// Kafka is the configuration struct to leverage when defining a Kafka resource
// TODO: add tls config portion
type Kafka struct {
	Topic              string   `json:"topic"`
	NumberOfPartitions uint     `json:"numberOfPartitions"`
	Brokers            []string `json:"hosts"`
}

// Fetch retrieves the next document from the kafka data source
func (k Kafka) Fetch(partID uint) error {
	// getrecord here
	return nil
}

// NumberOfWorkers returns number of partitions to handle by the pool
func (k Kafka) NumberOfWorkers() uint {
	return k.NumberOfPartitions
}
