package connection

// Source is the default interface that each connection source (e.g. awskinesis, kafka) need
// to satisfy in order to deliver events to the EG.
type Source interface {
	Fetch(uint, string) (*Records, error)
	NumberOfWorkers() uint
	Close() error
}

// SourceType abstraction for the data sources
type SourceType string

// SourceLoader takes JSON blob as an input and returns configured Source instance
type SourceLoader func([]byte) (Source, error)

// RegisterSource registers SourceLoader function for specific source type.
func RegisterSource(source SourceType, loader SourceLoader) {
	sources[source] = loader
}

var sources = make(map[SourceType]SourceLoader)

// Records is the default data structure to return message payloads
// from the registered source. There are two fields:
//   Data         -- a slice of []byte that represent the data payload of each message
//   LastSequence -- last sequence read from the source (kinesis sequence number, kafka log offset, etc)
type Records struct {
	Data         [][]byte
	LastSequence string
}
