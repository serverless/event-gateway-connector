package connection

import "context"

// Source is the default interface that each connection source (e.g. awskinesis, kafka) need
// to satisfy in order to deliver events to the EG
type Source interface {
	Fetch(context.Context, uint) error
	NumberOfWorkers() uint
}

// SourceLoader is where we define individual service types to return
type SourceLoader interface {
	Connect(ID, []byte) (Source, error)
}

// SourceType abstraction for the data sources
type SourceType string

var sources = make(map[SourceType]SourceLoader)

// RegisterSource ...
func RegisterSource(source SourceType, loader SourceLoader) {
	sources[source] = loader
}
