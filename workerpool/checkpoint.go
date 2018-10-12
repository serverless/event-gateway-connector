package workerpool

// Checkpointer is an interface definition for a checkpoint-based system for workers
//   This tool is useful for storing off last-read sequence numbers (aka checkpoints) for
//   individual sources
type Checkpointer interface {
	RetrieveCheckpoint(k string) (string, error)
	UpdateCheckpoint(k, v string) error
	DeleteCheckpoint(k string) error
}
