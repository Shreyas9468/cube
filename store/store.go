package store

// Store defines the interface for persisting orchestrator cluster state.
type Store interface {
	Put(bucket string, key string, value []byte) error
	Get(bucket string, key string) ([]byte, error)
	List(bucket string) ([][]byte, error)
	Delete(bucket string, key string) error
	Close() error
}
