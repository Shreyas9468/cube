package store

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
)

// BoltStore implements Store using an embedded BoltDB key-value database.
type BoltStore struct {
	db *bolt.DB
}

// NewBoltStore initializes a BoltDB instance with the specified buckets.
func NewBoltStore(filepathStr string, mode os.FileMode, buckets []string) (*BoltStore, error) {
	if dir := filepath.Dir(filepathStr); dir != "" && dir != "." {
		_ = os.MkdirAll(dir, 0755)
	}

	db, err := bolt.Open(filepathStr, mode, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("unable to open bolt db at %s: %w", filepathStr, err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		for _, b := range buckets {
			if _, err := tx.CreateBucketIfNotExists([]byte(b)); err != nil {
				return fmt.Errorf("unable to create bucket %s: %w", b, err)
			}
		}
		return nil
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	return &BoltStore{db: db}, nil
}

// Put writes a key-value pair to the specified bucket.
func (b *BoltStore) Put(bucket string, key string, value []byte) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte(bucket))
		if bkt == nil {
			return fmt.Errorf("bucket %s not found", bucket)
		}
		return bkt.Put([]byte(key), value)
	})
}

// Get retrieves the value associated with key in the specified bucket.
func (b *BoltStore) Get(bucket string, key string) ([]byte, error) {
	var val []byte
	err := b.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte(bucket))
		if bkt == nil {
			return fmt.Errorf("bucket %s not found", bucket)
		}
		v := bkt.Get([]byte(key))
		if v != nil {
			val = make([]byte, len(v))
			copy(val, v)
		}
		return nil
	})
	return val, err
}

// List returns all raw values stored in the specified bucket.
func (b *BoltStore) List(bucket string) ([][]byte, error) {
	var items [][]byte
	err := b.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte(bucket))
		if bkt == nil {
			return fmt.Errorf("bucket %s not found", bucket)
		}
		return bkt.ForEach(func(k, v []byte) error {
			item := make([]byte, len(v))
			copy(item, v)
			items = append(items, item)
			return nil
		})
	})
	return items, err
}

// Delete removes a key from the specified bucket.
func (b *BoltStore) Delete(bucket string, key string) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte(bucket))
		if bkt == nil {
			return fmt.Errorf("bucket %s not found", bucket)
		}
		return bkt.Delete([]byte(key))
	})
}

// Close closes the underlying BoltDB database file.
func (b *BoltStore) Close() error {
	return b.db.Close()
}
