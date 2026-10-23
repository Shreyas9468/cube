package store_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/Shreyas9468/cube/store"
)

// openTempStore creates a BoltStore in a temp directory and returns it along
// with a cleanup function that removes the database file.
func openTempStore(t *testing.T, buckets ...string) (*store.BoltStore, func()) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	s, err := store.NewBoltStore(path, 0600, buckets)
	if err != nil {
		t.Fatalf("NewBoltStore: %v", err)
	}
	return s, func() {
		s.Close()
		os.Remove(path)
	}
}

func TestPutAndGet(t *testing.T) {
	s, cleanup := openTempStore(t, "tasks")
	defer cleanup()

	key := "task-1"
	val := []byte(`{"id":"task-1"}`)

	if err := s.Put("tasks", key, val); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := s.Get("tasks", key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Errorf("Get returned %q, want %q", got, val)
	}
}

func TestGetMissingKeyReturnsNil(t *testing.T) {
	s, cleanup := openTempStore(t, "tasks")
	defer cleanup()

	got, err := s.Get("tasks", "nonexistent")
	if err != nil {
		t.Fatalf("Get on missing key returned error: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for missing key, got %q", got)
	}
}

func TestList(t *testing.T) {
	s, cleanup := openTempStore(t, "tasks")
	defer cleanup()

	entries := map[string][]byte{
		"a": []byte("val-a"),
		"b": []byte("val-b"),
		"c": []byte("val-c"),
	}
	for k, v := range entries {
		if err := s.Put("tasks", k, v); err != nil {
			t.Fatalf("Put(%s): %v", k, err)
		}
	}

	items, err := s.List("tasks")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(items) != len(entries) {
		t.Errorf("List returned %d items, want %d", len(items), len(entries))
	}
}

func TestDelete(t *testing.T) {
	s, cleanup := openTempStore(t, "tasks")
	defer cleanup()

	if err := s.Put("tasks", "k", []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Delete("tasks", "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	got, err := s.Get("tasks", "k")
	if err != nil {
		t.Fatalf("Get after delete: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil after delete, got %q", got)
	}
}

func TestOverwrite(t *testing.T) {
	s, cleanup := openTempStore(t, "tasks")
	defer cleanup()

	s.Put("tasks", "k", []byte("original"))
	s.Put("tasks", "k", []byte("updated"))

	got, _ := s.Get("tasks", "k")
	if string(got) != "updated" {
		t.Errorf("expected 'updated', got %q", got)
	}
}

func TestUnknownBucketReturnsError(t *testing.T) {
	s, cleanup := openTempStore(t, "tasks")
	defer cleanup()

	if err := s.Put("nonexistent", "k", []byte("v")); err == nil {
		t.Error("expected error for unknown bucket, got nil")
	}
}
