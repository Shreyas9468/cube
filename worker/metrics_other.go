//go:build !linux

package worker

// Stats is a stub for non-Linux platforms.
type Stats struct {
	TaskCount int
}

// GetStats returns an empty Stats struct on non-Linux platforms.
func GetStats() *Stats {
	return &Stats{}
}
