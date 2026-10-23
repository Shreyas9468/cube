// Package version holds build-time metadata injected via ldflags.
package version

var (
	// Version is the semantic version tag (e.g., "v0.3.1").
	Version = "dev"
	// Commit is the short git SHA of the build.
	Commit = "unknown"
	// BuildTime is the RFC3339 timestamp of the build.
	BuildTime = "unknown"
)
