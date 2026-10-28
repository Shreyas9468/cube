package main

import (
	"testing"

	"github.com/Shreyas9468/cube/task"
)

func TestStateToString(t *testing.T) {
	tests := []struct {
		state    task.State
		expected string
	}{
		{task.Pending, "Pending"},
		{task.Scheduled, "Scheduled"},
		{task.Running, "Running"},
		{task.Completed, "Completed"},
		{task.Failed, "Failed"},
		{task.State(99), "Unknown"},
	}

	for _, tt := range tests {
		got := stateToString(tt.state)
		if got != tt.expected {
			t.Errorf("stateToString(%v) = %s; want %s", tt.state, got, tt.expected)
		}
	}
}
