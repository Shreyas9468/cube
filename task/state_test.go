package task_test

import (
	"testing"

	"github.com/Shreyas9468/cube/task"
)

// TestValidStateTransitions verifies that the state machine allows only legal edges.
func TestValidStateTransitions(t *testing.T) {
	valid := []struct {
		from task.State
		to   task.State
	}{
		{task.Pending, task.Scheduled},
		{task.Scheduled, task.Running},
		{task.Running, task.Completed},
		{task.Running, task.Failed},
		{task.Scheduled, task.Failed},
	}

	for _, tc := range valid {
		if !task.ValidStateTransition(tc.from, tc.to) {
			t.Errorf("expected valid transition %v -> %v", tc.from, tc.to)
		}
	}
}

// TestInvalidStateTransitions verifies that illegal edges are rejected.
func TestInvalidStateTransitions(t *testing.T) {
	invalid := []struct {
		from task.State
		to   task.State
	}{
		{task.Pending, task.Running},   // must go through Scheduled first
		{task.Completed, task.Running}, // terminal state
		{task.Failed, task.Running},    // terminal state
		{task.Running, task.Pending},   // no backwards transitions
		{task.Completed, task.Pending},
	}

	for _, tc := range invalid {
		if task.ValidStateTransition(tc.from, tc.to) {
			t.Errorf("expected invalid transition %v -> %v", tc.from, tc.to)
		}
	}
}

// TestStateString verifies the string representation of states.
func TestStateString(t *testing.T) {
	cases := map[task.State]string{
		task.Pending:   "Pending",
		task.Scheduled: "Scheduled",
		task.Running:   "Running",
		task.Completed: "Completed",
		task.Failed:    "Failed",
	}
	for state, want := range cases {
		if got := state.String(); got != want {
			t.Errorf("State(%d).String() = %q, want %q", state, got, want)
		}
	}
}
