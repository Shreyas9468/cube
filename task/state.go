package task

var stateTransitionMap = map[State][]State{
	Pending:   {Scheduled},
	Scheduled: {Scheduled, Running, Failed},
	Running:   {Running, Completed, Failed},
	Completed: {},
	Failed:    {},
}

var stateNames = map[State]string{
	Pending:   "Pending",
	Scheduled: "Scheduled",
	Running:   "Running",
	Completed: "Completed",
	Failed:    "Failed",
}

// String returns a human-readable name for the state.
func (s State) String() string {
	if name, ok := stateNames[s]; ok {
		return name
	}
	return "Unknown"
}

// Contains checks if a given state exists in a slice.
func Contains(states []State, state State) bool {
	for _, s := range states {
		if s == state {
			return true
		}
	}
	return false
}

// ValidStateTransition returns true if transitioning from src to dst is allowed.
func ValidStateTransition(src State, dst State) bool {
	return Contains(stateTransitionMap[src], dst)
}
