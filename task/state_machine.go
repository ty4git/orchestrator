package task

import "slices"

type State int

//go:generate stringer -type=State
const (
	Pending State = iota
	Scheduled
	Running
	Completed
	Failed
	Stopped
	Deleted
)

var stateTransitionMap = map[State][]State{
	Pending:   {Scheduled, Stopped},
	Scheduled: {Running, Failed, Stopped},
	Running:   {Running, Completed, Failed, Stopped},
	Completed: {Running, Completed, Failed, Stopped},
	Failed:    {Stopped},
	Stopped:   {Deleted},
	Deleted:   {},
}

func Contains(states []State, state State) bool {
	return slices.Contains(states, state)
}

func IsValidStateTransition(src State, dst State) bool {
	return Contains(stateTransitionMap[src], dst)
}
