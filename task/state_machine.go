package task

type State int

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
	Pending:   {Scheduled},
	Scheduled: {Running, Failed},
	Running:   {Running, Completed, Failed, Stopped},
	Completed: {Running, Completed, Failed, Stopped},
	Failed:    {Stopped},
	Stopped:   {Deleted},
	Deleted:   {},
}

func Contains(states []State, state State) bool {
	for _, s := range states {
		if s == state {
			return true
		}
	}
	return false
}

func ValidStateTransition(src State, dst State) bool {
	return Contains(stateTransitionMap[src], dst)
}
