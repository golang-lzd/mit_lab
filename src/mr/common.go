package mr

const (
	Mapper = iota
	Reducer
)

const (
	TaskMapper = iota
	TaskReducer
	TaskWait
	TaskEnd
)

const (
	InitState = iota
	ReadyState
	RunningState
	FinishedState
)

type TaskInfo struct {
	State           int
	MapperOrReducer int
}
