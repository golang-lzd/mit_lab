package mr

import (
	"fmt"
	"github.com/bwmarrin/snowflake"
	"time"
)

type TaskKind int

const (
	TaskMapper TaskKind = iota
	TaskReducer
	TaskWait
	TaskEnd
)

type TaskEntity struct {
	TaskID    int64
	StartTime time.Time
}

type MapTask struct {
	TaskEntity
	FileIndex int
	FileName  string
	NReducer  int
}

type ReducerTask struct {
	TaskEntity
	ReducerIndex int
	FileCount    int
}

type TaskInfo interface {
}

var _ TaskInfo = (*MapTask)(nil)
var _ TaskInfo = (*ReducerTask)(nil)

func GetMiddleFileName(mapIdx int, reducerIdx int) string {
	return fmt.Sprintf("out-%d-%d", mapIdx, reducerIdx)
}

func GetOutFileName(reducerIdx int) string {
	return fmt.Sprintf("out-%d.txt", reducerIdx)
}

const TaskTimeOut = 10 * time.Second

type AskTaskReq struct {
}

type AskTaskReply struct {
	TaskID          int64
	TaskType        TaskKind
	MapTaskInfo     *MapTask
	ReducerTaskInfo *ReducerTask
}

type TaskDoneReq struct {
	TaskType TaskKind
	TaskID   int64
}

type TaskDoneReply struct {
}

func GetUUID() int64 {
	node, _ := snowflake.NewNode(1)
	return node.Generate().Int64()
}
