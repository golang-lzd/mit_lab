package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/bwmarrin/snowflake"
	"log"
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
	return fmt.Sprintf("middle-%d-%d", mapIdx, reducerIdx)
}

func GetOutFileName(reducerIdx int) string {
	return fmt.Sprintf("mr-out-%d", reducerIdx)
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

var node *snowflake.Node

func init() {
	node, _ = snowflake.NewNode(1)
}

func GetUUID() int64 {
	return node.Generate().Int64()
}

func FormatStruct(s interface{}) string {
	bs, err := json.Marshal(s)
	if err != nil {
		log.Println(err.Error())
	}
	var out bytes.Buffer
	json.Indent(&out, bs, "", "\t")
	return out.String()
}
