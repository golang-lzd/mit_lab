package mr

import (
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

// 定义 mapper,reducer 任务队列
// 假设一共输入文件数为NFile,Reducer 数为NReducer
// 那么一共NFile 个 Mapper 任务，NReducer 个 Reducer 任务
// 中间文件数为NFile*NReducer 个

// 假设Coordinator 不会挂掉
// TODO Coordinator 挂掉仍能正常工作的实现

type Coordinator struct {
	// Your definitions here.
	MapperReadyQueue  *BlockedQueue
	MapperRunningSet  *MapSet
	ReducerReadyQueue *BlockedQueue
	ReducerRunningSet *MapSet

	NReducer   int
	MapperDone bool
	AllDone    bool
	Files      []string
}

func (c *Coordinator) AskTask(args *AskTaskReq, reply *AskTaskReply) error {
	if c.AllDone {
		reply.TaskType = TaskEnd
		return nil
	}
	if c.MapperDone {
		if c.ReducerReadyQueue.Empty() {
			reply.TaskType = TaskWait
		} else {
			reply.ReducerTaskInfo = c.ReducerReadyQueue.PeekFront().(*ReducerTask)
			reply.TaskType = TaskReducer
			reply.TaskID = reply.ReducerTaskInfo.TaskID
			c.ReducerRunningSet.Set(reply.TaskID, reply.ReducerTaskInfo)
		}
	} else {
		if c.MapperReadyQueue.Empty() {
			reply.TaskType = TaskWait
		} else {
			reply.MapTaskInfo = c.MapperReadyQueue.PeekFront().(*MapTask)
			reply.TaskType = TaskMapper
			reply.TaskID = reply.MapTaskInfo.TaskID
			c.MapperRunningSet.Set(reply.TaskID, reply.MapTaskInfo)
		}
	}

	return nil
}

// 任务完成后
// 1. RunningQueue 标记任务为完成状态，删除该节点
// 2. 如果是Mapper任务完成，需要判断Mapper Running 队列是否为空，如果为空，进入Reducer 状态，创建Reducer 任务
// 3. 如果是Reducer 任务完成，需要判断Reducer Running 队列是否为空，如果为空，进入AllDone 状态，进程过一段时间推出。

func (c *Coordinator) TaskDone(args *TaskDoneReq, reply *TaskDoneReply) error {
	if args.TaskType == TaskMapper && c.MapperRunningSet.Exists(args.TaskID) {
		c.MapperRunningSet.Erase(args.TaskID)
		if c.MapperReadyQueue.Empty() && c.MapperRunningSet.Empty() {
			// 进入reducer 状态
			c.MapperDone = true
			for i := 0; i < c.NReducer; i++ {
				c.ReducerReadyQueue.PushBack(&ReducerTask{
					TaskEntity: TaskEntity{
						TaskID:    GetUUID(),
						StartTime: time.Now(),
					},
					ReducerIndex: i,
					FileCount:    len(c.Files),
				})
			}
		}
	} else {
		// 判断是否因超时或者网络阻塞导致任务已经被重新执行
		if c.ReducerRunningSet.Exists(args.TaskID) {
			c.ReducerRunningSet.Erase(args.TaskID)
			if c.ReducerReadyQueue.Empty() && c.ReducerRunningSet.Empty() {
				c.AllDone = true
			}
		}
	}

	return nil
}

func (c *Coordinator) LoopRemoveTimeoutMapTasks() {
	for {
		// 查找mappset
		for k, v := range c.MapperRunningSet.Data {
			taskInfo := v.(*MapTask)
			if time.Since(taskInfo.StartTime) > TaskTimeOut {
				c.MapperReadyQueue.PushBack(taskInfo)
				c.MapperRunningSet.Erase(k)
			}
		}
		// 查找 reducerset
		for k, v := range c.ReducerRunningSet.Data {
			taskInfo := v.(*ReducerTask)
			if time.Since(taskInfo.StartTime) > TaskTimeOut {
				c.ReducerReadyQueue.PushBack(taskInfo)
				c.MapperRunningSet.Erase(k)
			}
		}
		time.Sleep(time.Second)
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.AllDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapperReadyQueue: newBlockedQueue(),
		MapperDone:       false,
		AllDone:          false,
		NReducer:         nReduce,
		Files:            files,
	}

	// Your code here.
	// create map task
	for i := 0; i < len(files); i++ {
		c.MapperReadyQueue.list.PushBack(&MapTask{
			FileIndex: i,
			FileName:  files[i],
			NReducer:  nReduce,
			TaskEntity: TaskEntity{
				TaskID:    GetUUID(),
				StartTime: time.Now(),
			},
		})
	}

	c.server()

	// 需要开启一个协程 去定时查看是否存在超时的Mapper or Reducer
	go c.LoopRemoveTimeoutMapTasks()

	return &c
}
