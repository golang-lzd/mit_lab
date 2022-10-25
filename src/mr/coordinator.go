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
		log.Println("[Ask Task]:task all done.")
		return nil
	}
	if c.MapperDone {
		log.Println("收到 ask task 请求.当前处于reducer 状态")
		if c.ReducerReadyQueue.Empty() {
			reply.TaskType = TaskWait
			log.Println("[Ask Task]:[Mapper]:task wait")
		} else {
			reply.ReducerTaskInfo = c.ReducerReadyQueue.PeekFront().(*ReducerTask)
			reply.TaskType = TaskReducer
			reply.TaskID = reply.ReducerTaskInfo.TaskID
			reply.ReducerTaskInfo.StartTime = time.Now()
			c.ReducerRunningSet.Lock.Lock()
			c.ReducerRunningSet.Set(reply.TaskID, reply.ReducerTaskInfo)
			c.ReducerRunningSet.Lock.Unlock()
			log.Printf("[Ask Task]:[Mapper]:task:%+v \n", *reply)
		}
	} else {
		if c.MapperReadyQueue.Empty() {
			reply.TaskType = TaskWait
			log.Println("[Ask Task]:[Reducer]:task wait")
		} else {
			reply.MapTaskInfo = c.MapperReadyQueue.PeekFront().(*MapTask)
			reply.TaskType = TaskMapper
			reply.TaskID = reply.MapTaskInfo.TaskID
			reply.MapTaskInfo.StartTime = time.Now()
			c.MapperRunningSet.Lock.Lock()
			c.MapperRunningSet.Set(reply.TaskID, reply.MapTaskInfo)
			c.MapperRunningSet.Lock.Unlock()
			log.Printf("[Ask Task]:[Reducer]:task:%+v \n", *reply)
		}
	}

	return nil
}

// 任务完成后
// 1. RunningQueue 标记任务为完成状态，删除该节点
// 2. 如果是Mapper任务完成，需要判断Mapper Running 队列是否为空，如果为空，进入Reducer 状态，创建Reducer 任务
// 3. 如果是Reducer 任务完成，需要判断Reducer Running 队列是否为空，如果为空，进入AllDone 状态，进程过一段时间推出。

func (c *Coordinator) TaskDone(args *TaskDoneReq, reply *TaskDoneReply) error {
	if args.TaskType == TaskMapper {
		//log.Printf("[Task Done]:[Mapper]:args:%+v \n", *args)
		c.MapperRunningSet.Lock.Lock()
		if c.MapperRunningSet.Exists(args.TaskID) {
			log.Printf("[Task Done]:[Mapper]:task done,remove %d from mapperRunningSet \n", args.TaskID)
			c.MapperRunningSet.Erase(args.TaskID)
			if c.MapperRunningSet.Empty() && c.MapperReadyQueue.Empty() {
				log.Println("进入 reduce 状态")
				// 进入reducer 状态
				c.MapperDone = true
				for i := 0; i < c.NReducer; i++ {
					tmp := &ReducerTask{
						TaskEntity: TaskEntity{
							TaskID:    GetUUID(),
							StartTime: time.Now(),
						},
						ReducerIndex: i,
						FileCount:    len(c.Files),
					}
					log.Printf("新建立的Reducer Task :%v \n", FormatStruct(tmp))
					c.ReducerReadyQueue.PushBack(tmp)
				}
			}
		} else {
			log.Printf("[TaskDone]任务超时，已经从 mapperRunningSet 删除,args:%+v \n", *args)
		}
		c.MapperRunningSet.Lock.Unlock()

	} else {
		log.Printf("[Task Done]:[Reducer]:args:%+v \n", *args)
		// 判断是否因超时或者网络阻塞导致任务已经被重新执行
		c.ReducerRunningSet.Lock.Lock()
		if c.ReducerRunningSet.Exists(args.TaskID) {
			log.Printf("[Task Done]:[Reducer]:task done,remove %d from ReducerRunningSet \n", args.TaskID)
			c.ReducerRunningSet.Erase(args.TaskID)
			if c.ReducerReadyQueue.Empty() && c.ReducerRunningSet.Empty() {
				log.Println("reducer 完成")
				c.AllDone = true
			}
		} else {
			log.Printf("任务超时，已经从 reducerRunningSet 删除,args:%+v \n", *args)
		}
		c.ReducerRunningSet.Lock.Unlock()
	}

	return nil
}

func (c *Coordinator) LoopRemoveTimeoutMapTasks() {
	for {
		// 查找mappset
		c.MapperRunningSet.Lock.Lock()
		for k, v := range c.MapperRunningSet.Data {
			taskInfo := v.(*MapTask)
			if time.Since(taskInfo.StartTime) > TaskTimeOut {
				log.Printf("[LoopRemoveTimeoutMapTasks] 任务超时,从 MapperRunningSet 删除,%+v \n", *taskInfo)
				taskInfo.StartTime = time.Now()
				c.MapperReadyQueue.PushBack(taskInfo)
				c.MapperRunningSet.Erase(k)
			}
		}
		c.MapperRunningSet.Lock.Unlock()

		c.ReducerRunningSet.Lock.Lock()
		// 查找 reducerset
		for k, v := range c.ReducerRunningSet.Data {
			taskInfo := v.(*ReducerTask)
			if time.Since(taskInfo.StartTime) > TaskTimeOut {
				log.Printf("[LoopRemoveTimeoutMapTasks] 任务超时,从 ReducerRunningSet 删除,%+v \n", *taskInfo)
				taskInfo.StartTime = time.Now()
				c.ReducerReadyQueue.PushBack(taskInfo)
				c.ReducerRunningSet.Erase(k)
			}
		}
		c.ReducerRunningSet.Lock.Unlock()

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
		MapperReadyQueue:  newBlockedQueue(),
		MapperRunningSet:  NewMapSet(),
		ReducerRunningSet: NewMapSet(),
		ReducerReadyQueue: newBlockedQueue(),
		MapperDone:        false,
		AllDone:           false,
		NReducer:          nReduce,
		Files:             files,
	}
	log.Printf("coordinator:%v \n", FormatStruct(c))

	// Your code here.
	// create map task
	for i := 0; i < len(files); i++ {
		tmp := &MapTask{
			FileIndex: i,
			FileName:  files[i],
			NReducer:  nReduce,
			TaskEntity: TaskEntity{
				TaskID: GetUUID(),
				// StartTime: time.Now(),StartTime 是用于Running队列判断是否超时的，所以设置的时间应该是加入Running队列的时候才设置
			},
		}
		c.MapperReadyQueue.list.PushBack(tmp)
		log.Printf("Mapper Task:%s \n", FormatStruct(tmp))
	}

	c.server()

	// 需要开启一个协程 去定时查看是否存在超时的Mapper or Reducer
	go c.LoopRemoveTimeoutMapTasks()
	//go c.ConditionMonitor()

	return &c
}

func (c *Coordinator) ConditionMonitor() {
	for {
		log.Printf("[MapperReadyQueue]:size:%d \t [MapperRunningSet]:size:%d \t [ReducerReadyQueue]:size:%d \t [ReducerRunningSet]:size:%d \n", c.MapperReadyQueue.Size(), c.MapperRunningSet.Size(), c.ReducerReadyQueue.Size(), c.ReducerRunningSet.Size())
		time.Sleep(3 * time.Second)
	}
}
