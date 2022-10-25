package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type AWorker struct {
	Mapf    func(filename string, contents string) []KeyValue
	Reducef func(key string, values []string) string

	isDone   bool
	WorkerID int64
}

var worker *AWorker

func (aw *AWorker) process() {
	for !aw.isDone {
		aw.CallAskTask()
	}
}

func (aw *AWorker) CallTaskDone(reply *AskTaskReply) {
	args := TaskDoneReq{
		TaskType: reply.TaskType,
		TaskID:   reply.TaskID,
	}
	resp := TaskDoneReply{}

	_ = call("Coordinator.TaskDone", &args, &resp)
}

// 1. 根据传入的fileName 读出数据
// 2. 根据传入的mapf 得到word,count
// 3. 根据传入的NReducer 写入文件
// 4. send rpc call to notify coordinator task done

func ReadFromFile(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	return string(content)
}

func WriteToMiddleFile(data []KeyValue, filename string) {
	fw, _ := ioutil.TempFile("./", "middle-file-*.txt")
	enc := json.NewEncoder(fw)
	for _, kv := range data {
		_ = enc.Encode(&kv)
	}
	os.Rename(fw.Name(), filename)
	fw.Close()
}

func ReadFromMiddleFile(filename string) []KeyValue {
	fr, _ := os.Open(filename)
	dec := json.NewDecoder(fr)
	kvs := make([]KeyValue, 0)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kvs = append(kvs, kv)
	}
	return kvs
}

func (aw *AWorker) ExecMapTask(reply *AskTaskReply) {
	content := ReadFromFile(reply.MapTaskInfo.FileName)
	kvs := aw.Mapf(reply.MapTaskInfo.FileName, content)

	sort.SliceStable(kvs, func(i int, j int) bool {
		return kvs[i].Key < kvs[j].Key
	})
	for i := 0; i < reply.MapTaskInfo.NReducer; i++ {
		tmp := make([]KeyValue, 0)
		for _, v := range kvs {
			if ihash(v.Key)%reply.MapTaskInfo.NReducer == i {
				tmp = append(tmp, v)
			}
		}
		filename := GetMiddleFileName(reply.MapTaskInfo.FileIndex, i)
		WriteToMiddleFile(tmp, filename)
	}
	aw.CallTaskDone(reply)
}

// 1. 根据传入的ReducerID 读取数据
// 2. 将数据经过reducef 得到结果数组
// 3. 将结果数据写入文件
// 4. rpc 告知 Coordinator 任务完成

func (aw *AWorker) WriteToOutFile(data []KeyValue, ReduceIdx int) {
	filename := GetOutFileName(ReduceIdx)
	fw, _ := ioutil.TempFile("./", "out-tmp-*.txt")
	for i := 0; i < len(data); i++ {
		fmt.Fprintf(fw, "%s %s\n", data[i].Key, data[i].Value)
	}
	os.Rename(fw.Name(), filename)
	fw.Close()
}

func (aw *AWorker) ExecReduceTask(reply *AskTaskReply) {
	data := make([]KeyValue, 0)
	for i := 0; i < reply.ReducerTaskInfo.FileCount; i++ {
		filename := GetMiddleFileName(i, reply.ReducerTaskInfo.ReducerIndex)
		tmp := ReadFromMiddleFile(filename)
		data = append(data, tmp...)
	}

	sort.SliceStable(data, func(i, j int) bool {
		return data[i].Key < data[j].Key
	})

	ans := make([]KeyValue, 0)

	for i := 0; i < len(data); {
		j := i + 1
		for j < len(data) && data[i].Key == data[j].Key {
			j++
		}
		vals := make([]string, 0)
		for k := i; k < j; k++ {
			vals = append(vals, data[k].Value)
		}
		res := aw.Reducef(data[i].Key, vals)
		ans = append(ans, KeyValue{
			Key:   data[i].Key,
			Value: res,
		})
		i = j
	}

	aw.WriteToOutFile(ans, reply.ReducerTaskInfo.ReducerIndex)
	aw.CallTaskDone(reply)
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	worker = &AWorker{
		Mapf:     mapf,
		Reducef:  reducef,
		isDone:   false,
		WorkerID: GetUUID(),
	}
	log.Printf("workerID: %d \n", worker.WorkerID)
	worker.process()
}

// if the worker fails to contact the coordinator,
//it can assume that the coordinator has exited because the job is done, and so the worker can terminate too.

func (aw *AWorker) CallAskTask() {
	args := AskTaskReq{}
	reply := AskTaskReply{}

	ok := call("Coordinator.AskTask", &args, &reply)
	if ok {
		switch reply.TaskType {
		case TaskMapper:
			log.Printf("[Worker-%d] get mapper task.%v \n", aw.WorkerID, FormatStruct(reply))
			aw.ExecMapTask(&reply)
		case TaskReducer:
			log.Printf("[Worker-%d] get reducer task.%v \n", aw.WorkerID, FormatStruct(reply))
			aw.ExecReduceTask(&reply)
		case TaskWait:
			log.Printf("[Worker-%d] task wait \n", aw.WorkerID)
			time.Sleep(time.Second)
		case TaskEnd:
			log.Printf("[Worker-%d] 任务完成,worker 退出. \n", aw.WorkerID)
			// os.Exit(0)
			aw.isDone = true
		default:
			fmt.Println("发生未知错误")
			os.Exit(1)
		}
	} else {
		// 如果调用报错，假设 Coordinator 完成任务退出
		aw.isDone = true
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
