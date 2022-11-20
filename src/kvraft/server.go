package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"sync"
	"sync/atomic"
)

type Op struct {
	ReqID int64

	Method    string //"GET" or "PUT" or "Append"
	Key       string
	Value     string
	ClientID  int64
	CommandID int64
}

type CommandResult struct {
	ERR   Err
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	stopCh  chan bool

	maxraftstate      int // snapshot if log grows this big
	data              map[string]string
	notifyWaitCommand map[int64]chan CommandResult
	lastApplied       map[int64]int64
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	command := Op{
		ReqID:     nrand(),
		Method:    "GET",
		Key:       args.Key,
		ClientID:  args.ClientID,
		CommandID: args.CommandID,
	}
	kv.notifyWaitCommand[command.ReqID] = make(chan CommandResult)
	defer delete(kv.notifyWaitCommand, command.ReqID)

	_, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	select {
	case res := <-kv.notifyWaitCommand[command.ReqID]:
		reply.Value = res.Value
		reply.Err = res.ERR
		return
	case <-kv.stopCh:
		// 不该返回rpc 请求
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	command := Op{
		ReqID:     nrand(),
		Method:    args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientID:  args.ClientID,
		CommandID: args.CommandID,
	}
	kv.notifyWaitCommand[command.ReqID] = make(chan CommandResult)
	defer delete(kv.notifyWaitCommand, command.ReqID)

	_, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	select {
	case res := <-kv.notifyWaitCommand[command.ReqID]:
		reply.Err = res.ERR
		return
	case <-kv.stopCh:
		// 不该返回rpc 请求
		return
	}
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	close(kv.stopCh)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.stopCh = make(chan bool)
	kv.lastApplied = make(map[int64]int64)
	kv.notifyWaitCommand = make(map[int64]chan CommandResult)
	kv.data = make(map[string]string)
	// You may need initialization code here.

	go kv.HandleApplyMsg()
	return kv
}
