package shardkv

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
	"sync"
	"time"
)

type Op struct {
	ReqID int64

	Method    string
	Key       string
	Value     string
	ClientID  int64
	CommandID int64

	ConfigNum int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	sm           *shardctrler.Clerk
	persister    *raft.Persister
	// Your definitions here.
	stopCh            chan struct{}
	lastApplied       [shardctrler.NShards]map[int64]int64
	notifyWaitCommand map[int64]chan CommandResult
	data              [shardctrler.NShards]map[string]string

	// shards
	meShards     map[int]bool
	inputShards  map[int]bool
	outputShards map[int]map[int]MergeShardData //configNum -> shardID: ShardData(lastApplied+data),ShardID,configNum

	//config
	config    shardctrler.Config
	oldConfig shardctrler.Config

	//timer
	PullConfigTimer *time.Timer
	PullShardsTime  *time.Timer
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	res := kv.WaitCmdExec("Get", args.Key, "", args.ClientID, args.CommandID, args.ConfigNum)
	reply.Err = res.Err
	reply.Value = res.Value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	res := kv.WaitCmdExec(args.Op, args.Key, args.Value, args.ClientID, args.CommandID, args.ConfigNum)
	reply.Err = res.Err
}

type CommandResult struct {
	Err
	Value string
}

func (kv *ShardKV) WaitCmdExec(method, Key, Value string, clientID, commandID int64, configNum int) (res CommandResult) {
	kv.mu.Lock()
	command := Op{
		ReqID:     nrand(),
		Method:    method,
		Key:       Key,
		Value:     Value,
		ClientID:  clientID,
		CommandID: commandID,
		ConfigNum: configNum,
	}
	kv.notifyWaitCommand[command.ReqID] = make(chan CommandResult, 1)
	kv.mu.Unlock()
	defer kv.RemoveNotifyCommandCh(command.ReqID)

	_, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		res.Err = ErrWrongLeader
	}

	t := time.NewTimer(WaitCommandTimeOut)
	select {
	case <-kv.stopCh:
		res.Err = ErrServer
	case <-t.C:
		res.Err = ErrWaitCommandTimeOut
	case result := <-kv.notifyWaitCommand[command.ReqID]:
		res.Err = result.Err
		res.Value = result.Value
	}
	return res
}

func (kv *ShardKV) RemoveNotifyCommandCh(reqID int64) {
	kv.mu.Lock()
	delete(kv.notifyWaitCommand, reqID)
	kv.mu.Unlock()
}

func (kv *ShardKV) NotifyWaitCommand(reqID int64, err Err, value string) {
	if ch, ok := kv.notifyWaitCommand[reqID]; ok {
		ch <- CommandResult{
			Err:   err,
			Value: value,
		}
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	close(kv.stopCh)
	// Your code here, if desired.
}

func (kv *ShardKV) ticker() {
	go kv.HandleApplyMsg()

	go kv.pullConfig()

	go kv.FetchShards()
}

func (kv *ShardKV) pullConfig() {
	for {
		select {
		case <-kv.stopCh:
			return
		case <-kv.PullConfigTimer.C:
			if _, isLeader := kv.rf.GetState(); !isLeader {
				kv.ResetPullConfigTimer()
				continue
			}
			kv.mu.Lock()
			lastNum := kv.config.Num
			kv.mu.Unlock()

			conf := kv.sm.Query(lastNum + 1)
			if conf.Num == lastNum+1 {
				kv.mu.Lock()
				if len(kv.inputShards) == 0 && conf.Num == kv.config.Num+1 {
					kv.mu.Unlock()
					kv.rf.Start(conf)
				} else {
					kv.mu.Unlock()
				}
			}
			kv.ResetPullConfigTimer()
		}
	}
}

// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.persister = persister

	// handle op
	kv.stopCh = make(chan struct{})
	for i := 0; i < shardctrler.NShards; i++ {
		kv.lastApplied[i] = make(map[int64]int64)
		kv.data[i] = make(map[string]string)
	}

	kv.notifyWaitCommand = make(map[int64]chan CommandResult)

	// shard
	kv.meShards = make(map[int]bool)
	kv.inputShards = make(map[int]bool)
	kv.outputShards = make(map[int]map[int]MergeShardData)

	// timer
	kv.PullConfigTimer = time.NewTimer(PullConfigTimeOut)
	kv.PullShardsTime = time.NewTimer(PullShardsTimeOut)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.sm = shardctrler.MakeClerk(ctrlers)
	kv.config = shardctrler.Config{
		Num:    0,
		Shards: [10]int{},
		Groups: make(map[int][]string),
	}
	kv.oldConfig = shardctrler.Config{
		Num:    0,
		Shards: [10]int{},
		Groups: make(map[int][]string),
	}

	return kv
}
