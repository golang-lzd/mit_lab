package shardctrler

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"sync"
	"time"
)

type ShardCtrler struct {
	mu            sync.Mutex
	me            int
	rf            *raft.Raft
	applyCh       chan raft.ApplyMsg
	notifyWaitCmd map[int64]chan CommandResult
	stopCh        chan struct{}
	lastApplied   map[int64]int64
	persister     *raft.Persister
	// Your data here.

	configs []Config // indexed by config num
}

type CommandResult struct {
	Err
	Config
}

type Op struct {
	// Your data here.
	ReqID int64

	Method    string
	ClientID  int64
	CommandID int64
	Args      interface{}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	command := Op{
		ReqID:     nrand(),
		Method:    "Join",
		ClientID:  args.ClientID,
		CommandID: args.CommandID,
		Args:      *args,
	}
	res := sc.WaitCmd(&command)
	reply.Err = res.Err
	if res.Err == ErrWrongLeader {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	command := Op{
		ReqID:     nrand(),
		Method:    "Leave",
		ClientID:  args.ClientID,
		CommandID: args.CommandID,
		Args:      *args,
	}
	res := sc.WaitCmd(&command)
	reply.Err = res.Err
	if res.Err == ErrWrongLeader {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	command := Op{
		ReqID:     nrand(),
		Method:    "Move",
		ClientID:  args.ClientID,
		CommandID: args.CommandID,
		Args:      *args,
	}
	res := sc.WaitCmd(&command)
	reply.Err = res.Err
	if res.Err == ErrWrongLeader {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	if args.Num >= 0 && args.Num < len(sc.configs)-1 {
		reply.Err = OK
		reply.Config = sc.GetConfigByNum(args.Num)
		reply.WrongLeader = false
		return
	}

	command := Op{
		ReqID:     nrand(),
		Method:    "Query",
		ClientID:  args.ClientID,
		CommandID: args.CommandID,
		Args:      *args,
	}
	res := sc.WaitCmd(&command)
	reply.Err = res.Err
	if res.Err == ErrWrongLeader {
		reply.WrongLeader = true
	}
	reply.Config = res.Config
}

func (sc *ShardCtrler) WaitCmd(command *Op) (res CommandResult) {
	sc.mu.Lock()
	ch := make(chan CommandResult, 1)
	sc.notifyWaitCmd[command.ReqID] = ch
	sc.mu.Unlock()

	defer sc.RemoveNotifyWaitCommandCh(command.ReqID)

	_, _, isLeader := sc.rf.Start(*command)
	if !isLeader {
		res.Err = ErrWrongLeader
		return
	}
	t := time.NewTimer(WaitCommandTimeOut)
	select {
	case <-t.C:
		res.Err = ErrCommandTimeOut
	case result := <-ch:
		res.Err = result.Err
		res.Config = result.Config
	case <-sc.stopCh:
		res.Err = ErrServer
	}
	return res
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	close(sc.stopCh)
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	sc.stopCh = make(chan struct{})
	sc.lastApplied = make(map[int64]int64)
	sc.notifyWaitCmd = make(map[int64]chan CommandResult)

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.persister = persister

	// Your code here.

	go sc.HandleApplyMsg()
	return sc
}
