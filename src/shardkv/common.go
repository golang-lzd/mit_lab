package shardkv

import "time"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                    = "OK"
	ErrNoKey              = "ErrNoKey"
	ErrWrongGroup         = "ErrWrongGroup"
	ErrWrongLeader        = "ErrWrongLeader"
	ErrServer             = "ErrServer"
	ErrWaitCommandTimeOut = "ErrWaitCommandTimeOut"
)

type Err string

const (
	WaitCommandTimeOut    = 500 * time.Millisecond
	PullConfigTimeOut     = 100 * time.Millisecond
	PullShardsTimeOut     = 200 * time.Millisecond
	SendFetchShardTimeOut = 500 * time.Millisecond
	SendCleanShardTimeOut = 500 * time.Millisecond
	ChangeLeaderInternal  = 20 * time.Millisecond
)

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"

	ClientID  int64
	CommandID int64
	ConfigNum int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID  int64
	CommandID int64
	ConfigNum int
}

type GetReply struct {
	Err   Err
	Value string
}
