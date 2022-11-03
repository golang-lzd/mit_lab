package raft

import (
	"log"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func Min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func Max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

type LogItem struct {
	Command interface{}
	Term    int // 服务器收到这条命令时的任期
}

type AppendEntriesArgs struct {
	Term         int       // 领导人的任期
	LeaderID     int       // 领导人ID因此跟随者可以对客户端进行重定向
	PrevLogIndex int       // 紧邻新日志条目之前的那个日志条目的索引
	PrevLogTerm  int       // 紧邻新日志条目之前的那个日志条目的任期
	Entries      []LogItem // 需要被保存的日志条目(被当作心跳使用时，则日志条目内容为空
	LeaderCommit int       // 领导人的已知已提交的最高的日志条目的索引
}

type AppendEntriesReply struct {
	Term    int  // 当前任期，对于领导人而言，他会更新自己的任期
	Success bool // 如果跟随者所含有的条目和prevLogIndex 以及 prevLogTerm 匹配上了，则为 true
}

func (rf *Raft) SendAppendEntriesToPeers(server int) {
	preIndex := rf.NextIndex[server] - 1
	args := &AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: preIndex,
		PrevLogTerm:  rf.Log[preIndex].Term,
		Entries:      rf.Log[preIndex+1:],
		LeaderCommit: rf.CommitIndex,
	}
	reply := &AppendEntriesReply{}
	ok := rf.SendAppendEntries(server, args, reply)
	if !ok {

	}
}
func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	t := time.NewTimer(RPCTimeOut)
	ch := make(chan bool, 1)

	go func() {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		ch <- ok
	}()

	select {
	case <-t.C:
		return false
	case res := <-ch:
		return res
	}

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = rf.CurrentTerm

	if args.Term < rf.CurrentTerm {
		reply.Success = false
		return
	}
	if len(rf.Log)-1 >= args.PrevLogIndex && rf.Log[args.PrevLogIndex].Term == args.PrevLogTerm {
		// 根据raft 原理, 可知 prevLogIndex 之前的所有日志都能匹配上
		// 删除 preLogIndex 之后的所有日志
		rf.Log = rf.Log[:args.PrevLogIndex+1]
		i := 0
		for i < len(args.Entries) {
			rf.Log = append(rf.Log, args.Entries[i])
			i++
		}

		if args.LeaderCommit > rf.CommitIndex {
			rf.CommitIndex = Min(args.LeaderCommit, len(rf.Log)-1)
		}
		reply.Success = true
		return
	} else {
		reply.Success = false
		return
	}
}

func (rf *Raft) GetLastLogInfo() (LastLogTerm int, LastLogIndex int) {
	if len(rf.Log) == 1 {
		// 没有日志
		LastLogTerm = 0
		LastLogIndex = 0
		return
	} else {
		LastLogTerm = rf.Log[len(rf.Log)-1].Term
		LastLogIndex = len(rf.Log) - 1
		return
	}
}

func (rf *Raft) ResetElectionTimeOut() {
	rf.ElectionTimeoutTimer.Stop()
	rf.ElectionTimeoutTimer.Reset(ElectionTimeOut)
}

func (rf *Raft) StartElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.ResetElectionTimeOut()
	if _, isLeader := rf.GetState(); isLeader {
		return
	}
	// 开始选举
	rf.CurrentTerm++
	rf.StateMachine.SetState(CandidateState)
	rf.sendRequestVoteToPeers()
}
