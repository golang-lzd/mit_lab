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

func (rf *Raft) ResetHeartBeatTimeOut(server int) {
	rf.HeartBeatTimoutTimer[server].Stop()
	rf.HeartBeatTimoutTimer[server].Reset(HeartBeatTimeOut)
}

func (rf *Raft) SendAppendEntriesToPeers(server int) {
	rf.ResetHeartBeatTimeOut(server)
	if rf.StateMachine.GetState() != LeaderState {
		return
	}

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
	if ok {
		if reply.Term > rf.CurrentTerm {
			rf.mu.Lock()
			rf.CurrentTerm = reply.Term
			rf.mu.Unlock()
			rf.StateMachine.SetState(FollowerState)
		}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.CurrentTerm

	if args.Term < rf.CurrentTerm {
		reply.Success = false
		return
	} else if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.StateMachine.SetState(FollowerState)
	}
	// 心跳起作用
	rf.ResetElectionTimeOut()

	if len(rf.Log)-1 >= args.PrevLogIndex && rf.Log[args.PrevLogIndex].Term == args.PrevLogTerm {
		// 根据raft 原理, 可知 prevLogIndex 之前的所有日志都能匹配上
		// 新日志的开始和结束索引
		left := args.PrevLogIndex + 1
		right := args.PrevLogIndex + len(args.Entries)
		i := left
		ok := true
		for i = left; i <= right; i++ {
			if i >= len(rf.Log) {
				break
			}
			if rf.Log[i].Term == args.Entries[i-left].Term {
				continue
			} else {
				ok = false
				break
			}
		}
		if !ok {
			// 如果存在冲突条目，删除冲突条目及其后的所有日志
			rf.Log = rf.Log[:i]
			for j := i; j <= right; j++ {
				rf.Log = append(rf.Log, args.Entries[j-left])
			}
		} else if i >= len(rf.Log) {
			// 追加日志中尚未存在的任何新条目
			for j := i; j <= right; j++ {
				rf.Log = append(rf.Log, args.Entries[j-left])
			}
		}

		// 上一个新条目的索引
		// 假设冲突了 len(rf.Log)-1
		// 没冲突并且后边有旧日志 i-1
		// 没冲突并且后边没有旧日志 len(rf.Log)-1
		if args.LeaderCommit > rf.CommitIndex {
			if ok && i > right {
				rf.CommitIndex = Min(args.LeaderCommit, i-1)
			} else {
				rf.CommitIndex = Min(args.LeaderCommit, len(rf.Log)-1)
			}
		}
		reply.Success = true
		return
	} else {
		reply.Success = false
		return
	}
}

func (rf *Raft) GetLastLogInfo() (LastLogTerm int, LastLogIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.ElectionTimeoutTimer.Stop()
	rf.ElectionTimeoutTimer.Reset(ElectionTimeOut)
}

func (rf *Raft) StartElection() {
	rf.ResetElectionTimeOut()
	if _, isLeader := rf.GetState(); isLeader {
		return
	}
	// 开始选举
	rf.CurrentTerm++
	rf.StateMachine.SetState(CandidateState)
	rf.sendRequestVoteToPeers()
}
