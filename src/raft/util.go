package raft

import (
	"fmt"
	"log"
	"time"
)

func (rf *Raft) WithState(format string, a ...interface{}) string {
	_s := fmt.Sprintf(format, a...)
	return fmt.Sprintf("[Term-%d Raft-%d VoteFor-%d CommitIndex-%d] %s", rf.CurrentTerm, rf.me, rf.VotedFor, rf.CommitIndex, _s)
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
	//
	NextLogIndex int
	NextLogTerm  int
}

func (rf *Raft) ResetApplyMsgTimeOut() {
	rf.ApplyMsgTimer.Stop()
	rf.ApplyMsgTimer.Reset(ApplyMsgTimeOut)
}

func (rf *Raft) ResetHeartBeatTimeOutZeros(server int) {
	rf.HeartBeatTimoutTimer[server].Stop()
	rf.HeartBeatTimoutTimer[server].Reset(0 * time.Second)
}

func (rf *Raft) ResetHeartBeatTimeOut(server int) {
	rf.HeartBeatTimoutTimer[server].Stop()
	rf.HeartBeatTimoutTimer[server].Reset(HeartBeatTimeOut)
}

func (rf *Raft) GetSendEntriesDeepCopy(preIndex int) []LogItem {
	res := make([]LogItem, 0)
	for i := preIndex + 1; i < len(rf.Log); i++ {
		res = append(res, rf.Log[i])
	}
	return res
}

func (rf *Raft) SendAppendEntriesToPeers(server int) {
	rf.ResetHeartBeatTimeOut(server)
	if rf.StateMachine.GetState() != LeaderState || server == rf.me {
		return
	}
	rf.mu.Lock()
	//log.Println(rf.WithState("心跳超时，开始发送心跳给所有peer-%d \n", server))
	preIndex := rf.NextIndex[server] - 1
	rf.mu.Unlock()

	rf.mu.Lock()
	//log.Println(rf.WithState("server:%d len(log):%d preIndex:%d rf.NextIndex[server]:%d", server, len(rf.Log), preIndex, rf.NextIndex[server]))
	args := &AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: preIndex,
		PrevLogTerm:  rf.Log[preIndex].Term,
		Entries:      rf.GetSendEntriesDeepCopy(preIndex),
		LeaderCommit: rf.CommitIndex,
	}
	rf.mu.Unlock()
	log.Println(rf.WithState("心跳超时，开始发送心跳给所有peer-%d 参数为:%v\n", server, FormatStruct(args)))
	reply := &AppendEntriesReply{}
	ok := rf.SendAppendEntries(server, args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果接收到的RPC 的请求和响应中，任期号T>currentTerm ，则令currentTerm = T,并切换为追随者状态
	if reply.Term > rf.CurrentTerm {
		log.Println(rf.WithState("收到心跳响应,重置Term"))
		rf.CurrentTerm = reply.Term
		rf.StateMachine.SetState(FollowerState)
		rf.ResetElectionTimeOut()
		return
	}
	//  根据https://thesquareplanet.com/blog/students-guide-to-raft/#the-importance-of-details
	// 所说，当args.Term 和 currentTerm 不相等时，不应该再做后续的处理
	if rf.StateMachine.GetState() != LeaderState || args.Term != rf.CurrentTerm {
		return
	}

	log.Println(rf.WithState("收到心跳响应,响应状态:%t", reply.Success))
	if !reply.Success {
		if reply.NextLogIndex != 0 {
			rf.NextIndex[server] = reply.NextLogIndex
			rf.ResetHeartBeatTimeOutZeros(server)
		}
	} else {
		// 更新nextIndex,matchIndex
		rf.NextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		rf.MatchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.TryCommit()
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
	defer func() {
		rf.mu.Unlock()
	}()

	reply.Term = rf.CurrentTerm

	if args.Term < rf.CurrentTerm {
		reply.Success = false
		reply.NextLogIndex = 1
		return
	} else if args.Term >= rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.StateMachine.SetState(FollowerState)
		rf.VotedFor = -1
	}
	// 心跳起作用
	rf.ResetElectionTimeOut()

	if len(rf.Log)-1 >= args.PrevLogIndex && rf.Log[args.PrevLogIndex].Term == args.PrevLogTerm {
		// 单纯的心跳
		if len(args.Entries) == 0 {
			if rf.CommitIndex < args.LeaderCommit {
				// 这里可以推断: 上一个新日志的索引为prevLogIndex,当len(entries)=0时，prevLogIndex 就是leader 的最后一个日志记录
				// 所以 leaderCommit 一定小于等于prevLogIndex.
				rf.CommitIndex = args.LeaderCommit
				rf.notifyApplyCh <- struct{}{}
			}
			reply.NextLogIndex = args.PrevLogIndex + 1
			reply.Success = true
			return
		} else {
			if len(args.Entries)+args.PrevLogIndex >= len(rf.Log)-1 {
				rf.Log = append(rf.Log[:args.PrevLogIndex+1], args.Entries...)
				if rf.CommitIndex < args.LeaderCommit {
					// 这里可以推断: 上一个新日志的索引为prevLogIndex,当len(entries)=0时，prevLogIndex 就是leader 的最后一个日志记录
					// 所以 leaderCommit 一定小于等于prevLogIndex.
					rf.CommitIndex = args.LeaderCommit
					rf.notifyApplyCh <- struct{}{}
				}
				reply.NextLogIndex = args.PrevLogIndex + len(args.Entries) + 1
				reply.Success = true
				return
			} else {
				// 根据raft 原理, 可知 prevLogIndex 之前的所有日志都能匹配上
				// 新日志的开始和结束索引
				left := args.PrevLogIndex + 1
				right := args.PrevLogIndex + len(args.Entries)
				i := left
				ok := true
				for i = left; i <= right; i++ {
					if rf.Log[i].Term == args.Entries[i-left].Term {
						continue
					} else {
						ok = false
						break
					}
				}
				if !ok {
					// 如果存在冲突条目，删除冲突条目及其后的所有日志
					rf.Log = append(rf.Log[:args.PrevLogIndex+1], args.Entries...)
				}
				// 上一个新条目的索引
				// 假设冲突了 len(rf.Log)-1
				// 没冲突并且后边有旧日志 i-1
				// 没冲突并且后边没有旧日志 len(rf.Log)-1
				if args.LeaderCommit > rf.CommitIndex {
					rf.CommitIndex = Min(right, args.LeaderCommit)
					rf.notifyApplyCh <- struct{}{}
				}
				//log.Println(rf.WithState("当前日志为:%v", FormatStruct(rf.Log)))
				reply.Success = true
				reply.NextLogIndex = args.PrevLogIndex + len(args.Entries) + 1
				return
			}

		}

	} else {
		// 假设日志不符合
		reply.Success = false
		if len(rf.Log)-1 < args.PrevLogIndex {
			// 日志不够
			reply.NextLogIndex = len(rf.Log)
			return
		} else {
			// 日志 Term 不符合,每次-1
			reply.NextLogIndex = args.PrevLogIndex
			return
		}
	}
}

func (rf *Raft) GetLastLogInfo() (LastLogTerm int, LastLogIndex int) {
	LastLogTerm = rf.Log[len(rf.Log)-1].Term
	LastLogIndex = len(rf.Log) - 1
	return
}

// 如果commitIndex > lastApplied，则 lastApplied 递增，并将log[lastApplied]应用到状态机中

func (rf *Raft) StartApplyLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := rf.LastApplied; i <= rf.CommitIndex; i++ {
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.Log[i].Command,
			CommandIndex: i,
		}
		rf.applyCh <- applyMsg
	}
	rf.LastApplied = rf.CommitIndex
}

func (rf *Raft) TryCommit() {
	preCommitIndex := rf.CommitIndex
	for i := rf.CommitIndex + 1; i < len(rf.Log); i++ {
		// 假设存在 N 满足N > commitIndex，使得大多数的 matchIndex[i] ≥ N以及log[N].term == currentTerm 成立，则令 commitIndex = N
		if rf.Log[i].Term != rf.CurrentTerm {
			continue
		}
		count := 0
		for j := 0; j < len(rf.peers); j++ {
			if i <= rf.MatchIndex[j] {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.CommitIndex = i
		}
	}
	if rf.CommitIndex > preCommitIndex {
		rf.notifyApplyCh <- struct{}{}
	}

}
func (rf *Raft) ResetElectionTimeOutZeros() {
	rf.ElectionTimeoutTimer.Stop()
	rf.ElectionTimeoutTimer.Reset(0 * time.Second)
}

func (rf *Raft) ResetElectionTimeOut() {
	rf.ElectionTimeoutTimer.Stop()
	rf.ElectionTimeoutTimer.Reset(GetElectionTimeOut())
}

func (rf *Raft) StartElection() {
	rf.ResetElectionTimeOut()
	if _, isLeader := rf.GetState(); isLeader {
		return
	}
	rf.mu.Lock()
	// 开始选举
	// 如果是Follower -> candidate,
	// 如果是candidate -> candidate
	rf.CurrentTerm++
	rf.StateMachine.SetState(CandidateState)
	rf.VotedFor = rf.me
	log.Println(rf.WithState("选举超时,开始执行startElection \n"))
	rf.mu.Unlock()
	rf.sendRequestVoteToPeers()
}
