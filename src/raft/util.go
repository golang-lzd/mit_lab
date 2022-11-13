package raft

import (
	"log"
	"time"
)

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

type InstallSnapShotArgs struct {
	Term              int //current leader term
	LeaderID          int
	LastIncludedIndex int    // 快照中包含的最后日志条目的索引值
	LastIncludedTerm  int    // 快照中包含的最后日志条目的任期号
	Offset            int    // 分块在快照中字节偏移量
	Data              []byte // 从快照开始的快照分块的原始字节
	Done              bool   // 是否是最后一个分块，实现时不分块，即 always set Done=true
}

type InstallSnapShotReply struct {
	Term int
}

func (rf *Raft) GetStoreIndexByLogIndex(index int) int {
	//TODO
	return index
}

func (rf *Raft) GetLastLogTermAndIndex() (int, int) {
	//TODO
	return 0, 0
}

func (rf *Raft) SendInstallSnapShotToPeer(server int) {
	rf.mu.Lock()
	args := &InstallSnapShotArgs{
		Term:              rf.CurrentTerm,
		LeaderID:          rf.me,
		LastIncludedIndex: rf.lastSnapShotIndex,
		LastIncludedTerm:  rf.lastSnapShotTerm,
		Offset:            0,
		Data:              rf.persister.ReadSnapshot(), //TODO
		Done:              true,
	}

	rf.mu.Unlock()
	reply := &InstallSnapShotReply{}
	ok := rf.SendInstallSnapShot(server, args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.CurrentTerm {
		log.Println(rf.WithState("收到 node-%d 心跳响应,重置Term", server))
		rf.CurrentTerm = reply.Term
		rf.StateMachine.SetState(FollowerState)
		rf.VotedFor = -1
		rf.ResetElectionTimeOut()
		rf.persist()
		return
	}
	//  根据https://thesquareplanet.com/blog/students-guide-to-raft/#the-importance-of-details
	// 所说，当args.Term 和 currentTerm 不相等时，不应该再做后续的处理
	if rf.StateMachine.GetState() != LeaderState || args.Term != rf.CurrentTerm {
		return
	}

	// 更新matchIndex ,nextIndex
	rf.MatchIndex[server] = Max(rf.MatchIndex[server], rf.lastSnapShotIndex)
	rf.NextIndex[server] = Max(rf.NextIndex[server], rf.lastSnapShotIndex+1)

}

func (rf *Raft) SendInstallSnapShot(server int, args *InstallSnapShotArgs, reply *InstallSnapShotReply) bool {
	// TODO 是否改成一直发送直到能送达follower
	t := time.NewTimer(RPCTimeOut)
	ch := make(chan bool, 1)

	go func() {
		ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
		ch <- ok
	}()

	select {
	case <-t.C:
		return false
	case res := <-ch:
		return res
	}
}

func (rf *Raft) InstallSnapShot(args *InstallSnapShotArgs, reply *InstallSnapShotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.CurrentTerm {
		return
	} else if args.Term > rf.CurrentTerm || rf.StateMachine.GetState() != FollowerState { // 这里注意：当状态不是Follower 时也要重置
		rf.CurrentTerm = args.Term
		rf.StateMachine.SetState(FollowerState)
		rf.ResetElectionTimeOut()
		rf.VotedFor = -1
		rf.persist()
	}

	reply.Term = rf.CurrentTerm

	if rf.lastSnapShotIndex >= args.LastIncludedIndex {
		return
	}
	rf.applyCh <- ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}
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
	rf.mu.Lock()
	//log.Println(rf.WithState("心跳超时，开始发送心跳给所有peer-%d \n", server))
	preIndex := rf.NextIndex[server] - 1
	//log.Println(rf.WithState("server:%d len(log):%d preIndex:%d rf.NextIndex[server]:%d", server, len(rf.Log), preIndex, rf.NextIndex[server]))
	args := &AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: preIndex,
		PrevLogTerm:  rf.Log[preIndex].Term,
		Entries:      rf.GetSendEntriesDeepCopy(preIndex),
		LeaderCommit: rf.CommitIndex,
	}

	if rf.StateMachine.GetState() != LeaderState || server == rf.me {
		rf.mu.Unlock()
		return
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
		log.Println(rf.WithState("收到 node-%d 心跳响应,重置Term", server))
		rf.CurrentTerm = reply.Term
		rf.StateMachine.SetState(FollowerState)
		rf.ResetElectionTimeOut()
		rf.persist()
		return
	}
	//  根据https://thesquareplanet.com/blog/students-guide-to-raft/#the-importance-of-details
	// 所说，当args.Term 和 currentTerm 不相等时，不应该再做后续的处理
	if rf.StateMachine.GetState() != LeaderState || args.Term != rf.CurrentTerm {
		return
	}

	log.Println(rf.WithState("收到 node-%d 心跳响应,响应状态:%t", server, reply.Success))
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
		rf.ResetElectionTimeOut()
		rf.VotedFor = -1
	}

	defer rf.persist()
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
	rf.persist()
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
	rf.mu.Lock()
	rf.ResetElectionTimeOut()
	if rf.StateMachine.GetState() == LeaderState {
		rf.mu.Unlock()
		return
	}
	// 开始选举
	// 如果是Follower -> candidate,
	// 如果是candidate -> candidate
	rf.CurrentTerm++
	rf.StateMachine.SetState(CandidateState)
	rf.VotedFor = rf.me
	rf.persist()
	log.Println(rf.WithState("选举超时,开始执行startElection \n"))
	rf.mu.Unlock()
	rf.sendRequestVoteToPeers()
}
