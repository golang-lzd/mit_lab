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
	if index-rf.lastSnapShotIndex < 0 {
		return -1
	}
	return index - rf.lastSnapShotIndex
}

func (rf *Raft) GetLastLogTermAndIndex() (int, int) {
	return rf.Log[len(rf.Log)-1].Term, rf.lastSnapShotIndex + len(rf.Log) - 1
}

func (rf *Raft) IsOutArgsAppendEntries(args *AppendEntriesArgs) bool {
	argsLastLogIndex := args.PrevLogIndex + len(args.Entries)
	lastLogTerm, lastLogIndex := rf.GetLastLogTermAndIndex()
	if lastLogTerm == args.Term && argsLastLogIndex < lastLogIndex {
		return true
	}
	return false
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
	log.Println(rf.WithState("发送安装快照请求,%s", FormatStruct(args)))
	ok := rf.SendInstallSnapShot(server, args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.CurrentTerm {
		log.Println(rf.WithState("收到 node-%d 安装快照响应,重置Term", server))
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
	for i := 0; i < 10; i++ {
		// TODO 是否改成一直发送直到能送达follower
		t := time.NewTimer(RPCTimeOut)
		ch := make(chan bool, 1)

		go func() {
			ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
			ch <- ok
		}()

		select {
		case <-t.C:
			continue
		case res := <-ch:
			if !res {
				continue
			} else {
				return res
			}
		}
	}
	return false
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

func (rf *Raft) GetAppendLogs(server int) (preLogIndex int, preLogTerm int, logEntries []LogItem) {
	logEntries = make([]LogItem, 0)
	nextIndex := rf.NextIndex[server]
	// 当nextIndex在快照范围内时
	if nextIndex <= rf.lastSnapShotIndex {
		preLogIndex = rf.lastSnapShotIndex
		preLogTerm = rf.lastSnapShotTerm
		return
	}
	// 当nextIndex 大于最大索引时
	lastTerm, lastIndex := rf.GetLastLogTermAndIndex()
	if lastIndex < nextIndex {
		preLogTerm = lastTerm
		preLogIndex = lastIndex
		return
	}

	for i := nextIndex; i <= lastIndex; i++ {
		logEntries = append(logEntries, rf.Log[rf.GetStoreIndexByLogIndex(i)])
	}

	preLogIndex = nextIndex - 1
	if preLogIndex == rf.lastSnapShotIndex {
		preLogTerm = rf.lastSnapShotTerm
	} else {
		preLogTerm = rf.Log[rf.GetStoreIndexByLogIndex(preLogIndex)].Term
	}
	return
}

func (rf *Raft) SendAppendEntriesToPeers(server int) {
	rf.ResetHeartBeatTimeOut(server)
	rf.mu.Lock()
	preIndex, preTerm, logEntries := rf.GetAppendLogs(server)
	//log.Println(rf.WithState("server:%d len(log):%d preIndex:%d rf.NextIndex[server]:%d", server, len(rf.Log), preIndex, rf.NextIndex[server]))
	args := &AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: preIndex,
		PrevLogTerm:  preTerm,
		Entries:      logEntries,
		LeaderCommit: rf.CommitIndex,
	}

	if rf.StateMachine.GetState() != LeaderState || server == rf.me {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	if len(args.Entries) != 0 {
		log.Println(rf.WithState("心跳超时，开始发送心跳给所有peer-%d 参数为:%v\n", server, FormatStruct(args)))
	}
	reply := &AppendEntriesReply{}
	ok := rf.SendAppendEntries(server, args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
	}()
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

	if !reply.Success {
		if reply.NextLogIndex != 0 {
			if reply.NextLogIndex > rf.lastSnapShotIndex {
				rf.NextIndex[server] = reply.NextLogIndex
				rf.ResetHeartBeatTimeOutZeros(server)
			} else {
				go rf.SendInstallSnapShotToPeer(server)
			}
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
		for i := 0; i < 10; i++ {
			ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
			if !ok {
				time.Sleep(10 * time.Millisecond)
				continue
			} else {
				ch <- ok
				return
			}
		}
		//log.Println(rf.WithState("尝试十次失败，AppendEntries to %d", server))
	}()

	select {
	case <-t.C:
		log.Println(rf.WithState("AppendEntries to %d超时", server))
		return false
	case res := <-ch:
		return res
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//log.Println(rf.WithState("收到了心跳请求,%v", FormatStruct(args)))

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
	_, lastIndex := rf.GetLastLogTermAndIndex()

	// prevLogIndex 和 lastSnapShotIndex,lastIndex 的关系
	if args.PrevLogIndex < rf.lastSnapShotIndex {
		reply.Success = false
		reply.NextLogIndex = rf.lastSnapShotIndex + 1
	} else if args.PrevLogIndex > lastIndex {
		reply.Success = false
		reply.NextLogIndex = lastIndex + 1
	} else if args.PrevLogIndex == rf.lastSnapShotIndex {
		if !rf.IsOutArgsAppendEntries(args) {
			reply.Success = true
			rf.Log = append(rf.Log[:1], args.Entries...)
			_, currentIndex := rf.GetLastLogTermAndIndex()
			reply.NextLogIndex = currentIndex + 1
		} else {
			reply.Success = true
			reply.NextLogIndex = 0
		}
		return
	} else if args.PrevLogTerm == rf.Log[rf.GetStoreIndexByLogIndex(args.PrevLogIndex)].Term {
		if !rf.IsOutArgsAppendEntries(args) {
			reply.Success = true
			rf.Log = append(rf.Log[:rf.GetStoreIndexByLogIndex(args.PrevLogIndex)+1], args.Entries...)
			_, currentIndex := rf.GetLastLogTermAndIndex()
			reply.NextLogIndex = currentIndex
		} else {
			reply.Success = true
			reply.NextLogIndex = 0 // 当为0 时，不处理
		}
	} else {
		term := rf.Log[rf.GetStoreIndexByLogIndex(args.PrevLogIndex)].Term
		index := args.PrevLogIndex
		for index > rf.CommitIndex && index > rf.lastSnapShotIndex && rf.Log[rf.GetStoreIndexByLogIndex(index)].Term == term {
			index--
		}
		reply.Success = false
		reply.NextLogIndex = index + 1
	}
	// 有提交数据的话，此时可以保证args.LeaderCommit小于新日志的最大索引
	if reply.Success {
		if args.LeaderCommit > rf.CommitIndex {
			rf.CommitIndex = args.LeaderCommit
			rf.notifyApplyCh <- struct{}{}
		}
	}

}

// 如果commitIndex > lastApplied，则 lastApplied 递增，并将log[lastApplied]应用到状态机中

func (rf *Raft) StartApplyLogs() {
	var msgs []ApplyMsg
	rf.mu.Lock()

	if rf.LastApplied < rf.lastSnapShotIndex {
		rf.mu.Unlock()
		rf.CondInstallSnapshot(rf.lastSnapShotTerm, rf.lastSnapShotIndex, rf.persister.snapshot)
		return
	} else if rf.CommitIndex < rf.LastApplied {
		msgs = make([]ApplyMsg, 0)
	} else {
		msgs = make([]ApplyMsg, 0)
		for i := rf.LastApplied + 1; i <= rf.CommitIndex; i++ {
			msgs = append(msgs, ApplyMsg{
				CommandValid: true,
				Command:      rf.Log[rf.GetStoreIndexByLogIndex(i)].Command,
				CommandIndex: i,
			})
		}
	}
	rf.mu.Unlock()

	for _, msg := range msgs {
		rf.applyCh <- msg
		rf.mu.Lock()
		rf.LastApplied = msg.CommandIndex
		rf.mu.Unlock()
	}
}

func (rf *Raft) TryCommit() {
	preCommitIndex := rf.CommitIndex
	_, lastIndex := rf.GetLastLogTermAndIndex()
	for i := rf.CommitIndex + 1; i <= lastIndex; i++ {
		// 假设存在 N 满足N > commitIndex，使得大多数的 matchIndex[i] ≥ N以及log[N].term == currentTerm 成立，则令 commitIndex = N
		if rf.Log[rf.GetStoreIndexByLogIndex(i)].Term != rf.CurrentTerm {
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
