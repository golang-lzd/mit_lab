package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"log"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	StateMachine *FSM

	mu            sync.Mutex          // Lock to protect shared access to this peer's state
	peers         []*labrpc.ClientEnd // RPC end points of all peers
	persister     *Persister          // Object to hold this peer's persisted state
	me            int                 // this peer's index into peers[]
	dead          int32               // set by Kill()
	applyCh       chan ApplyMsg
	notifyApplyCh chan struct{}
	// Your data here (2A, 2B, 2C).
	ApplyMsgTimer *time.Timer //定时去应用日志

	ElectionTimeoutTimer *time.Timer // 超时之后就会触发选举,election timeout 是 从follower 变成 candidate 的时间，也是candidate等待投票结束的时间,leader 没有选举超时的概念

	HeartBeatTimoutTimer []*time.Timer // 超时之后就会触发发送心跳

	// persistent state on all servers
	CurrentTerm int       // 服务器已知最新的任期,初始化为0,线性增加
	VotedFor    int       // 当前任期内收到选票的candidatedID，初始为-1
	Log         []LogItem //log entries

	// volatile state on all servers
	CommitIndex int // 已知已提交的最高的日志条目的索引,初始化为0,单调递增
	LastApplied int // 已经被应用到状态机的最高的日志条目的索引,初始化为0,单调递增

	// volatile state on leaders
	// reinitialized after election
	NextIndex  []int // 对于每一台服务器,发送到该服务器的下一个日志条目的索引(初始值为领导人最后的日志条目的索引+1)
	MatchIndex []int // 对于每一台服务器,已知的已经复制到该服务器的最高日志条目的索引,(初始值为0，单调递增)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.CurrentTerm
	isleader = rf.StateMachine.GetState() == LeaderState
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	enc := labgob.NewEncoder(w)
	enc.Encode(rf.Log)
	enc.Encode(rf.CurrentTerm)
	enc.Encode(rf.VotedFor)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var (
		log         []LogItem
		currentTerm int
		voteFor     int
	)
	if d.Decode(&log) != nil || d.Decode(&currentTerm) != nil || d.Decode(voteFor) != nil {
		return
	}
	rf.Log = log
	rf.CurrentTerm = currentTerm
	rf.VotedFor = voteFor
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 候选人的任务号
	CandidatedID int // 请求选票的候选人的ID
	LastLogIndex int // 候选人的最后日志条目的索引
	LastLogTerm  int // 候选人最后日志条目的任期号
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 当前任期号，以便候选人去更新自己的任期号
	VoteGranted bool // 候选人赢得了此张选票时为真
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.CurrentTerm {
		// 如果接收到的 RPC 请求或响应中，任期号T > currentTerm，则令 currentTerm = T，并切换为跟随者状态
		tmp := rf.CurrentTerm
		rf.CurrentTerm = args.Term
		rf.StateMachine.SetState(FollowerState)
		rf.VotedFor = -1
		rf.ResetElectionTimeOut()
		rf.persist()
		log.Println(rf.WithState("Term: %d->%d 接收到 node-%d 较大Term,重置选举超时", tmp, args.Term, args.CandidatedID))
	}

	reply.Term = rf.CurrentTerm

	// 如果term < currentTerm返回 false
	if args.Term < rf.CurrentTerm {
		reply.VoteGranted = false
		return
	}

	if rf.StateMachine.GetState() == LeaderState {
		reply.VoteGranted = false
		return
	}

	// 如果 votedFor 为空或者为 candidateId，并且候选人的日志至少和自己一样新，那么就投票给他
	if rf.VotedFor == -1 || rf.VotedFor == args.CandidatedID {
		if (rf.Log[len(rf.Log)-1].Term == args.LastLogTerm && len(rf.Log)-1 <= args.LastLogIndex) || (rf.Log[len(rf.Log)-1].Term < args.LastLogTerm) {
			reply.VoteGranted = true
			rf.VotedFor = args.CandidatedID
			rf.persist()
			return
		} else {
			reply.VoteGranted = false
			return
		}
	} else {
		reply.VoteGranted = false
		return
	}
}

func (rf *Raft) sendRequestVoteToPeers() {
	var VoteGrantedCount int64 = 0
	var resCount int64 = 0
	ch := make(chan *RequestVoteReply, len(rf.peers)-1)
	rf.mu.Lock()

	LastLogTerm, LastLogIndex := rf.GetLastLogInfo()
	args := &RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidatedID: rf.me,
		LastLogTerm:  LastLogTerm,
		LastLogIndex: LastLogIndex,
	}
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(i, args, reply)
			ch <- reply
			if !ok {
				atomic.AddInt64(&resCount, 1)
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.CurrentTerm {
				rf.CurrentTerm = reply.Term
				// 当前raft 过期,假设 当前处于candidate
				rf.StateMachine.SetState(FollowerState)
				rf.ResetElectionTimeOut()
				rf.VotedFor = -1
				rf.persist()
			}

			if reply.VoteGranted && reply.Term == args.Term {
				log.Println(rf.WithState("收到 node-%d 的投票", i))
				atomic.AddInt64(&VoteGrantedCount, 1)
				atomic.AddInt64(&resCount, 1)
			} else {
				// 没有接收到 i 的投票
				atomic.AddInt64(&resCount, 1)
			}
		}(i)
	}

	// 当还没有收到半数同意的票的时候

	for int(atomic.LoadInt64(&VoteGrantedCount)+1) <= len(rf.peers)/2 {
		// 中间有可能出现状态转变
		if rf.StateMachine.GetState() != CandidateState {
			return
		}
		if int(atomic.LoadInt64(&resCount)) == len(rf.peers)-1 {
			break
		}
		if args.Term != rf.CurrentTerm {
			break
		}
	}

	// 收到了半数以上同意的票
	if int(atomic.LoadInt64(&VoteGrantedCount)+1) > len(rf.peers)/2 {
		log.Println(rf.WithState("收到半数以上票，成为leader"))
		if rf.StateMachine.GetState() == CandidateState && rf.CurrentTerm == args.Term {
			rf.StateMachine.SetState(LeaderState)
			return
		}

		// 成为leader 后需要更新的状态
		for i := 0; i < len(rf.peers); i++ {
			rf.mu.Lock()
			if i == rf.me {
				rf.NextIndex[i] = len(rf.Log)
				rf.MatchIndex[i] = len(rf.Log) - 1
				rf.mu.Unlock()
				continue
			}
			rf.NextIndex[i] = len(rf.Log)
			rf.MatchIndex[i] = 0
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	t := time.NewTimer(RPCTimeOut)
	ch := make(chan bool, 1)

	go func() {
		for i := 0; i < 10; i++ {
			ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
			if !ok {
				continue
			} else {
				ch <- ok
				return
			}
		}
	}()
	select {
	case <-t.C:
		return false
	case <-ch:
		return true
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.StateMachine.GetState() != LeaderState {
		return -1, rf.CurrentTerm, false
	}

	rf.Log = append(rf.Log, LogItem{
		Term:    rf.CurrentTerm,
		Command: command,
	})
	rf.persist()
	rf.NextIndex[rf.me] = len(rf.Log)
	rf.MatchIndex[rf.me] = len(rf.Log) - 1

	for i := 0; i < len(rf.peers); i++ {
		rf.ResetHeartBeatTimeOutZeros(i)
	}
	index = len(rf.Log) - 1
	term = rf.CurrentTerm

	log.Println(rf.WithState("收到命令，%v", command))
	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	// 定时应用日志
	go func() {
		for rf.killed() == false {
			select {
			case <-rf.ApplyMsgTimer.C:
				rf.notifyApplyCh <- struct{}{}
				rf.ResetApplyMsgTimeOut()
			case <-rf.notifyApplyCh:
				rf.StartApplyLogs()
			}
		}
	}()
	// 定时监听选举超时
	go func() {
		for rf.killed() == false {
			select {
			case <-rf.ElectionTimeoutTimer.C:
				// 选举超时，开始触发选举
				// 这里将leader和 candidate,follower 一起处理

				// command list:
				// 1. 首先清空计时器
				// 2. 判断当前状态是follower or candidate
				// 3. 更新Term 和 状态
				// 4. 开始请求投票流程:
				// 4-1. 首先给自己投票
				// 4-2 	如果接收到大多数服务器的选票，那么就变成领导人
				//		如果接收到来自新的领导人的附加日志（AppendEntries）RPC，则转变成跟随者 => 检测状态是否发生了变化
				//		如果选举过程超时，则再次发起一轮选举 => 上一轮的选举此时大概率已经结束返回，因为设定了RPC Timeout 为100ms,通过并发保证了总处理时间小于300ms
				rf.StartElection()
			}
		}
	}()

	// 定时发送心跳
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {

			for rf.killed() == false {
				select {
				case <-rf.HeartBeatTimoutTimer[i].C:
					// command list:
					// 1. 首先清空计时器
					// 2. 判断当前状态是否是leader ?

					rf.SendAppendEntriesToPeers(i)
				}
			}
		}(i)
	}
}

// the service or tester wants to create a Raft server.
// persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	// Your initialization code here (2A, 2B, 2C).
	rf.StateMachine = NewFSM(FollowerState)
	rf.ApplyMsgTimer = time.NewTimer(ApplyMsgTimeOut)
	rf.ElectionTimeoutTimer = time.NewTimer(GetElectionTimeOut())
	rf.HeartBeatTimoutTimer = make([]*time.Timer, 0)
	for i := 0; i < len(peers); i++ {
		rf.HeartBeatTimoutTimer = append(rf.HeartBeatTimoutTimer, time.NewTimer(HeartBeatTimeOut))
	}
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.Log = []LogItem{{Term: 0, Command: "None"}}
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.NextIndex = make([]int, 0)
	rf.MatchIndex = make([]int, 0)
	for i := 0; i < len(peers); i++ {
		rf.NextIndex = append(rf.NextIndex, 1)
		rf.MatchIndex = append(rf.MatchIndex, 0)
	}

	//rf.applyCh = make(chan ApplyMsg, 100)
	rf.notifyApplyCh = make(chan struct{}, 100)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
