package raft

import (
	"sync"
)

// 状态机实现 raft 状态转移

type FSMState string

type FSM struct {
	mu    sync.Mutex
	state FSMState
}

func (f *FSM) GetState() FSMState {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.state
}

func (f *FSM) SetState(newState FSMState) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.state = newState
}

// 实例化FSM
func NewFSM(initState FSMState) *FSM {
	return &FSM{
		state: initState,
	}
}

// state
var (
	InitializedState = FSMState("initialized")
	LeaderState      = FSMState("leader")
	CandidateState   = FSMState("candidate")
	FollowerState    = FSMState("follower")
)
