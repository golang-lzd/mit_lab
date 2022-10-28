package raft

import (
	"log"
	"sync"
)

type FSMState string
type FSMEvent string
type FSMHandler func() FSMState

type FSM struct {
	mu       sync.Mutex
	state    FSMState
	handlers map[FSMState]map[FSMEvent]FSMHandler
}

func (f *FSM) GetState() FSMState {
	return f.state
}

func (f *FSM) SetState(newState FSMState) {
	f.state = newState
}

func (f *FSM) AddHandler(state FSMState, event FSMEvent, handler FSMHandler) *FSM {
	if _, ok := f.handlers[state]; !ok {
		f.handlers[state] = make(map[FSMEvent]FSMHandler)
	}
	if _, ok := f.handlers[state][event]; ok {
		log.Printf("[警告] 状态(%s)事件(%s)已定义过", state, event)
	}
	f.handlers[state][event] = handler
	return f
}

func (f *FSM) Call(event FSMEvent) FSMState {
	f.mu.Lock()
	defer f.mu.Unlock()

	events := f.handlers[f.GetState()]
	if events == nil {
		return f.GetState()
	}
	if fn, ok := events[event]; ok {
		oldState := f.GetState()
		f.SetState(fn())
		newState := f.GetState()
		log.Println("状态从 [", oldState, "] 变成 [", newState, "]")
	}
	return f.GetState()
}

// 实例化FSM
func NewFSM(initState FSMState) *FSM {
	return &FSM{
		state:    initState,
		handlers: make(map[FSMState]map[FSMEvent]FSMHandler),
	}
}

// state
var (
	InitializedState = FSMState("initialized")
	LeaderState      = FSMState("leader")
	CandidateState   = FSMState("candidate")
	FollowerState    = FSMState("follower")
)

// event
var (
	StartsUpEvent                     = FSMEvent("starts up")
	TimeOutStartsElectionEvent        = FSMEvent("timeout starts election")
	TimeOutNewElectionEvent           = FSMEvent("timeout new election")
	ReceiveVotesFromMajorityOfServers = FSMEvent("receive votes from majority servers")
	DiscoversServerWithHigherTerm     = FSMEvent("discover server with higher term")
	DiscoverCurrentLeaderOrNewTerm    = FSMEvent("discover current leader or new term")
)

// handler

var (
	StartsUpHandler = FSMHandler(func() FSMState {
		return FollowerState
	})
	TimeOutStartsElectionHandler = FSMHandler(func() FSMState {
		return CandidateState
	})
	TimeOutNewElectionHandler = FSMHandler(func() FSMState {
		return CandidateState
	})
	ReceiveVotesFromMajoriryOfServersHandler = FSMHandler(func() FSMState {
		return LeaderState
	})
	DiscoversServerWithHigherTermHandler = FSMHandler(func() FSMState {
		return FollowerState
	})
	DiscoverCurrentLeaderOrNewTermHandler = FSMHandler(func() FSMState {
		return FollowerState
	})
)

func (f *FSM) Init() {
	// starts up ->Follower
	f.AddHandler(InitializedState, StartsUpEvent, StartsUpHandler)

	// Follower -> candidate
	f.AddHandler(FollowerState, TimeOutStartsElectionEvent, TimeOutStartsElectionHandler)

	// Candidate
	f.AddHandler(CandidateState, TimeOutNewElectionEvent, TimeOutNewElectionHandler)
	f.AddHandler(CandidateState, ReceiveVotesFromMajorityOfServers, ReceiveVotesFromMajoriryOfServersHandler)
	f.AddHandler(CandidateState, DiscoverCurrentLeaderOrNewTerm, DiscoverCurrentLeaderOrNewTermHandler)

	// leader
	f.AddHandler(LeaderState, DiscoversServerWithHigherTerm, DiscoversServerWithHigherTermHandler)
}
