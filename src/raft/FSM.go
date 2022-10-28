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

var (
	InitializedState = FSMState("initialized")
	LeaderState      = FSMState("leader")
	CandidateState   = FSMState("candidate")
	FollowerState    = FSMState("follower")
)

