package raft

import (
	"math/rand"
	"time"
)

// 选举超时时间的设定
// 实验要求：Leader 发送心跳检测 RPC 的频率不超过 10 次/ 秒。=> HeartBeatTimeout > 100ms
// 根据论文5.2要求，150ms <=ElectionTimeOut <=300ms;HeartBeatTimeout<150ms

// 因此可以得到 100ms<HeartBeatTimeout<150ms
// 150ms <=ElectionTimeOut <=300ms

const (
	ElectionTimeOut  = 300 * time.Millisecond
	HeartBeatTimeOut = 150 * time.Millisecond
	RPCTimeOut       = 100 * time.Millisecond
	ApplyMsgTimeOut  = 50 * time.Millisecond
)

func GetElectionTimeOut() time.Duration {
	return ElectionTimeOut + time.Duration(rand.Int())%(30*time.Millisecond)
}
