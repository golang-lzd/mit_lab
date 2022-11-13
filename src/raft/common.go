package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
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

func FormatStruct(s interface{}) string {
	bs, err := json.Marshal(s)
	if err != nil {
		log.Println(err.Error())
	}
	var out bytes.Buffer
	json.Indent(&out, bs, "", "\t")
	return out.String()
}

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
