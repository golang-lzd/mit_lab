package kvraft

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"log"
	"math/big"
	"time"
)

const (
	OK                = "OK"
	ErrNoKey          = "ErrNoKey"
	ErrWrongLeader    = "ErrWrongLeader"
	ErrServer         = "ErrServer"
	ErrCommandTimeOut = "ErrCommandTimeOut"
)

const (
	WaitCommandTimeOut   = 500 * time.Millisecond
	ChangeLeaderInterval = 20 * time.Millisecond
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"

	ClientID  int64
	CommandID int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string

	ClientID  int64
	CommandID int64
}

type GetReply struct {
	Err   Err
	Value string
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

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}
