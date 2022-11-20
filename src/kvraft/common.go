package kvraft

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
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

func (kv *KVServer) WithState(format string, a ...interface{}) string {
	_s := fmt.Sprintf(format, a...)
	return fmt.Sprintf("[] %s", _s)
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}
