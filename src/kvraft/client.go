package kvraft

import (
	"6.824/labrpc"
	"log"
)
import "crypto/rand"
import "math/big"

// 假设每一个clerk 都不会并发执行多个请求,可以通过map[clientID]commandID 记录每一个client的最后一个command.
//

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	ClientID int64
	LeaderID int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:       key,
		ClientID:  ck.ClientID,
		CommandID: nrand(),
	}
	reply := GetReply{}
	for {
		ok := ck.servers[ck.LeaderID].Call("KVServer.Get", &args, &reply)
		if !ok {
			continue
		}
		switch reply.Err {
		case OK:
			return reply.Value
		case ErrNoKey:
			return ""
		case ErrWrongLeader:
			ck.LeaderID = (ck.LeaderID + 1) % len(ck.servers)
			continue
		default:
			continue
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientID:  ck.ClientID,
		CommandID: nrand(),
	}
	reply := PutAppendReply{}
	for {
		ok := ck.servers[ck.LeaderID].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			continue
		}
		switch reply.Err {
		case OK:
			return
		case ErrNoKey:
			log.Println("出现了不该出现的错误")
			panic("出现了不该出现的错误")
		case ErrWrongLeader:
			ck.LeaderID = (ck.LeaderID + 1) % len(ck.servers)
			continue
		default:
			continue
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
