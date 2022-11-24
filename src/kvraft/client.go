package kvraft

import (
	"6.824/labrpc"
	"fmt"
	"math/rand"
	"time"
)

// 假设每一个clerk 都不会并发执行多个请求,可以通过map[clientID]commandID 记录每一个client的最后一个command.
//

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	ClientID int64
	LeaderID int
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.ClientID = nrand()
	ck.LeaderID = rand.Int() % len(servers)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)

func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:       key,
		ClientID:  ck.ClientID,
		CommandID: nrand(),
	}
	reply := GetReply{}

	//log.Println(ck.WithState("Req: Get %s", key))
	for {
		ok := ck.servers[ck.LeaderID].Call("KVServer.Get", &args, &reply)
		if !ok {
			time.Sleep(ChangeLeaderInterval)
			ck.LeaderID = (ck.LeaderID + 1) % len(ck.servers)
			continue
		}
		switch reply.Err {
		case OK:
			//log.Println(ck.WithState("Resp: Get %s -> %s", key, reply.Value))
			return reply.Value
		case ErrNoKey:
			//log.Println(ck.WithState("Resp: Get %s -> %s", key, ""))
			return ""
		case ErrCommandTimeOut:
			continue
		default:
			time.Sleep(ChangeLeaderInterval)
			ck.LeaderID = (ck.LeaderID + 1) % len(ck.servers)
			continue
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)

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

	//log.Println(ck.WithState("Req: putAppend %s %s", key, value))
	for {
		ok := ck.servers[ck.LeaderID].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			time.Sleep(ChangeLeaderInterval)
			ck.LeaderID = (ck.LeaderID + 1) % len(ck.servers)
			continue
		}
		switch reply.Err {
		case OK:
			//log.Println(ck.WithState("Resp: putAppend %s %s ->%s", key, value, "Success"))
			return
		case ErrNoKey:
			//log.Println(ck.WithState("出现了不该出现的错误"))
			panic("出现了不该出现的错误")
		case ErrWrongLeader:
			time.Sleep(ChangeLeaderInterval)
			ck.LeaderID = (ck.LeaderID + 1) % len(ck.servers)
			continue
		case ErrServer:
			time.Sleep(ChangeLeaderInterval)
			ck.LeaderID = (ck.LeaderID + 1) % len(ck.servers)
			continue
		case ErrCommandTimeOut:
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

func (ck *Clerk) WithState(format string, a ...interface{}) string {
	_s := fmt.Sprintf(format, a...)
	return fmt.Sprintf("[ClientID-%d  LeaderID:%d] %s", ck.ClientID, ck.LeaderID, _s)
}
