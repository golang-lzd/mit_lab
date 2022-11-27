package shardkv

import (
	"6.824/shardctrler"
	"time"
)

type MergeShardData struct {
	ConfigNum   int
	ShardID     int
	Data        map[string]string
	LastApplied map[int64]int64
}

type CleanShardDataArgs struct {
	ConfigNum int
	ShardNum  int
}

type CleanShardDataReply struct {
	Success bool
}

type FetchShardDataArgs struct {
	ConfigNum int
	ShardNum  int
}

type FetchShardDataReply struct {
	Success     bool
	Data        map[string]string
	LastApplied map[int64]int64
}

func (fsd *FetchShardDataReply) Copy() FetchShardDataReply {
	res := FetchShardDataReply{
		Success:     fsd.Success,
		Data:        make(map[string]string),
		LastApplied: make(map[int64]int64),
	}
	for k, v := range fsd.Data {
		res.Data[k] = v
	}
	for k, v := range fsd.LastApplied {
		res.LastApplied[k] = v
	}

	return res
}

func (kv *ShardKV) FetchShards() {
	for {
		select {
		case <-kv.stopCh:
			return
		case <-kv.PullConfigTimer.C:
			_, isLeader := kv.rf.GetState()
			if isLeader {
				for shardID, _ := range kv.inputShards {
					go kv.SendFetchShard(shardID, kv.oldConfig)
				}
			}
			kv.ResetFetchShardTimer()
		}
	}
}

// TODO 协程泄漏

func (kv *ShardKV) SendFetchShard(shardID int, config shardctrler.Config) {
	args := FetchShardDataArgs{
		ShardNum:  shardID,
		ConfigNum: config.Num,
	}

	t := time.NewTimer(FetchShardTimeOut)
	for {
		for _, server := range config.Groups[config.Shards[shardID]] {
			reply := FetchShardDataReply{}
			srv := kv.make_end(server)

			t.Stop()
			t.Reset(FetchShardTimeOut)
			done := make(chan bool, 1)
			go func(args *FetchShardDataArgs, reply *FetchShardDataReply) {
				done <- srv.Call("ShardKV.FetchShard", args, reply)
			}(&args, &reply)
			select {
			case <-kv.stopCh:
				return
			case <-t.C:
				continue
			case ok := <-done:
				if ok && reply.Success {
					kv.mu.Lock()
					if _, ok := kv.inputShards[shardID]; ok && config.Num+1 == kv.config.Num {
						replyCopy := reply.Copy()
						mergeShardData := MergeShardData{
							ConfigNum:   args.ConfigNum,
							ShardID:     args.ShardNum,
							Data:        replyCopy.Data,
							LastApplied: replyCopy.LastApplied,
						}
						kv.rf.Start(mergeShardData)
						kv.mu.Unlock()
						return
					} else {
						kv.mu.Unlock()
					}
				}
			}
		}
		kv.mu.Lock()
		if config.Num+1 != kv.config.Num || len(kv.inputShards) == 0 {
			kv.mu.Unlock()
			break
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) FetchShard(args *FetchShardDataArgs, reply *FetchShardDataReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 必须是过去的config
	if args.ConfigNum >= kv.config.Num {
		return
	}

	if configData, ok := kv.outputShards[args.ConfigNum]; ok {
		if shardData, ok := configData[args.ShardNum]; ok {
			reply.Success = true
			reply.Data = make(map[string]string)
			for k, v := range shardData.Data {
				reply.Data[k] = v
			}
			for k, v := range shardData.LastApplied {
				reply.LastApplied[k] = v
			}
		}
	}
}

func (kv *ShardKV) SendCleanShardData(shardID int, config shardctrler.Config) {
	args := CleanShardDataArgs{
		ShardNum:  shardID,
		ConfigNum: config.Num,
	}

	t := time.NewTimer(CleanShardTimeOut)
	for {
		for _, server := range config.Groups[config.Shards[shardID]] {
			reply := CleanShardDataReply{}
			srv := kv.make_end(server)
			done := make(chan bool, 1)
			go func(args *CleanShardDataArgs, reply *CleanShardDataReply) {
				done <- srv.Call("ShardKV.CleanShardData", args, reply)
			}(&args, &reply)

			t.Stop()
			t.Reset(CleanShardTimeOut)
			select {
			case <-kv.stopCh:
				return
			case <-t.C:
				continue
			case ok := <-done:
				if ok && reply.Success {
					return
				}
			}
		}

		kv.mu.Lock()
		if config.Num+1 != kv.config.Num || len(kv.inputShards) == 0 {
			kv.mu.Unlock()
			break
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) CleanShardData(args *CleanShardDataArgs, reply *CleanShardDataReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.ConfigNum >= kv.config.Num {
		return
	}

	_, _, isLeader := kv.rf.Start(*args)
	if isLeader {
		return
	}
	for i := 0; i < 10; i++ {
		if !kv.OutputDataExists(args.ConfigNum, args.ShardNum) {
			reply.Success = true
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}
