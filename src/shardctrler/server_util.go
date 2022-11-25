package shardctrler

import (
	"6.824/labgob"
	"sort"
)

func init() {
	labgob.Register(JoinArgs{})
	labgob.Register(QueryArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})

	labgob.Register(JoinReply{})
	labgob.Register(QueryReply{})
	labgob.Register(LeaveReply{})
	labgob.Register(MoveReply{})
}

func (sc *ShardCtrler) RemoveNotifyWaitCommandCh(reqId int64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	delete(sc.notifyWaitCmd, reqId)
}

func (sc *ShardCtrler) HandleApplyMsg() {
	for {
		select {
		case <-sc.stopCh:
			return
		case applyMsg := <-sc.applyCh:
			if applyMsg.SnapshotValid {
				continue
			}
			// CommandValid is true
			if applyMsg.Command == nil {
				continue
			}
			command := applyMsg.Command.(Op)

			sc.mu.Lock()
			if command.Method == "Query" {
				args := command.Args.(QueryArgs)
				sc.NotifyWaitCommand(command.ReqID, OK, sc.GetConfigByNum(args.Num), false)
			} else {
				isRepeated := false
				// whether command  has been executed.
				if commandID, ok := sc.lastApplied[command.ClientID]; ok {
					if commandID == command.CommandID {
						isRepeated = true
					}
				}

				if !isRepeated {
					if command.Method == "Join" {
						args := command.Args.(JoinArgs)
						sc.handleJoinCommand(args)
						sc.lastApplied[command.ClientID] = command.CommandID
					} else if command.Method == "Move" {
						args := command.Args.(MoveArgs)
						sc.handleMoveCommand(args)
						sc.lastApplied[command.ClientID] = command.CommandID
					} else if command.Method == "Leave" {
						args := command.Args.(LeaveArgs)
						sc.handleLeaveCommand(args)
						sc.lastApplied[command.ClientID] = command.CommandID
					} else {
						sc.mu.Unlock()
						panic("unsupported method.")
					}
				}
				sc.NotifyWaitCommand(command.ReqID, OK, Config{}, false)
			}
			sc.mu.Unlock()
		}

	}
}

func (sc *ShardCtrler) NotifyWaitCommand(reqID int64, err Err, config Config, wrongLeader bool) {
	if channel, ok := sc.notifyWaitCmd[reqID]; ok {
		channel <- CommandResult{
			Err:         err,
			Config:      config,
			WrongLeader: wrongLeader,
		}
	}
}

func (sc *ShardCtrler) handleJoinCommand(args JoinArgs) {
	config := sc.GetConfigByNum(-1)
	config.Num += 1

	for k, v := range args.Servers {
		config.Groups[k] = v
	}
	sc.AdjustConfig(&config)
	sc.configs = append(sc.configs, config)
}

func (sc *ShardCtrler) handleMoveCommand(args MoveArgs) {
	config := sc.GetConfigByNum(-1)
	config.Num += 1

	config.Shards[args.Shard] = args.GID
	sc.AdjustConfig(&config)
	sc.configs = append(sc.configs, config)
}

func (sc *ShardCtrler) handleLeaveCommand(args LeaveArgs) {
	config := sc.GetConfigByNum(-1)
	config.Num += 1

	for _, gid := range args.GIDs {
		delete(config.Groups, gid)
		for shard, _ := range config.Shards {
			if gid == config.Shards[shard] {
				config.Shards[shard] = 0
			}
		}
	}
	sc.AdjustConfig(&config)
	sc.configs = append(sc.configs, config)
}

// 分配目标：
// 1. 使得每个组的分片数量尽可能接近，负载均衡
// 2. 每个组的分片变动尽可能的小，使得数据移动少，节省时间。

// 尽可能的较少的改变配置
// 假设共23 个分片，四个组
// 最初分配为 5 8 5 5
// 现在增加一个组，那么每个组最少4个,3个空闲，当前空出的为0
// 如果某个组当前数量
// 0 5 8 5 5
// 0 5 6 4 4
// 4 5 6 4 4

func (sc *ShardCtrler) AdjustConfig(config *Config) *Config {
	if len(config.Groups) == 0 {
		config.Shards = [NShards]int{}
	} else if len(config.Groups) == 1 {
		for gid, _ := range config.Groups {
			for i, _ := range config.Shards {
				config.Shards[i] = gid
			}
		}
	} else if len(config.Groups) <= NShards {
		avgShardsCount := NShards / len(config.Groups)
		otherShardsCount := NShards - avgShardsCount*len(config.Groups)
		isTryAgain := true

		for isTryAgain {
			isTryAgain = false
			gids := []int{}
			for gid, _ := range config.Groups {
				gids = append(gids, gid)
			}
			sort.Ints(gids)

			for _, gid := range gids {
				// 当前数量
				count := 0
				for _, v := range config.Shards {
					if gid == v {
						count++
					}
				}
				if count == avgShardsCount {
					continue
				} else if count > avgShardsCount && otherShardsCount != 0 {
					temp := 0
					for idx, v := range config.Shards {
						if v == gid {
							temp++
							if temp > avgShardsCount && otherShardsCount != 0 {
								otherShardsCount--
							} else if temp > avgShardsCount {
								config.Shards[idx] = 0
							}
						}
					}
				} else if count > avgShardsCount && otherShardsCount == 0 {
					temp := 0
					for idx, v := range config.Shards {
						if v == gid {
							temp++
							if temp > avgShardsCount {
								config.Shards[idx] = 0
							}
						}
					}
				} else {
					for idx, g := range config.Shards {
						if g == 0 {
							count++
							config.Shards[idx] = gid
							if count >= avgShardsCount {
								break
							}
						}
					}
					if count < avgShardsCount {
						isTryAgain = true
						continue
					}
				}
			}
			// 剩余的均分
			cur := 0
			for idx, v := range config.Shards {
				if v == 0 {
					config.Shards[idx] = gids[cur]
					cur += 1
					cur = cur % len(gids)
				}
			}
		}
	} else {
		// 0 1 2 3 0 0 0 0

		gidFlags := make(map[int]int, 0)
		emptyShards := make([]int, 0)
		for idx, v := range config.Shards {
			if v == 0 {
				emptyShards = append(emptyShards, idx)
			} else {
				if _, ok := gidFlags[v]; ok {
					config.Shards[idx] = 0
					emptyShards = append(emptyShards, idx)
				} else {
					gidFlags[v] = 1
				}
			}
		}

		if len(emptyShards) > 0 {
			gids := make([]int, 0)
			for gid, _ := range config.Groups {
				gids = append(gids, gid)
			}
			sort.Ints(gids)
			temp := 0
			for _, gid := range gids {
				if _, ok := gidFlags[gid]; !ok {
					gidFlags[gid] = 1
					config.Shards[emptyShards[temp]] = gid
					temp++
					if temp >= len(emptyShards) {
						break
					}
				}
			}
		}
	}
	return config
}

func (sc *ShardCtrler) GetConfigByNum(num int) Config {
	if num < 0 || num >= len(sc.configs) {
		return sc.configs[len(sc.configs)-1].Copy()
	}
	return sc.configs[num].Copy()
}
