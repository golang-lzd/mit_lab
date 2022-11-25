package shardctrler

import "6.824/labgob"

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

						sc.lastApplied[command.ClientID] = command.CommandID
					} else if command.Method == "Move" {

						sc.lastApplied[command.ClientID] = command.CommandID
					} else if command.Method == "Leave" {

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
	sc.adjustConfig(&config)
	sc.configs = append(sc.configs, config)
}

func (sc *ShardCtrler) handleMoveCommand(args MoveArgs) {
	config := sc.GetConfigByNum(-1)
	config.Num += 1

	config.Shards[args.Shard] = args.GID
	sc.adjustConfig(&config)
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
	sc.adjustConfig(&config)
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

func (sc *ShardCtrler) adjustConfig(config *Config) {
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
			gids := []int{}
			for gid, _ := range config.Groups {
				gids = append(gids, gid)
			}

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

				} else if count > avgShardsCount && otherShardsCount == 0 {

				} else {

				}

			}
		}

	}
}

func (sc *ShardCtrler) GetConfigByNum(num int) Config {
	if num < 0 || num >= len(sc.configs) {
		return sc.configs[len(sc.configs)-1].Copy()
	}
	return sc.configs[num].Copy()
}
