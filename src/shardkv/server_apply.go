package shardkv

import (
	"6.824/shardctrler"
)

func (kv *ShardKV) HandleApplyMsg() {
	for {
		select {
		case <-kv.stopCh:
			return
		case applyMsg := <-kv.applyCh:
			if applyMsg.SnapshotValid {
				// TODO
				continue
			}
			cmdIndex := applyMsg.CommandIndex
			if op, ok := applyMsg.Command.(Op); ok {
				kv.HandleOpCommand(cmdIndex, op)
			} else if config, ok := applyMsg.Command.(shardctrler.Config); ok {
				kv.HandleConfigCommand(cmdIndex, config)
			} else {
				panic("unsupported command type.")
			}
		}
	}
}

func (kv *ShardKV) HandleOpCommand(cmdIndex int, command Op) {
	if err := kv.ProcessKeyReady(command.ConfigNum, command.Key); err != OK {
		kv.NotifyWaitCommand(command.ReqID, err, "")
		return
	}

	shardID := key2shard(command.Key)
	kv.mu.Lock()
	if command.Method == "Get" {
		err, val := kv.GetValueByKey(command.Key)
		kv.NotifyWaitCommand(command.ReqID, err, val)
	} else {
		isRepeated := false
		// whether command  has been executed.
		if commandID, ok := kv.lastApplied[shardID][command.ClientID]; ok {
			if commandID == command.CommandID {
				isRepeated = true
			}
		}

		if !isRepeated {
			if command.Method == "Put" {
				kv.data[shardID][command.Key] = command.Value
				kv.lastApplied[shardID][command.ClientID] = command.CommandID
			} else if command.Method == "Append" {
				if val, ok := kv.data[shardID][command.Key]; ok {
					kv.data[shardID][command.Key] = val + command.Value
				} else {
					kv.data[shardID][command.Key] = command.Value
				}
				kv.lastApplied[shardID][command.ClientID] = command.CommandID
			} else {
				kv.mu.Unlock()
				panic("unsupported method.")
			}
		}
		kv.NotifyWaitCommand(command.ReqID, OK, "")
	}
	kv.SaveSnapShot(cmdIndex)
	kv.mu.Unlock()
}

func (kv *ShardKV) HandleConfigCommand(cmdIndex int, conf shardctrler.Config) {
	if conf.Num <= kv.config.Num {
		return
	}

	inputShards := make([]int, 0)
	outputShards := make([]int, 0)
	meShards := make([]int, 0)
	oldConfig := kv.config.Copy()

	for shardID, gid := range conf.Shards {
		if gid == kv.gid {
			meShards = append(meShards, shardID)
			if _, ok := kv.meShards[shardID]; !ok {
				inputShards = append(inputShards, shardID)
			}
		} else {
			if _, ok := kv.meShards[shardID]; ok {
				outputShards = append(outputShards, shardID)
			}
		}
	}

	kv.meShards = make(map[int]bool)
	for _, shardID := range meShards {
		kv.meShards[shardID] = true
	}

	kv.inputShards = make(map[int]bool)
	if oldConfig.Num != 0 {
		for _, shardID := range inputShards {
			kv.inputShards[shardID] = true
		}
	}

	//TODO outputShards
	kv.config = conf
	kv.oldConfig = oldConfig
}

func (kv *ShardKV) HandleMergeShardDataCommand(cmdIndex int, op Op) {

}

func (kv *ShardKV) HandleCleanShardDataCommand(cmdIndex int, op Op) {

}

func (kv *ShardKV) GetValueByKey(key string) (Err, string) {
	shardID := key2shard(key)
	if val, ok := kv.data[shardID][key]; ok {
		return OK, val
	} else {
		return ErrNoKey, ""
	}
}

func (kv *ShardKV) ProcessKeyReady(configNum int, key string) Err {
	if configNum == 0 || configNum != kv.config.Num {
		return ErrWrongGroup
	}

	shardID := key2shard(key)
	if _, ok := kv.meShards[shardID]; !ok {
		return ErrWrongGroup
	}

	if _, ok := kv.inputShards[shardID]; ok {
		return ErrWrongGroup
	}
	return OK
}
