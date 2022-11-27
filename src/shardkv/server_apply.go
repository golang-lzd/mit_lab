package shardkv

import (
	"6.824/shardctrler"
	"log"
)

func (kv *ShardKV) HandleApplyMsg() {
	for {
		select {
		case <-kv.stopCh:
			return
		case applyMsg := <-kv.applyCh:
			if applyMsg.SnapshotValid {
				kv.mu.Lock()
				kv.CondInstallSnapShot(applyMsg.SnapshotTerm, applyMsg.SnapshotIndex, applyMsg.Snapshot)
				kv.mu.Unlock()
				continue
			}
			if !applyMsg.CommandValid {
				continue
			}
			cmdIndex := applyMsg.CommandIndex
			if op, ok := applyMsg.Command.(Op); ok {
				log.Println(kv.WithState("接收到Op 命令:%s", FormatStruct(op)))
				kv.HandleOpCommand(cmdIndex, op)
			} else if config, ok := applyMsg.Command.(shardctrler.Config); ok {
				log.Println(kv.WithState("接收到config 命令:%s", FormatStruct(config)))
				kv.HandleConfigCommand(cmdIndex, config)
			} else if data, ok := applyMsg.Command.(MergeShardData); ok {
				log.Println(kv.WithState("接收到 mergeShardData 命令:%s", FormatStruct(data)))
				kv.HandleMergeShardDataCommand(cmdIndex, data)
			} else if args, ok := applyMsg.Command.(CleanShardDataArgs); ok {
				log.Println(kv.WithState("接收到CleanShardData 命令:%s", FormatStruct(args)))
				kv.HandleCleanShardDataCommand(cmdIndex, args)
			} else {
				panic("unsupported command type.")
			}
		}
	}
}

func (kv *ShardKV) HandleOpCommand(cmdIndex int, command Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if err := kv.ProcessKeyReady(command.ConfigNum, command.Key); err != OK {
		kv.NotifyWaitCommand(command.ReqID, err, "")
		return
	}

	shardID := key2shard(command.Key)
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
				panic("unsupported method.")
			}
		}
		kv.NotifyWaitCommand(command.ReqID, OK, "")
	}
	kv.saveSnapShot(cmdIndex)
}

func (kv *ShardKV) HandleConfigCommand(cmdIndex int, conf shardctrler.Config) {
	kv.mu.Lock()
	defer func() {
		log.Println(kv.WithState("config command finished."))
		kv.mu.Unlock()
	}()

	if conf.Num <= kv.config.Num {
		kv.saveSnapShot(cmdIndex)
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

	kv.outputShards[oldConfig.Num] = make(map[int]MergeShardData)
	for _, shardID := range outputShards {
		kv.outputShards[oldConfig.Num][shardID] = MergeShardData{
			ConfigNum:   oldConfig.Num,
			ShardID:     shardID,
			Data:        kv.data[shardID],
			LastApplied: kv.lastApplied[shardID],
		}
		kv.lastApplied[shardID] = make(map[int64]int64)
		kv.data[shardID] = make(map[string]string)
	}

	kv.config = conf
	kv.oldConfig = oldConfig

	kv.saveSnapShot(cmdIndex)
}

func (kv *ShardKV) HandleMergeShardDataCommand(cmdIndex int, data MergeShardData) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.config.Num != data.ConfigNum+1 {
		return
	}
	if _, ok := kv.inputShards[data.ShardID]; !ok {
		return
	}
	kv.data[data.ShardID] = make(map[string]string)
	kv.lastApplied[data.ShardID] = make(map[int64]int64)

	for k, v := range data.Data {
		kv.data[data.ShardID][k] = v
	}
	for k, v := range data.LastApplied {
		kv.lastApplied[data.ShardID][k] = v
	}
	delete(kv.inputShards, data.ShardID)
	kv.saveSnapShot(cmdIndex)
	go kv.SendCleanShardData(data.ShardID, kv.oldConfig)

}

func (kv *ShardKV) HandleCleanShardDataCommand(cmdIndex int, args CleanShardDataArgs) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.OutputDataExists(args.ConfigNum, args.ShardNum) {
		delete(kv.outputShards[args.ConfigNum], args.ConfigNum)
	}

	kv.saveSnapShot(cmdIndex)
}
