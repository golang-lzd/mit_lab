package shardkv

import "6.824/labgob"

func init() {
	labgob.Register(CleanShardDataArgs{})
	labgob.Register(MergeShardData{})
}

func (kv *ShardKV) ResetPullConfigTimer() {
	kv.PullConfigTimer.Stop()
	kv.PullConfigTimer.Reset(PullConfigTimeOut)
}

func (kv *ShardKV) ResetFetchShardTimer() {
	kv.PullShardsTime.Stop()
	kv.PullShardsTime.Reset(PullShardsTimeOut)
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

func (kv *ShardKV) OutputDataExists(configNum int, shardID int) bool {
	if config, ok := kv.outputShards[configNum]; ok {
		if _, tmp := config[shardID]; tmp {
			return true
		}
	}
	return false
}
