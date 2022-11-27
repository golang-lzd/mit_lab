package shardkv

import (
	"6.824/labgob"
	"6.824/shardctrler"
	"bytes"
	"encoding/json"
	"fmt"
	"log"
)

func init() {
	labgob.Register(CleanShardDataArgs{})
	labgob.Register(MergeShardData{})
	labgob.Register(shardctrler.Config{})

}

func (kv *ShardKV) ResetPullConfigTimer() {
	kv.PullConfigTimer.Stop()
	kv.PullConfigTimer.Reset(PullConfigTimeOut)
}

func (kv *ShardKV) ResetFetchShardTimer() {
	kv.PullShardsTimer.Stop()
	kv.PullShardsTimer.Reset(PullShardsTimeOut)
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

func FormatStruct(s interface{}) string {
	bs, err := json.Marshal(s)
	if err != nil {
		log.Println(err.Error())
	}
	var out bytes.Buffer
	json.Indent(&out, bs, "", "\t")
	return out.String()
}

func (kv *ShardKV) WithState(format string, a ...interface{}) string {
	_s := fmt.Sprintf(format, a...)
	return fmt.Sprintf("[Gid-%d  Node-%d  ConfigNum-%d] %s", kv.gid, kv.me, kv.config.Num, _s)
}
