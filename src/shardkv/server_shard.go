package shardkv

import "6.824/shardctrler"

type MergeShardData struct {
	ConfigNum    int
	ShardID      int
	Data         map[string]string
	CommandIndex int
}

type CleanShardDataArgs struct {
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
					go kv.FetchShard(shardID, kv.oldConfig)
				}
			}
			kv.ResetFetchShardTimer()
		}
	}
}

func (kv *ShardKV) FetchShard(shardID int, config shardctrler.Config) {

}
