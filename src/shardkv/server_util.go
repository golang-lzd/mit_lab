package shardkv

func (kv *ShardKV) ResetPullConfigTimer() {
	kv.PullConfigTimer.Stop()
	kv.PullConfigTimer.Reset(PullConfigTimeOut)
}

func (kv *ShardKV) ResetFetchShardTimer() {
	kv.PullShardsTime.Stop()
	kv.PullShardsTime.Reset(PullShardsTimeOut)
}
