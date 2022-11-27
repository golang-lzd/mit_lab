package shardkv

import (
	"6.824/labgob"
	"6.824/shardctrler"
	"bytes"
)

func (kv *ShardKV) CondInstallSnapShot(snapShotTerm int, snapShotIndex int, snapShot []byte) {
	if snapShot == nil || len(snapShot) == 0 {
		return
	}

	r := bytes.NewReader(snapShot)
	dec := labgob.NewDecoder(r)

	var (
		data         [shardctrler.NShards]map[string]string
		lastApplied  [shardctrler.NShards]map[int64]int64
		outputShards map[int]map[int]MergeShardData
		inputShards  map[int]bool
		oldConfig    shardctrler.Config
		conf         shardctrler.Config
		meShards     map[int]bool
	)

	if dec.Decode(&data) != nil ||
		dec.Decode(&lastApplied) != nil ||
		dec.Decode(&outputShards) != nil ||
		dec.Decode(&inputShards) != nil ||
		dec.Decode(&oldConfig) != nil ||
		dec.Decode(&conf) != nil ||
		dec.Decode(&meShards) != nil {
		panic("decode error")
	}
	kv.data = data
	kv.lastApplied = lastApplied
	kv.outputShards = outputShards
	kv.inputShards = inputShards
	kv.oldConfig = oldConfig
	kv.config = conf
	kv.meShards = meShards

}

func (kv *ShardKV) saveSnapShot(cmdIndex int) {
	if kv.maxraftstate == -1 || kv.persister.RaftStateSize() < kv.maxraftstate {
		return
	}
	w := new(bytes.Buffer)
	enc := labgob.NewEncoder(w)
	if enc.Encode(kv.data) != nil ||
		enc.Encode(kv.lastApplied) != nil ||
		enc.Encode(kv.outputShards) != nil ||
		enc.Encode(kv.inputShards) != nil ||
		enc.Encode(kv.oldConfig) != nil ||
		enc.Encode(kv.config) != nil ||
		enc.Encode(kv.meShards) != nil {
		panic("encode error")
	}
	data := w.Bytes()
	kv.rf.Snapshot(cmdIndex, data)
}

func (kv *ShardKV) InitReadSnapShot() {
	if kv.persister.ReadSnapshot() == nil || len(kv.persister.ReadSnapshot()) == 0 {
		return
	}

	r := bytes.NewReader(kv.persister.ReadSnapshot())
	dec := labgob.NewDecoder(r)

	var (
		data         [shardctrler.NShards]map[string]string
		lastApplied  [shardctrler.NShards]map[int64]int64
		outputShards map[int]map[int]MergeShardData
		inputShards  map[int]bool
		oldConfig    shardctrler.Config
		conf         shardctrler.Config
		meShards     map[int]bool
	)

	var err error
	if err = dec.Decode(&data); err != nil {
		panic(err.Error())
	}
	if err = dec.Decode(&lastApplied); err != nil {
		panic(err.Error())
	}
	if err = dec.Decode(&outputShards); err != nil {
		panic(err.Error())
	}
	if err = dec.Decode(&inputShards); err != nil {
		panic(err.Error())
	}
	if err = dec.Decode(&oldConfig); err != nil {
		panic(err.Error())
	}
	if err = dec.Decode(&conf); err != nil {
		panic(err.Error())
	}
	if err = dec.Decode(&meShards); err != nil {
		panic(err.Error())
	}

	kv.data = data
	kv.lastApplied = lastApplied
	kv.outputShards = outputShards
	kv.inputShards = inputShards
	kv.oldConfig = oldConfig
	kv.config = conf
	kv.meShards = meShards
}
