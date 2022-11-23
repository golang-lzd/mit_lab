package kvraft

import (
	"6.824/labgob"
	"6.824/raft"
	"bytes"
	"fmt"
)

func (kv *KVServer) HandleApplyMsg() {
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
			// CommandValid is true
			if applyMsg.Command == nil {
				continue
			}
			command := applyMsg.Command.(Op)

			kv.mu.Lock()

			if command.Method == "Get" {
				err, val := kv.GetValueByKey(command.Key)
				kv.NotifyWaitCommand(command.ReqID, err, val)
			} else {
				isRepeated := false
				// whether command  has been executed.
				if commandID, ok := kv.lastApplied[command.ClientID]; ok {
					if commandID == command.CommandID {
						isRepeated = true
					}
				}

				if !isRepeated {
					if command.Method == "Put" {
						kv.data[command.Key] = command.Value
						kv.lastApplied[command.ClientID] = command.CommandID
					} else if command.Method == "Append" {
						if val, ok := kv.data[command.Key]; ok {
							kv.data[command.Key] = val + command.Value
						} else {
							kv.data[command.Key] = command.Value
						}
						kv.lastApplied[command.ClientID] = command.CommandID
					} else {
						kv.mu.Unlock()
						panic("unsupported method.")
					}
				}
				kv.NotifyWaitCommand(command.ReqID, OK, "")
			}
			kv.SaveSnapShot(applyMsg.CommandIndex)
			kv.mu.Unlock()
		}

	}
}

func (kv *KVServer) GetValueByKey(key string) (Err, string) {
	if val, ok := kv.data[key]; ok {
		return OK, val
	} else {
		return ErrNoKey, ""
	}
}

func (kv *KVServer) NotifyWaitCommand(reqId int64, err Err, value string) {
	if channel, ok := kv.notifyWaitCommand[reqId]; ok {
		channel <- CommandResult{
			ERR:   err,
			Value: value,
		}
	}
}

func (kv *KVServer) RemoveNotifyWaitCommandCh(reqId int64) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.notifyWaitCommand, reqId)
}

func (kv *KVServer) WithState(format string, a ...interface{}) string {
	_s := fmt.Sprintf(format, a...)
	return fmt.Sprintf("[Server-%d  isLeader:%v] %s", kv.me, kv.rf.StateMachine.GetState() == raft.LeaderState, _s)
}

func (kv *KVServer) SaveSnapShot(index int) {
	if kv.maxraftstate == -1 || kv.maxraftstate > kv.persister.RaftStateSize() {
		return
	}
	out := &bytes.Buffer{}
	enc := labgob.NewEncoder(out)
	if err := enc.Encode(kv.data); err != nil {
		panic(err)
	}
	if err := enc.Encode(kv.lastApplied); err != nil {
		panic(err)
	}
	kv.rf.Snapshot(index, out.Bytes())
}

func (kv *KVServer) InitReadSnapShot() {
	data := kv.persister.ReadSnapshot()
	if len(data) < 1 {
		return
	}
	r := bytes.NewReader(data)
	d := labgob.NewDecoder(r)
	var lastApplied map[int64]int64
	var kvdata map[string]string
	if err := d.Decode(&kvdata); err != nil {
		panic(err)
	}
	if err := d.Decode(&lastApplied); err != nil {
		panic(err)
	}
	kv.data = kvdata
	kv.lastApplied = lastApplied
}

func (kv *KVServer) CondInstallSnapShot(snapShotTerm int, snapShotIndex int, data []byte) {
	if len(data) < 1 {
		return
	}
	kv.rf.CondInstallSnapshot(snapShotTerm, snapShotIndex, data)
	r := bytes.NewReader(data)
	d := labgob.NewDecoder(r)
	var lastApplied map[int64]int64
	var kvdata map[string]string
	if err := d.Decode(&kvdata); err != nil {
		panic(err)
	}
	if err := d.Decode(&lastApplied); err != nil {
		panic(err)
	}
	kv.data = kvdata
	kv.lastApplied = lastApplied
}
