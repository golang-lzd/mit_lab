package kvraft

import (
	"6.824/raft"
	"fmt"
)

func (kv *KVServer) HandleApplyMsg() {
	for {
		select {
		case <-kv.stopCh:
			return
		case applyMsg := <-kv.applyCh:
			if applyMsg.SnapshotValid {
				//TODO
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
