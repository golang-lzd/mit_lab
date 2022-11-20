package kvraft

func (kv *KVServer) HandleApplyMsg() {
	for applyMsg := range kv.applyCh {
		if applyMsg.SnapshotValid {
			//
			continue
		}
		if !applyMsg.CommandValid {
			continue
		}
		// CommandValid is true
		if applyMsg.Command != nil {
			continue
		}
		command := applyMsg.Command.(Op)

		// whether command  has been executed.
		if commandID, ok := kv.lastApplied[command.ClientID]; ok {
			if commandID == command.CommandID {
				continue
			}
		}

		if command.Method == "GET" {
			err, val := kv.GetValueByKey(command.Key)
			kv.lastApplied[command.ClientID] = command.CommandID
			kv.NotifyWaitCommand(command.ReqID, err, val)
		} else if command.Method == "PUT" {
			kv.data[command.Key] = command.Value
			kv.lastApplied[command.ClientID] = command.CommandID
			kv.NotifyWaitCommand(command.ReqID, OK, "")
		} else if command.Method == "Append" {
			if val, ok := kv.data[command.Key]; ok {
				kv.data[command.Key] = val + command.Value
			} else {
				kv.data[command.Key] = command.Value
			}
			kv.lastApplied[command.ClientID] = command.CommandID
			kv.NotifyWaitCommand(command.ReqID, OK, "")
		} else {
			panic("op.Method is not supported.")
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
