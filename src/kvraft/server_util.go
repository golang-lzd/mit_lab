package kvraft

func (kv *KVServer) HandleApplyMsg() {
	for applyMsg := range kv.applyCh {
		if applyMsg.SnapshotValid {
			//

			continue
		}
		// CommandValid is true
		if applyMsg.Command != nil {
			continue
		}
		command := applyMsg.Command.(Op)
		if command.Method == "GET" {

		} else if command.Method == "PUT" {

		} else if command.Method == "Append" {

		} else {
			panic("op.Method is not supported.")
		}
	}
}