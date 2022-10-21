package mr

import "sync"

type MapSet struct {
	Data   map[int64]TaskInfo
	RWLock sync.RWMutex
}

func (ms *MapSet) Erase(taskID int64) {
	ms.RWLock.Lock()
	defer ms.RWLock.Unlock()
	delete(ms.Data, taskID)
}

func (ms *MapSet) Set(taskID int64, data TaskInfo) {
	ms.RWLock.Lock()
	ms.Data[taskID] = data
	ms.RWLock.Unlock()
}

func (ms *MapSet) Empty() bool {
	ms.RWLock.RLock()
	defer ms.RWLock.RUnlock()

	return len(ms.Data) == 0
}

func (ms *MapSet) Exists(taskID int64) bool {
	ms.RWLock.RLock()
	defer ms.RWLock.RUnlock()

	_, ok := ms.Data[taskID]
	return ok
}
