package mr

import "sync"

// 可以借助可重入锁实现

type MapSet struct {
	Data map[int64]TaskInfo
	Lock *sync.Mutex
}

func (ms *MapSet) Size() int {
	return len(ms.Data)
}
func (ms *MapSet) Erase(taskID int64) {
	delete(ms.Data, taskID)
}

func (ms *MapSet) Set(taskID int64, data TaskInfo) {
	ms.Data[taskID] = data
}

func (ms *MapSet) Empty() bool {
	return len(ms.Data) == 0
}

func (ms *MapSet) Exists(taskID int64) bool {
	_, ok := ms.Data[taskID]
	return ok
}

func NewMapSet() *MapSet {
	return &MapSet{
		Data: make(map[int64]TaskInfo),
		Lock: &sync.Mutex{},
	}
}
