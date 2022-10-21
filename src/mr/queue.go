package mr

import "sync"

type Node struct {
	value interface{}

	pre, next *Node
}

type DLinkedList struct {
	head, tail *Node

	Count int
}

func NewDLinkedList() *DLinkedList {
	head := &Node{}
	tail := &Node{}
	head.next = tail
	tail.pre = head

	return &DLinkedList{
		Count: 0,
		head:  head,
		tail:  tail,
	}
}

func (obj *DLinkedList) Empty() bool {
	return obj.Count == 0
}

func (obj *DLinkedList) PushBack(value interface{}) {
	tmp := obj.tail.pre
	tmp.next = &Node{
		value: value,
		pre:   tmp,
		next:  obj.tail,
	}
	obj.tail.pre = tmp.next
	obj.Count++
}

func (obj *DLinkedList) PopFront() interface{} {
	if !obj.Empty() {
		tmp := obj.head.next
		obj.head.next = tmp.next
		tmp.next.pre = obj.head
		obj.Count--
		return tmp.value
	} else {
		return nil
	}
}

type BlockedQueue struct {
	list *DLinkedList
	lock *sync.Mutex
}

func (queue *BlockedQueue) PeekFront() TaskInfo {
	defer queue.lock.Unlock()
	queue.lock.Lock()

	tmp := queue.list.PopFront()
	if tmp != nil {
		return tmp.(TaskInfo)
	} else {
		return nil
	}
}

func (queue *BlockedQueue) PushBack(task TaskInfo) {
	defer queue.lock.Unlock()
	queue.lock.Lock()

	queue.list.PushBack(task)
}

func (queue *BlockedQueue) Empty() bool {
	return queue.list.Empty()
}

func newBlockedQueue() *BlockedQueue {
	queue := BlockedQueue{
		list: NewDLinkedList(),
		lock: &sync.Mutex{},
	}
	return &queue
}
