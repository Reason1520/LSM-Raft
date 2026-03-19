package lsm

import (
	"container/heap"
	"fmt"
)

// Iterator defines the unified iterator interface for LSM components.
type Iterator interface {
	Valid() bool
	Next()
	Key() string
	Value() string
	TrancID() uint64
	Close() error
	Seek(key string) bool
	SeekFirst() bool
}

// HeapItem is an item stored in the heap.
type HeapItem struct {
	iter    Iterator
	key     string
	value   string
	trancID uint64
	listID  int
}

type iterHeap []HeapItem

func (h iterHeap) Len() int { return len(h) }

func (h iterHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *iterHeap) Push(x interface{}) { *h = append(*h, x.(HeapItem)) }

func (h *iterHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// Less defines heap ordering: smaller key first; for equal keys, larger trancID first.
func (h iterHeap) Less(i, j int) bool {
	if h[i].key != h[j].key {
		return h[i].key < h[j].key
	}
	if h[i].trancID != h[j].trancID {
		return h[i].trancID > h[j].trancID
	}
	return h[i].listID < h[j].listID
}

// HeapIterator merges multiple iterators with de-duplication by key.
type HeapIterator struct {
	heap       *iterHeap
	current    *HeapItem
	iters      []Iterator
	skipDelete bool
	closed     bool
	maxTrancID uint64
}

func NewHeapIterator(iters []Iterator, skipDelete bool, maxTrancID uint64) *HeapIterator {
	hi := &HeapIterator{heap: &iterHeap{}, iters: iters, skipDelete: skipDelete, maxTrancID: maxTrancID}
	if len(iters) == 0 {
		return hi
	}

	for i, iter := range iters {
		if iter.Valid() {
			heap.Push(hi.heap, HeapItem{
				iter:    iter,
				key:     iter.Key(),
				value:   iter.Value(),
				trancID: iter.TrancID(),
				listID:  i,
			})
		}
	}
	if hi.heap.Len() > 0 {
		hi.Next()
	}

	return hi
}

func (hi *HeapIterator) advanceAndPush(item HeapItem) {
	item.iter.Next()
	if item.iter.Valid() {
		item.key = item.iter.Key()
		item.value = item.iter.Value()
		item.trancID = item.iter.TrancID()
		heap.Push(hi.heap, item)
	}
}

// Next moves to the next unique key (newest visible version wins).
func (hi *HeapIterator) Next() {
	for {
		if hi.heap.Len() == 0 {
			hi.current = nil
			return
		}

		top := heap.Pop(hi.heap).(HeapItem)
		targetKey := top.key
		candidates := []HeapItem{top}

		// 收集同 key 的所有版本
		hi.advanceAndPush(top)
		for hi.heap.Len() > 0 && (*hi.heap)[0].key == targetKey {
			older := heap.Pop(hi.heap).(HeapItem)
			candidates = append(candidates, older)
			hi.advanceAndPush(older)
		}

		// 选择最新的可见版本（trancID <= maxTrancID）
		chosenIdx := -1
		for i, cand := range candidates {
			if hi.maxTrancID != 0 && cand.trancID > hi.maxTrancID {
				continue
			}
			if chosenIdx == -1 {
				chosenIdx = i
				continue
			}
			chosen := candidates[chosenIdx]
			if cand.trancID > chosen.trancID || (cand.trancID == chosen.trancID && cand.listID < chosen.listID) {
				chosenIdx = i
			}
		}

		// 如果没有可见版本，则继续
		if chosenIdx == -1 {
			continue
		}

		// 如果是删除操作，则继续
		chosen := candidates[chosenIdx]
		if hi.skipDelete && chosen.value == "" {
			continue
		}

		hi.current = &chosen
		return
	}
}

func (hi *HeapIterator) Valid() bool { return !hi.closed && hi.current != nil }

func (hi *HeapIterator) Key() string {
	if !hi.Valid() {
		return ""
	}
	return hi.current.key
}

func (hi *HeapIterator) Value() string {
	if !hi.Valid() {
		return ""
	}
	return hi.current.value
}

func (hi *HeapIterator) TrancID() uint64 {
	if !hi.Valid() {
		return 0
	}
	return hi.current.trancID
}

func (hi *HeapIterator) Seek(key string) bool {
	if hi.closed {
		return false
	}

	hi.heap = &iterHeap{}
	hi.current = nil

	for i, iter := range hi.iters {
		if iter.Seek(key) {
			heap.Push(hi.heap, HeapItem{
				iter:    iter,
				key:     iter.Key(),
				value:   iter.Value(),
				trancID: iter.TrancID(),
				listID:  i,
			})
		}
	}

	hi.Next()
	return hi.Valid()
}

func (hi *HeapIterator) SeekFirst() bool {
	if hi.closed {
		return false
	}

	hi.heap = &iterHeap{}
	hi.current = nil

	for i, iter := range hi.iters {
		if iter.SeekFirst() {
			heap.Push(hi.heap, HeapItem{
				iter:    iter,
				key:     iter.Key(),
				value:   iter.Value(),
				trancID: iter.TrancID(),
				listID:  i,
			})
		}
	}

	hi.Next()
	return hi.Valid()
}

func (hi *HeapIterator) Close() error {
	if hi.closed {
		return nil
	}

	var errRet error
	for _, iter := range hi.iters {
		if err := iter.Close(); err != nil {
			errRet = fmt.Errorf("iterator close error: %v", err)
		}
	}

	hi.closed = true
	hi.current = nil
	hi.heap = nil
	return errRet
}
