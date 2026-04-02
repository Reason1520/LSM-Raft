package lsm

import "container/heap"

// VersionedHeapIterator merges iterators without de-duplicating keys.
// Order: key ASC, trancID DESC.
type VersionedHeapIterator struct {
	heap       *iterHeap
	current    *HeapItem
	iters      []Iterator
	closed     bool
	maxTrancID uint64
}

func NewVersionedHeapIterator(iters []Iterator, maxTrancID uint64) *VersionedHeapIterator {
	it := &VersionedHeapIterator{heap: &iterHeap{}, iters: iters, maxTrancID: maxTrancID}
	if len(iters) == 0 {
		return it
	}
	for i, iter := range iters {
		if iter.Valid() {
			heap.Push(it.heap, HeapItem{
				iter:    iter,
				key:     iter.Key(),
				value:   iter.Value(),
				trancID: iter.TrancID(),
				listID:  i,
			})
		}
	}
	it.Next()
	return it
}

func (it *VersionedHeapIterator) rebuild() {
	it.heap = &iterHeap{}
	for i, iter := range it.iters {
		if iter.Valid() {
			heap.Push(it.heap, HeapItem{
				iter:    iter,
				key:     iter.Key(),
				value:   iter.Value(),
				trancID: iter.TrancID(),
				listID:  i,
			})
		}
	}
	it.current = nil
	if it.heap.Len() > 0 {
		it.Next()
	}
}

func (it *VersionedHeapIterator) advance(item HeapItem) (HeapItem, bool) {
	for {
		item.iter.Next()
		if !item.iter.Valid() {
			return HeapItem{}, false
		}
		item.key = item.iter.Key()
		item.value = item.iter.Value()
		item.trancID = item.iter.TrancID()
		if it.maxTrancID != 0 && item.trancID > it.maxTrancID {
			continue
		}
		return item, true
	}
}

func (it *VersionedHeapIterator) Next() {
	for {
		if it.heap.Len() == 0 {
			it.current = nil
			return
		}
		item := heap.Pop(it.heap).(HeapItem)
		if it.maxTrancID != 0 && item.trancID > it.maxTrancID {
			if next, ok := it.advance(item); ok {
				heap.Push(it.heap, next)
			}
			continue
		}
		it.current = &item
		if next, ok := it.advance(item); ok {
			heap.Push(it.heap, next)
		}
		return
	}
}

func (it *VersionedHeapIterator) Valid() bool { return !it.closed && it.current != nil }

func (it *VersionedHeapIterator) Key() string {
	if !it.Valid() {
		return ""
	}
	return it.current.key
}

func (it *VersionedHeapIterator) Value() string {
	if !it.Valid() {
		return ""
	}
	return it.current.value
}

func (it *VersionedHeapIterator) TrancID() uint64 {
	if !it.Valid() {
		return 0
	}
	return it.current.trancID
}

func (it *VersionedHeapIterator) Seek(key string) bool {
	if it.closed {
		return false
	}
	for _, iter := range it.iters {
		_ = iter.Seek(key)
	}
	it.rebuild()
	return it.Valid()
}

func (it *VersionedHeapIterator) SeekFirst() bool {
	if it.closed {
		return false
	}
	for _, iter := range it.iters {
		_ = iter.SeekFirst()
	}
	it.rebuild()
	return it.Valid()
}

func (it *VersionedHeapIterator) Close() error {
	if it.closed {
		return nil
	}
	for _, iter := range it.iters {
		_ = iter.Close()
	}
	it.closed = true
	it.current = nil
	it.heap = nil
	return nil
}
