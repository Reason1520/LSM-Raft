package lsm

import "sort"

type LevelIterator struct {
	heap *HeapIterator
}

// NewLevelIterator: 创建 LevelIterator
func NewLevelIterator(engine *LSMEngine, maxTrancID uint64) *LevelIterator {
	iters := make([]Iterator, 0)

	// Level0: newest SST first
	if l0, ok := engine.levelSSTID[0]; ok && l0.Len() > 0 {
		ids := make([]uint16, 0, l0.Len())
		for e := l0.Front(); e != nil; e = e.Next() {
			ids = append(ids, e.Value.(uint16))
		}
		sort.Slice(ids, func(i, j int) bool {
			return ids[i] > ids[j]
		})
		for _, id := range ids {
			sst := engine.SSTs[id]
			iters = append(iters, NewSSTIterator(sst, maxTrancID))
		}
	}

	// Level1+
	for lvl := uint16(1); lvl <= engine.curMaxLevel; lvl++ {
		l, ok := engine.levelSSTID[lvl]
		if !ok || l.Len() == 0 {
			continue
		}

		ids := make([]uint16, 0, l.Len())
		for e := l.Front(); e != nil; e = e.Next() {
			ids = append(ids, e.Value.(uint16))
		}
		// Prefer newer SSTs for duplicate keys within the same level.
		sort.Slice(ids, func(i, j int) bool {
			return ids[i] > ids[j]
		})

		for _, id := range ids {
			sst := engine.SSTs[id]
			iters = append(iters, NewSSTIterator(sst, maxTrancID))
		}
	}

	return &LevelIterator{
		heap: NewHeapIterator(iters, true, maxTrancID),
	}
}

func (it *LevelIterator) Next() { it.heap.Next() }

func (it *LevelIterator) Valid() bool { return it.heap.Valid() }

func (it *LevelIterator) Key() string { return it.heap.Key() }

func (it *LevelIterator) Value() string { return it.heap.Value() }

func (it *LevelIterator) TrancID() uint64 { return it.heap.TrancID() }

func (it *LevelIterator) Close() error { return it.heap.Close() }

func (it *LevelIterator) Seek(key string) bool { return it.heap.Seek(key) }

func (it *LevelIterator) SeekFirst() bool { return it.heap.SeekFirst() }
