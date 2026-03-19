package lsm

import "sort"

/*
* ConcactIterator: 用于连接某一层Level的多个SST
* 此处默认多个SST是不重叠且排序的, 因此使用于Level > 0的SST的连接
*/

type ConcactIterator struct {
	curIter 	Iterator	// 当前cur_idx指向的SST的迭代器
	curIdx 		uint16		// 当前迭代器指向的SST在ssts中的索引
	ssts 		[]*SSTable	// 这一层所有SST句柄的切片
	maxTrancID 	uint64
	closed 		bool
}

// 创建一个ConcactIterator
func NewConcactIterator(ssts []*SSTable, maxTrancID uint64) *ConcactIterator {
	if len(ssts) > 1 {
		sort.Slice(ssts, func(i, j int) bool {
			return ssts[i].firstKey < ssts[j].firstKey
		})
	}
	it := &ConcactIterator{
		curIdx:		0,
		ssts:		ssts,
		maxTrancID:	maxTrancID,
		closed:		false,
	}
	if len(ssts) > 0 {
		it.curIter = NewSSTIterator(ssts[0], maxTrancID)
	}

	return it
}

// Valid: 判断当前迭代器是否有效
func (it *ConcactIterator) Valid() bool {
	if it.closed || it.curIter == nil {
		return false
	}

	return it.curIter.Valid()
}

// Next: 迭代到下一个元素
func (it *ConcactIterator) Next() {
	if !it.Valid() {
		return
	}

	// 移动到当前SST的迭代器的下一个Block
	it.curIter.Next()

	if !it.curIter.Valid() {
		// 如果当前SST迭代器走到了末尾，则移动到下一个SST
		it.curIdx++
		if it.curIdx >= uint16(len(it.ssts)) {
			// 如果迭代器已经到了该层，则置空并返回
			it.curIter = nil
			return
		}

		it.curIter = NewSSTIterator(it.ssts[it.curIdx], it.maxTrancID)
	}
}

// Key: 获取当前迭代器的key
func (it *ConcactIterator) Key() string {
	if !it.Valid() {
		return ""
	}

	return it.curIter.Key()
}

// Value: 获取当前迭代器的value
func (it *ConcactIterator) Value() string {
	if !it.Valid() {
		return ""
	}

	return it.curIter.Value()
}

// TrancID 获取当前事务ID
func (it *ConcactIterator) TrancID() uint64 {
	if !it.Valid() {
		return 0
	}
	return it.curIter.TrancID()
}

// Close: 释放迭代器
func (it *ConcactIterator) Close() error {
	if it.closed {
		return nil
	}

	it.closed = true

	if it.curIter != nil {
		return it.curIter.Close()
	}

	return nil
}

// Seek: 将迭代器定位到第一个大于等于 key 的位置
func (it *ConcactIterator) Seek(key string) bool {
	if it.ssts == nil || len(it.ssts) == 0 {
		it.curIter = nil
		return false
	}

	idx := 0
	// 移动到第一个大于等于key的SST
	for idx < len(it.ssts) && it.ssts[idx].lastKey < key {
		idx++
	}
	if idx >= len(it.ssts) {
		it.curIter = nil
		return false
	}

	it.curIdx = uint16(idx)
	target := it.ssts[idx]
	iter := NewSSTIterator(target, it.maxTrancID)
	if iter == nil {
		it.curIter = nil
		return false
	}

	it.curIter = iter
	if key <= target.firstKey {
		return iter.Seek(target.firstKey)
	}
	return iter.Seek(key)
}

// SeekFirst 将迭代器定位到第一个元素
func (it *ConcactIterator) SeekFirst() bool {
	if it.ssts == nil || len(it.ssts) == 0 {
		it.curIter = nil
		return false
	}

	it.curIdx = 0
	iter := NewSSTIterator(it.ssts[0], it.maxTrancID)
	if iter == nil {
		it.curIter = nil
		return false
	}
	it.curIter = iter
	return iter.SeekFirst()
}

