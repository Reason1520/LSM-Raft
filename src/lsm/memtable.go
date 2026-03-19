package lsm

import (
	"container/list"
	"sync"
)

const (
	PUT    = "put"
	GET    = "get"
	REMOVE = "remove"
)

type KV struct {
	Key   string
	Value string
}

/*
* Memtable 是一个内存表，用于存储键值对。
 */

type Memtable struct {
	currentTable     *SkipList
	frozenTableQueue list.List
	frozenBytes      int
	maxTableSize     int
	frozenMutex      sync.Mutex
	curMutex         sync.Mutex
}

func NewMemtable() *Memtable {
	mt := new(Memtable)
	mt.currentTable = NewSkipList(16)
	mt.frozenTableQueue.Init()
	mt.frozenBytes = 0
	mt.maxTableSize = 67108864 // Calculated from 64 * 1024 * 1024
	return mt
}

// SetMaxTableSize sets the max in-memory size before freezing.
func (mt *Memtable) SetMaxTableSize(bytes int) {
	mt.maxTableSize = bytes
}

// NewIterator 创建一个迭代器
func (mt *Memtable) NewMemtableIterator(skipDelete bool, trancID uint64) *HeapIterator {
	var iters []Iterator

	mt.curMutex.Lock()
	it := mt.currentTable.NewIterator(nil, trancID)
	it.Seek("")
	iters = append(iters, it)
	mt.curMutex.Unlock()

	mt.frozenMutex.Lock()
	for e := mt.frozenTableQueue.Front(); e != nil; e = e.Next() {
		it := e.Value.(*SkipList).NewIterator(nil, trancID)
		it.Seek("")
		iters = append(iters, it)
	}
	mt.frozenMutex.Unlock()

	return NewHeapIterator(iters, skipDelete, trancID)
}

// Put 将一个键值对插入 Memtable 中
func (mt *Memtable) Put(key string, value string, trancID uint64) {
	if mt.currentTable == nil {
		mt.currentTable = NewSkipList(16)
	}
	// 检查是否要冻结当前表
	mt.FrozenCurTable()

	mt.currentTable.Put(key, value, trancID)
}

// PutLock Put的带锁版本
func (mt *Memtable) PutLock(key string, value string, trancID uint64) {
	if mt.currentTable == nil {
		mt.curMutex.Lock()
		mt.currentTable = NewSkipList(16)
		mt.curMutex.Unlock()
	}
	// 检查是否要冻结当前表
	mt.curMutex.Lock()
	mt.FrozenCurTableLock()

	mt.currentTable.Put(key, value, trancID)
	mt.curMutex.Unlock()
}

// PutBatch 批量插入键值对
func (mt *Memtable) PutBatch(KVs []KV, trancID uint64) {
	if mt.currentTable == nil {
		mt.curMutex.Lock()
		mt.currentTable = NewSkipList(16)
		mt.curMutex.Unlock()
	}
	// 检查是否要冻结当前表
	mt.curMutex.Lock()
	mt.FrozenCurTableLock()

	for _, kv := range KVs {
		mt.currentTable.Put(kv.Key, kv.Value, trancID)
	}
	mt.curMutex.Unlock()
}

// CurGet 获取当前表中的键值对
func (mt *Memtable) CurGet(key string, trancID uint64) *SkipListIterator {
	it := mt.currentTable.Get(key, trancID)
	return it
}

// FrozenGet 获取冻结表中的键值对
func (mt *Memtable) FrozenGet(key string, trancID uint64) *SkipListIterator {
	if mt.frozenTableQueue.Len() == 0 {
		return mt.currentTable.NewIterator(nil, trancID)
	}

	for e := mt.frozenTableQueue.Front(); e != nil; e = e.Next() {
		table := e.Value.(*SkipList)
		it := table.Get(key, trancID)
		if it.Valid() && it.Key() == key {
			// 找到即返回，因为这是当前能找到的最新的版本
			return it
		}
	}
	return mt.currentTable.NewIterator(nil, trancID)
}

// GetLock 获取键值对的带锁版本
func (mt *Memtable) GetLock(key string, trancID uint64) *SkipListIterator {
	// 先从当前表中找
	mt.curMutex.Lock()
	it := mt.CurGet(key, trancID)
	mt.curMutex.Unlock()
	if it.Valid() && it.Key() == key {
		return it
	} else {
		// 再从冻结表中找
		mt.frozenMutex.Lock()
		defer mt.frozenMutex.Unlock()
		return mt.FrozenGet(key, trancID)
	}
}

// Get 获取键值对
func (mt *Memtable) Get(key string, trancID uint64) *SkipListIterator {
	// 先从当前表中找
	it := mt.CurGet(key, trancID)
	if it.Valid() && it.Key() == key {
		return it
	} else {
		// 再从冻结表中找
		return mt.FrozenGet(key, trancID)
	}
}

// Remove 从 Memtable 中删除一个键值对
func (mt *Memtable) Remove(key string, trancID uint64) {
	mt.Put(key, "", trancID)
}

// RemoveLock Remove的带锁版本
func (mt *Memtable) RemoveLock(key string, trancID uint64) {
	mt.PutLock(key, "", trancID)
}

// RemoveBatch 批量删除键值对
func (mt *Memtable) RemoveBatch(keys []string, trancID uint64) {
	var KVs []KV
	for _, key := range keys {
		KVs = append(KVs, KV{Key: key, Value: ""})
	}
	mt.PutBatch(KVs, trancID)
}

// FrozenCurTable 冻结活跃表
func (mt *Memtable) FrozenCurTable() {
	if mt.currentTable.sizeByte > mt.maxTableSize {
		// 如果当前表的size大于maxsize，切换新表
		mt.frozenTableQueue.PushFront(mt.currentTable)
		mt.frozenBytes += mt.currentTable.sizeByte
		mt.currentTable = NewSkipList(16)
	}
}

// FrozenCurTableLock 冻结活跃表的带锁版本，调用时需要先获取curMutex锁
func (mt *Memtable) FrozenCurTableLock() {
	if mt.currentTable.sizeByte > mt.maxTableSize {
		// 如果当前表的size大于maxsize，切换新表
		mt.frozenMutex.Lock()
		mt.frozenTableQueue.PushFront(mt.currentTable)
		mt.frozenBytes += mt.currentTable.sizeByte
		mt.frozenMutex.Unlock()
		mt.currentTable = NewSkipList(16)
	}
}

// PopOldFrozenTable 获取旧冻结表
func (mt *Memtable) PopOldFrozenTable() *SkipList {
	if mt.frozenTableQueue.Len() == 0 {
		return nil
	}
	element := mt.frozenTableQueue.Back()
	skiplist := element.Value.(*SkipList)
	mt.frozenTableQueue.Remove(element)

	return skiplist
}

// PopOldFrozenTableLock 获取旧冻结表的带锁版本
func (mt *Memtable) PopOldFrozenTableLock() *SkipList {
	mt.frozenMutex.Lock()
	defer mt.frozenMutex.Unlock()

	if mt.frozenTableQueue.Len() == 0 {
		return nil
	}
	element := mt.frozenTableQueue.Back()
	skiplist := element.Value.(*SkipList)
	mt.frozenTableQueue.Remove(element)

	return skiplist
}

// ItersPreffix 获取指定前缀的迭代器
func (mt *Memtable) ItersPreffix(prefix string, trancID uint64) *HeapIterator {
	it := mt.NewMemtableIterator(true, trancID)
	it.Seek(prefix)
	return it
}

// ItersMonotonyPredicate 获取谓词查询的迭代器
func (mt *Memtable) ItersMonotonyPredicate(predicate PredicateFunc, trancID uint64) (*HeapIterator, *HeapIterator, bool) {
	it := mt.NewMemtableIterator(true, trancID)
	// 找 start
	for it.Valid() {
		r := predicate(it.Key())

		if r == 0 {
			break
		}

		if r < 0 {
			it.Close()
			return nil, nil, false
		}

		it.Next()
	}

	if !it.Valid() {
		it.Close()
		return nil, nil, false
	}

	startKey := it.Key()
	start := mt.NewMemtableIterator(true, trancID)
	start.Seek(startKey)

	end := mt.NewMemtableIterator(true, trancID)
	end.Seek(startKey)

	// 找 end
	for end.Valid() {
		if predicate(end.Key()) < 0 {
			return start, end, true
		}

		end.Next()
	}

	return start, nil, true
}
