package lsm

import (
	"container/list"
	"sync"
)

type CacheKey struct {
	sstID 	uint16
	blockID	uint16
}

type CacheItem struct {
	cacheKey 	CacheKey
	cacheBlock 	*Block
	accessCount	int
}

type BlockCache struct {
	capacity 			uint16
	k 					uint16
	cacheListGreaterK	*list.List
	cacheListLessK		*list.List
	
	// map -> list node
	cacheMap map[CacheKey]*list.Element

	// 统计
	totalRequests int
	hitRequests   int

	mutex sync.Mutex
}

// 创建缓存
func NewBlockCache(capacity uint16, k uint16) *BlockCache {
	cache := &BlockCache{
		capacity:			capacity,
		k: 					k,
		cacheListGreaterK:	list.New(),
		cacheListLessK:		list.New(),
		cacheMap:			make(map[CacheKey]*list.Element),
	}
	return cache
}

// Put: 插入缓存
func (bc *BlockCache) Put(sstID uint16, blockID uint16, block *Block) {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	key := CacheKey{sstID, blockID}

	// 缓存项已存在
	if elem, ok := bc.cacheMap[key]; ok {
		item := elem.Value.(*CacheItem)
		item.cacheBlock = block
		bc.updateAccess(elem)
		return
	}

	// 缓存项不存在
	// 淘汰
	if len(bc.cacheMap) >= int(bc.capacity) {
		bc.Evict()
	}

	item := &CacheItem{
		cacheKey:		key,
		cacheBlock:		block,
		accessCount:	1,
	}

	elem := bc.cacheListLessK.PushFront(item)
	bc.cacheMap[key] = elem
}

// Get: 获取缓存项
func (bc *BlockCache) Get(sstID uint16, blockID uint16) *Block {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	bc.totalRequests++
	key := CacheKey{sstID, blockID}
	elem, ok := bc.cacheMap[key]
	if !ok {
		// 缓存项不存在
		return nil
	}
	bc.hitRequests++
	item := elem.Value.(*CacheItem)
	bc.updateAccess(elem)

	return item.cacheBlock
}

// UpdateAccess: 更新缓存项的访问次数
func (bc *BlockCache) updateAccess(elem *list.Element) {

	item := elem.Value.(*CacheItem)

	// 从原链表移除
	if item.accessCount < int(bc.k) {
		bc.cacheListLessK.Remove(elem)
	} else {
		bc.cacheListGreaterK.Remove(elem)
	}

	// 更新访问次数
	if item.accessCount < int(bc.k) {
		item.accessCount++
	}

	// 放入新链表
	if item.accessCount < int(bc.k) {
		newElem := bc.cacheListLessK.PushFront(item)
		bc.cacheMap[item.cacheKey] = newElem
	} else {
		newElem := bc.cacheListGreaterK.PushFront(item)
		bc.cacheMap[item.cacheKey] = newElem
	}
}

// Evict: 淘汰缓存项
func (bc *BlockCache) Evict() {

	// 优先淘汰 lessK
	if bc.cacheListLessK.Len() > 0 {

		elem := bc.cacheListLessK.Back()
		item := elem.Value.(*CacheItem)

		delete(bc.cacheMap, item.cacheKey)
		bc.cacheListLessK.Remove(elem)

		return
	}

	// 否则淘汰 greaterK
	if bc.cacheListGreaterK.Len() > 0 {

		elem := bc.cacheListGreaterK.Back()
		item := elem.Value.(*CacheItem)

		delete(bc.cacheMap, item.cacheKey)
		bc.cacheListGreaterK.Remove(elem)
	}
}

// HitRate: 获取命中率
func (bc *BlockCache) HitRate() float64 {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	if bc.totalRequests == 0 {
		return 0
	}

	return float64(bc.hitRequests) / float64(bc.totalRequests)
}