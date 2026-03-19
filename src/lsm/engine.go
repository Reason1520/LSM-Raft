package lsm

import (
	"container/list"
	"os"
	"sync"
	"fmt"
	"strings"
	"sort"
	"math"
)

const (
	BLOCK_CAPACITY = 32768					// Calculated from 32 * 1024
	LSM_SST_LEVEL_RATIO = 4					// 相邻两层的单个SST容量之比, 同时也是单层SST数量的阈值
	LSM_TOL_MEM_SIZE_LIMIT = 67108864		// Calculated from 64 * 1024 * 1024
	LSM_PER_MEM_SIZE_LIMIT = 4194304 		// alculated from 4 * 1024 * 1024  SST基础容量
	LSM_BLOCK_CACHE_CAPACITY = 1024			// block缓存大小
	LSM_BLOCK_CACHE_K = 8					// block缓存的LRU-K算法的k值
	BLOOM_FILTER_EXPECTED_SIZE = 65536		// 布隆过滤器中存储的元素数量
	BLOOM_FILTER_EXPECTED_ERROR_RATE = 0.1 	// 布隆过滤器允许的错误率
)

type VwithID struct {
	value 	string
	trancID	uint64
}

type LSM struct {
	engine *LSMEngine
}

type LSMEngine struct {
	dataDir 	string					// 数据目录
	memtable 	*Memtable				// 整个LSM Tree引擎的内存表部分
	levelSSTID 	map[uint16]*list.List	// 从level到这一层的SSTID数组, 每一个SST由一个sst_id唯一表示
	SSTs 		map[uint16]*SSTable		// SSTID到SST的映射
	SSTsMutex 	sync.RWMutex			// SSTs的读写锁
	blockCache 	*BlockCache				// block缓存
	nextSSTID 	uint16					// 每次分配sst_id时next_sst_id都会自增1
	curMaxLevel	uint16					// 当前SST的最大的level
}

func NewLSMEngine(dataDir string) *LSMEngine {
	engine := &LSMEngine{
		dataDir: 		dataDir,
		memtable: 		NewMemtable(),
		levelSSTID:		make(map[uint16]*list.List),
		SSTs: 			make(map[uint16]*SSTable),
		blockCache:		NewBlockCache(LSM_BLOCK_CACHE_CAPACITY, LSM_BLOCK_CACHE_K),
		nextSSTID:		0,
		curMaxLevel:	0,
	}
	
	// 初始化数据目录
	os.MkdirAll(dataDir, os.ModePerm)

	files, err := os.ReadDir(dataDir)
	if err != nil {
		return engine
	}

	type sstInfo struct {
		id    		uint16
		level 		uint16
		name  		string
		sst  		*SSTable
	}

	var infos []sstInfo

	// 收集所有 SST 信息
	for _, file := range files {

		name := file.Name()

		if !strings.HasPrefix(name, "sst_") {
			continue
		}

		var id uint16
		var level uint16

		_, err := fmt.Sscanf(name, "sst_%d.%d", &id, &level)
		if err != nil {
			continue
		}

		infos = append(infos, sstInfo{
			id:    id,
			level: level,
			name:  name,
		})
	}

	levelSSTs := make(map[uint16][]*SSTable)

	// 加载 SST
	for i := range infos {
		path := dataDir + "/" + infos[i].name

		f, err := os.Open(path)
		if err != nil {
			continue
		}

		sst := OpenSST(infos[i].id, f, engine.blockCache, BLOCK_CAPACITY)
		if sst == nil {
			f.Close()
			continue
		}

		infos[i].sst = sst
		engine.SSTs[infos[i].id] = sst
		levelSSTs[infos[i].level] = append(levelSSTs[infos[i].level], sst)

		if infos[i].id >= engine.nextSSTID {
			engine.nextSSTID = infos[i].id + 1
		}
		if infos[i].level > engine.curMaxLevel {
			engine.curMaxLevel = infos[i].level
		}
	}

	for level, ssts := range levelSSTs {
		if engine.levelSSTID[level] == nil {
			engine.levelSSTID[level] = list.New()
		}
		if level == 0 {
			// L0 需要保持“新->旧”的优先级顺序
			sort.Slice(ssts, func(i, j int) bool {
				return ssts[i].sstID > ssts[j].sstID
			})
		} else {
			// L1+ 需要按 key 有序
			sort.Slice(ssts, func(i, j int) bool {
				return ssts[i].firstKey < ssts[j].firstKey
			})
		}

		for _, sst := range ssts {
			engine.levelSSTID[level].PushBack(sst.sstID)
		}
	}

	return engine
}

// SetMemtableMaxSize sets the memtable size threshold before freezing.
func (lsme *LSMEngine) SetMemtableMaxSize(bytes int) {
	lsme.memtable.SetMaxTableSize(bytes)
}

// Put: 插入数据
func (lsme *LSMEngine) Put(key string, value string, trancID uint64) uint64{
	lsme.memtable.PutLock(key, value, trancID)

	if lsme.memtable.frozenTableQueue.Len() > 0 {
		return lsme.Flush(false)
	}

	return 0
}

// Remove: 删除数据
func (lsme *LSMEngine) Remove(key string, trancID uint64) uint64{
	lsme.memtable.RemoveLock(key, trancID)

	return 0
}

// PutBatch: 批量插入数据
func (lsme *LSMEngine) PutBatch(KVs []KV, trancID uint64) uint64{
	lsme.memtable.PutBatch(KVs, trancID)

	if lsme.memtable.frozenTableQueue.Len() > 0 {
		return lsme.Flush(false)
	}

	return 0
}

// RemoveBatch: 批量删除数据
func (lsme *LSMEngine) RemoveBatch(keys []string, trancID uint64) uint64{
	lsme.memtable.RemoveBatch(keys, trancID)

	return 0
}

// SSTGet: 在sst中查询数据
func (lsme *LSMEngine) SSTGet(key string, trancID uint64) (string, uint64) {
	lsme.SSTsMutex.RLock()
	defer lsme.SSTsMutex.RUnlock()

	for level := uint16(0); level <= lsme.curMaxLevel; level++ {
		if  lsme.levelSSTID[level] == nil {
			continue
		}
		for sstid := lsme.levelSSTID[level].Front(); sstid != nil; sstid = sstid.Next() {
			sst := lsme.SSTs[sstid.Value.(uint16)]
			it := sst.Get(key, trancID)
			if it.Valid() && it.Key() == key {
				return it.Value(), it.TrancID()
			}
		}
	}

	return "", 0
}

// Get: 查询数据, value表示查询到的值, tranc_id表示这个键值对最新的修改事务的的tranc_id
func (lsme *LSMEngine) Get(key string, trancID uint64) (string, uint64) {
	// 尝试从memtable中获取
	it := lsme.memtable.GetLock(key, trancID)
	defer it.Close()

	if it.Valid() && it.Key() == key {
		return it.Value(), it.trancID
	}

	// 尝试从SSTs中获取
	return lsme.SSTGet(key, trancID)
}

// GetBatch
func (lsme *LSMEngine) GetBatch(keys []string, trancID uint64) map[string]VwithID {
	res := make(map[string]VwithID)

	for _, key := range keys {
		// 尝试从memtable中获取
		it := lsme.memtable.GetLock(key, trancID)
		defer it.Close()

		if it.Valid() && it.Key() == key {
			res[key] = VwithID{value: it.Value(), trancID: it.trancID}
			continue
		}

		// 尝试从SSTs中获取
		value, trancID := lsme.SSTGet(key, trancID)
		res[key] = VwithID{value: value, trancID: trancID}
	}

	return res
}

// Flush: 刷盘
func (lsme *LSMEngine) Flush(flushcur bool) uint64 {
	// 获取一个table
	var table *SkipList
	if flushcur {
		table = lsme.memtable.currentTable
	} else {
		table = lsme.memtable.PopOldFrozenTableLock()
	}
	if table == nil {
		return 0
	}

	// 构造SST
	bloomfilter := NewBloomFilter(BLOOM_FILTER_EXPECTED_SIZE, BLOOM_FILTER_EXPECTED_ERROR_RATE)
	builder := NewSSTBuilder(BLOCK_CAPACITY, bloomfilter, lsme.blockCache, true)
	it := table.NewIterator(nil, 0)
	it.Seek("")
	defer it.Close()

	for it.Valid() {
		builder.Add(it.Key(), it.Value(), it.trancID)
		it.Next()
	}

	level := uint16(0)
	path := fmt.Sprintf("%s/sst_%032d.%d", lsme.dataDir, lsme.nextSSTID, level)
	sst := builder.Build(lsme.nextSSTID, path)

	// 添加到SSTs中
	lsme.SSTsMutex.Lock()
	lsme.SSTs[lsme.nextSSTID] = sst

	if lsme.levelSSTID[level] == nil {
		lsme.levelSSTID[level] = list.New()
	}

	lsme.levelSSTID[level].PushFront(lsme.nextSSTID)

	if level > lsme.curMaxLevel {
		lsme.curMaxLevel = level
	}

	lsme.nextSSTID++
	lsme.SSTsMutex.Unlock()

	lsme.ChecksumCompact()

	return 0
}

// Close: 关闭lsme
func (lsme *LSMEngine) Close() {
	// 将memtable中的数据刷盘
	for {
		// 刷frozen table
		if lsme.memtable.frozenTableQueue.Len() == 0 {
			break
		}
		lsme.Flush(false)
	}
	// 刷memtable
	lsme.Flush(true)

	// 关闭文件
	lsme.SSTsMutex.Lock()
	defer lsme.SSTsMutex.Unlock()

	for _, sst := range lsme.SSTs {
		if sst.file != nil {
			sst.file.Close()
		}
	}
}

// GetSSTSize: 获取任意一层SST的容量
func (lsm *LSMEngine) GetSSTSize(level uint16) int {
	return int(
		float64(LSM_PER_MEM_SIZE_LIMIT) *
			math.Pow(float64(LSM_SST_LEVEL_RATIO), float64(level)),
	)
}

// GenSSTFromIter: 从一个迭代器中构造新的SST
func (lsme *LSMEngine) GenSSTFromIter(baseit Iterator, targetsize int, targetlevel uint16) []*SSTable {
	// 新的SST
	var res []*SSTable
	bloomfilter := NewBloomFilter(BLOOM_FILTER_EXPECTED_SIZE, BLOOM_FILTER_EXPECTED_ERROR_RATE)
	builder := NewSSTBuilder(BLOCK_CAPACITY, bloomfilter, lsme.blockCache, true)
	curSize := 0

	for baseit.Valid() {
		key := baseit.Key()
		value := baseit.Value()
		trancID := baseit.TrancID()
		builder.Add(key, value, trancID)

		curSize += len(key) + len(value)
		// 如果满足大小超过targetsize，则构造一个SST
		if curSize >= targetsize {
			path := fmt.Sprintf("%s/sst_%032d.%d", lsme.dataDir, lsme.nextSSTID, targetlevel)
			sst := builder.Build(lsme.nextSSTID, path)
			res = append(res, sst)
			lsme.nextSSTID++

			bloomfilter = NewBloomFilter(BLOOM_FILTER_EXPECTED_SIZE, BLOOM_FILTER_EXPECTED_ERROR_RATE)
			builder = NewSSTBuilder(BLOCK_CAPACITY, bloomfilter, lsme.blockCache, true)
			curSize = 0
		}
		baseit.Next()
	}
	if curSize > 0 {
		path := fmt.Sprintf("%s/sst_%032d.%d", lsme.dataDir, lsme.nextSSTID, targetlevel)
		sst := builder.Build(lsme.nextSSTID, path)
		res = append(res, sst)
		lsme.nextSSTID++
	}

	return res
}

// Fulll0l1Compact: 将L0层和L1层的SST合并到L1层
func (lsme *LSMEngine) Full0l1Compact(l0IDs []uint16, l1IDs []uint16) []*SSTable {
	// todo trancID后面再改
	// L0 迭代器
	var l0iters []Iterator
	for _, id := range l0IDs {
		l0sst := lsme.SSTs[id]
		l0iters = append(l0iters, NewSSTIterator(l0sst, 0))
	}

	l0Iter := NewHeapIterator(l0iters, false, 0)

	// L1 迭代器
	var l1sst []*SSTable
	for _, id := range l1IDs {
		l1sst = append(l1sst, lsme.SSTs[id])
	}

	l1Iter := NewConcactIterator(l1sst, 0)

	// merge
	mergeIter := NewTwoMergeIterator(l0Iter, l1Iter, 0)

	targetSize := lsme.GetSSTSize(1)

	return lsme.GenSSTFromIter(mergeIter, targetSize, 1)
}

// FullCommonCompact: 负责其他相邻Level的SST合并
func (lsm *LSMEngine) FullCommonCompact(lxIDs []uint16, lyIDs []uint16, levelY uint16) []*SSTable {
	var sstX []*SSTable
	for _, id := range lxIDs {
		sstX = append(sstX, lsm.SSTs[id])
	}

	var sstY []*SSTable
	for _, id := range lyIDs {
		sstY = append(sstY, lsm.SSTs[id])
	}

	iterX := NewConcactIterator(sstX, 0)
	iterY := NewConcactIterator(sstY, 0)

	merge := NewTwoMergeIterator(iterX, iterY, 0)

	targetSize := lsm.GetSSTSize(levelY)

	return lsm.GenSSTFromIter(merge, targetSize, levelY)
}

// FullCompact: 指定层级的所有 SSTable 压缩到下一层级
func (lsme *LSMEngine) FullCompact(srclevel uint16) {
	if lsme.levelSSTID[srclevel] == nil {
		return
	}

	// 检查目标层级是否需要压缩
	if lsme.levelSSTID[srclevel + 1] != nil {
		if lsme.levelSSTID[srclevel + 1].Len() >=  LSM_SST_LEVEL_RATIO {
			// 如果需要压缩，递归调用
			lsme.FullCompact(srclevel + 1)
		}
	}

	// 获取源层级和目标层级的 SSTable ID
	lxIDs := make([]uint16, 0)
	for iter := lsme.levelSSTID[srclevel].Front(); iter != nil; iter = iter.Next() {
		lxIDs = append(lxIDs, iter.Value.(uint16))
	}
	lyIDs := make([]uint16, 0)
	if  srclevel + 1 <= lsme.curMaxLevel {
		for iter := lsme.levelSSTID[srclevel + 1].Front(); iter != nil; iter = iter.Next() {
			lyIDs = append(lyIDs, iter.Value.(uint16))
		}
	}

	// 合并
	newssts := []*SSTable{}
	if srclevel == 0 {
		// 如果是0层合并
		newssts = lsme.Full0l1Compact(lxIDs, lyIDs)
	} else {
		// 如果不是0层合并
		newssts = lsme.FullCommonCompact(lxIDs, lyIDs, srclevel + 1)
	}

	// 清除旧的SST
	for _, id := range lxIDs {
		if sst, ok := lsme.SSTs[id]; ok && sst != nil {
			path := ""
			if sst.file != nil {
				path = sst.file.Name()
			}
			sst.Close()
			if path != "" {
				_ = os.Remove(path)
			}
		}
		delete(lsme.SSTs, id)
	}
	for _, id := range lyIDs {
		if sst, ok := lsme.SSTs[id]; ok && sst != nil {
			path := ""
			if sst.file != nil {
				path = sst.file.Name()
			}
			sst.Close()
			if path != "" {
				_ = os.Remove(path)
			}
		}
		delete(lsme.SSTs, id)
	}
	lsme.levelSSTID[srclevel] = list.New()
	lsme.levelSSTID[srclevel + 1] = list.New()
	lsme.levelSSTID[srclevel + 1] = list.New()

	// 更新最大层级
	if srclevel + 1 >  lsme.curMaxLevel {
		lsme.curMaxLevel = srclevel + 1
	}

	// 添加新的SST
	if lsme.levelSSTID[srclevel + 1] == nil {
		lsme.levelSSTID[srclevel + 1] = list.New()
	}
	// 按照firstkey从小到大排序
	sort.Slice(newssts, func(i, j int) bool {
		return newssts[i].firstKey < newssts[j].firstKey
	})
	for _, sst := range newssts {
		lsme.SSTs[sst.sstID] = sst
		lsme.levelSSTID[srclevel + 1].PushBack(sst.sstID)
	}
}

// ChecksumCompact: 检查合并
func (lsme *LSMEngine) ChecksumCompact() {
	for level := uint16(0); level <= lsme.curMaxLevel; level++ {
		if lsme.levelSSTID[level] == nil {
			continue
		}

		if lsme.levelSSTID[level].Len() >= LSM_SST_LEVEL_RATIO {
			lsme.FullCompact(level)
		}
	}
}

// Begin: 返回指向第一个键的迭代器
func (lsme *LSMEngine) Begin(tranid uint64) Iterator {
	it := NewLevelIterator(lsme, tranid)
	it.SeekFirst()
	// 后面看要不要处理返回false的情况
	return it
}

// LsmItersMonotonyPredicate: 范围查询
func (lsme *LSMEngine) LsmItersMonotonyPredicate(trancID uint64, predicate PredicateFunc) (Iterator, Iterator, bool) {
	makeIter := func() *TwoMergeIterator {
		memIt := lsme.memtable.NewMemtableIterator(true, trancID)
		levelIt := NewLevelIterator(lsme, trancID)
		return NewTwoMergeIterator(memIt, levelIt, trancID)
	}

	// 找到起始 key
	probe := makeIter()
	if !probe.SeekFirst() {
		probe.Close()
		return nil, nil, false
	}
	for probe.Valid() {
		r := predicate(probe.Key())
		if r == 0 {
			break
		}
		if r < 0 {
			probe.Close()
			return nil, nil, false
		}
		probe.Next()
	}
	if !probe.Valid() {
		probe.Close()
		return nil, nil, false
	}
	startKey := probe.Key()
	probe.Close()

	// 构造 start/end 迭代器
	start := makeIter()
	start.Seek(startKey)

	end := makeIter()
	end.Seek(startKey)
	for end.Valid() {
		if predicate(end.Key()) < 0 {
			return start, end, true
		}
		end.Next()
	}

	return start, nil, true
}
