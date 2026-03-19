package lsm

import (
	"encoding/binary"
	"os"
	"hash/crc32"
	"fmt"
	"io"
)

/*
* SSTable: SSTable
*/

type SSTable struct {
	// FileObj file
	file 		*os.File
	metas 		[]BlockMeta
	bloomOffset	uint32
	metaOffset	uint32
	sstID 		uint16
	blockCap 	uint16
	firstKey 	string
	lastKey 	string
	bloomFilter *BloomFilter
	blockCache 	*BlockCache
	minTrancID 	uint64
	maxTrancID 	uint64
}

// Open: 将SST文件的元信息进行解码和加载
func OpenSST(sstID uint16, file *os.File, blockcache *BlockCache, blockCap uint16) *SSTable {
	data, result := os.ReadFile(file.Name())
	if result != nil || len(data) < 24 {
		fmt.Println("OpenSST: SSTable file error")
		return nil
	}
	
	// 解码额外信息
	extraInfo := data[len(data)-24:]
	metaoffset := binary.LittleEndian.Uint32(extraInfo[0:4])
	bloomoffset := binary.LittleEndian.Uint32(extraInfo[4:8])
	mintrancid := binary.LittleEndian.Uint64(extraInfo[8:16])
	maxTrancid := binary.LittleEndian.Uint64(extraInfo[16:24])

	// 解码bloom
	bloomdata := data[bloomoffset:len(data)-24]
	bloomfilter := DecodeBloomFilter(bloomdata)

	// 解码meta
	metadata := data[metaoffset:bloomoffset]
	metadatalen := len(metadata)
	if metadatalen < 6 {
		// 数据长度不足以包含校验码和数字
		fmt.Println("OpenSST: SSTable file error")
		return nil
	}
	metanum := binary.LittleEndian.Uint16(metadata[0:2])
	// 提取原始数据计算出的 Checksum
	actualData := metadata[2:metadatalen-4]
	expectChecksum := binary.LittleEndian.Uint32(metadata[metadatalen-4:])
		
	if crc32.ChecksumIEEE(actualData) != expectChecksum {
		fmt.Println("OpenSST: SSTable file error")
		return nil 
	}
	// 解码
	metas := DecodeMeta(actualData)

	firstkey := ""
	lastkey := ""
	if metanum != 0 {
		firstkey = metas[0].firstKey
		lastkey = metas[metanum-1].lastKey
	}
	if metanum > uint16(len(metas)) {
		fmt.Println("OpenSST: SSTable file error")
		return nil
	}

	sst := &SSTable{
		file: 			file,
		metas: 			metas,
		bloomOffset:	bloomoffset,
		metaOffset:		metaoffset,
		sstID: 			sstID,
		blockCap: 		blockCap,
		firstKey: 		firstkey,
		lastKey: 		lastkey,
		bloomFilter: 	bloomfilter,
		blockCache: 	blockcache,
		minTrancID: 	mintrancid,
		maxTrancID: 	maxTrancid,
	}

	return sst
}

// Close releases the underlying SST file handle.
func (sst *SSTable) Close() error {
	if sst == nil || sst.file == nil {
		return nil
	}
	err := sst.file.Close()
	sst.file = nil
	return err
}

// ReadBlock
func (sst *SSTable) ReadBlock(blockIdx int64) *Block {
	if blockIdx >= int64(len(sst.metas)) || blockIdx < 0 {
		fmt.Println("ReadBlock: index out of range")
		return nil
	}

	// 从缓存池获取block
	if sst.blockCache != nil {
		block := sst.blockCache.Get(sst.sstID, uint16(blockIdx));
		if block != nil {
			return block
		}
	}

	// 从文件系统中读取block
	blockoffset := sst.metas[blockIdx].offset
	datasize := sst.metas[blockIdx].blockSize + 4
	blockdata := make([]byte, datasize)
	_, err := sst.file.ReadAt(blockdata, int64(blockoffset))
	if err != nil && err != io.EOF {
		fmt.Println("ReadBlock: SSTable file error")
		return nil
	}
	block := DecodeBlock(blockdata, sst.blockCap, true)
	// 放入缓存中
	if sst.blockCache != nil {
		sst.blockCache.Put(sst.sstID, uint16(blockIdx), block)
	}

	return block
}

// FindBlockIdx: 根据key查询Block
func (sst *SSTable) FindBlockIdx(key string) int {
	if len(sst.metas) == 0 {
		return -1
	}
	if sst.lastKey < key {
		return -1
	}

	// 查找第一个 lastKey >= key 的 block
	left := 0
	right := len(sst.metas) - 1
	for left < right {
		mid := (left + right) / 2
		if sst.metas[mid].lastKey < key {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return left
}

// Get: 根据key获取value
func (sst *SSTable) Get(key string, trancID uint64) *SSTIterator {
	// 先用布隆过滤器判断存不存在
	if sst.bloomFilter != nil && !sst.bloomFilter.Contains(key) {
		return &SSTIterator{sst: sst, blockIt: nil, maxTrancID: trancID}
	}
	si :=  NewSSTIterator(sst, trancID)
	// todo: 这里maxTrancID应该为修改过这个值的最大trancID
	si.Seek(key)
	if !si.Valid() {
		fmt.Printf("SSTable: Get key false")
	}
	return si
}

// ItersMonotonyPredicate: 谓词查询
func (sst *SSTable) ItersMonotonyPredicate(trancID uint64, predicate PredicateFunc) (*SSTIterator, *SSTIterator, bool) {
	if predicate(sst.firstKey) < 0 || predicate(sst.lastKey) > 0 {
		fmt.Printf("ItersMonotonyPredicate: predicate out of range in SSTable")
		return nil, nil, false
	}

	left := int64(0)
	right := int64(len(sst.metas) - 1)
	mid := int64(0)

	for left <= right {
		mid = (left + right) / 2
		midMeta := sst.metas[mid]
		if predicate(midMeta.firstKey) < 0 {
			right = mid - 1
		} else if predicate(midMeta.lastKey) > 0 {
			left = mid + 1
		} else {
			break
		}
	}
	if left > right {
		fmt.Printf("ItersMonotonyPredicate: predicate out of range in SSTable")
		return nil, nil, false
	}

	for left = mid; left >= 0; left-- {
		meta := sst.metas[left]
		if predicate(meta.lastKey) > 0 {
			break
		}
	}
	left ++ 

	for right = mid; right < int64(len(sst.metas)); right++ {
		meta := sst.metas[right]
		if predicate(meta.firstKey) < 0 {
			break
		}
	}

	if left < 0 {
		left = 0
	}
	leftIter := NewSSTIterator(sst, trancID)
	leftIter.Seek(sst.metas[left].firstKey)

	if right >= int64(len(sst.metas)) { 
		return leftIter, nil, true
	}
	rightIter := NewSSTIterator(sst, trancID)
	rightIter.Seek(sst.metas[right].firstKey)

	return leftIter, rightIter, true
}

/*
* SSTBuilder: SSTable构建者	
*/

type SSTBuilder struct { 
	block 		Block
	firstKey	string
	lastKey 	string
	metas 		[]BlockMeta
	data 		[]byte
	blockCap 	uint16
	bloomFilter *BloomFilter
	blockCache 	*BlockCache
	maxTrancID	uint64
	minTrancID	uint64
}

func NewSSTBuilder(blockCap uint16, bloomfilter *BloomFilter, blockcache *BlockCache, hasBloom bool) *SSTBuilder { 
	sstb := SSTBuilder{
		block: *NewBlock(blockCap), 
		blockCap: blockCap,
		blockCache: blockcache,
	}
	if hasBloom { 
		sstb.bloomFilter = bloomfilter
	}
	return &sstb
}

// Add 添加数据
func (sb *SSTBuilder) Add(key string, value string, trancID uint64) {
	if sb.firstKey == "" {
		sb.firstKey = key
	}

	sb.lastKey = key

	if sb.minTrancID == 0 || trancID < sb.minTrancID {
		sb.minTrancID = trancID
	}

	if trancID > sb.maxTrancID {
		sb.maxTrancID = trancID
	}

	result := sb.block.AddEntry(key, value, trancID, false)
	if !result {
		sb.FinishBlock()
		sb.block.AddEntry(key, value, trancID, true)
	}

	if sb.bloomFilter != nil {
		sb.bloomFilter.Add(key)
	}
}

// FinshBlock 将block写入data中
func (sb *SSTBuilder) FinishBlock() {
	if sb.block.EntryCount() == 0 {
		return
	}
	
	offset := uint32(len(sb.data))
	sb.data = append(sb.data, sb.block.Encode(true)...)

	firstkey, _ := sb.block.GetKeyAt(0)
	lastkey, _ := sb.block.GetKeyAt(sb.block.EntryCount() - 1)
	blockmeta := NewBlockMeta(offset, uint32(sb.block.Size()), firstkey, lastkey)

	sb.metas = append(sb.metas, blockmeta)
	sb.block = *NewBlock(sb.blockCap)
}

// Build 构建SSTable
func (sb *SSTBuilder) Build(sstID uint16, path string) *SSTable {
	if sb.block.EntryCount() > 0 {
		sb.FinishBlock()
	}

	metaoffset := uint32(len(sb.data))

	// 写meta
	metanum := uint16(len(sb.metas))
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, metanum)
	sb.data = append(sb.data, buf...)
	metadata := EncodeMeta(sb.metas)
	sb.data = append(sb.data, metadata...)

	// 写meta哈希值
	checksum := crc32.ChecksumIEEE(metadata)
	buf = make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, checksum)
	sb.data = append(sb.data, buf...)

	// 写bloom
	if sb.bloomFilter !=nil {
		bloomdata := sb.bloomFilter.Encode()
		sb.data = append(sb.data, bloomdata...)
	}
	bloomoffset := metaoffset + uint32(len(metadata)) + 6

	// 写额外信息
	buf = make([]byte, 24)
	binary.LittleEndian.PutUint32(buf[0:4], metaoffset)
	binary.LittleEndian.PutUint32(buf[4:8], bloomoffset)
	binary.LittleEndian.PutUint64(buf[8:16], sb.minTrancID)
	binary.LittleEndian.PutUint64(buf[16:24], sb.maxTrancID)
	sb.data = append(sb.data, buf...)

	// 创建文件
	file, _ := os.Create(path)
	file.Write(sb.data)

	return &SSTable{
		file: 			file,
		metas: 			sb.metas,
		bloomOffset:	bloomoffset,
		metaOffset:		metaoffset,
		blockCap:		sb.blockCap,
		sstID: 			sstID,
		firstKey: 		sb.firstKey,
		lastKey: 		sb.lastKey,
		bloomFilter: 	sb.bloomFilter,
		blockCache: 	sb.blockCache,
		maxTrancID: 	sb.maxTrancID,
		minTrancID: 	sb.minTrancID,
	}
}

/*
* SSTIterator: SSTable迭代器
*/

type SSTIterator struct {
	sst 		*SSTable
	blockIdx 	uint16
	maxTrancID 	uint64
	blockIt 	*BlockIterator
}

// NewSSTIterator: 创建迭代器
func NewSSTIterator(sst *SSTable, trancID uint64) *SSTIterator { 
	si := &SSTIterator{
		sst: sst,
		maxTrancID: trancID,
	}
	si.SeekFirst()
	return si
}

// Valid: 判断迭代器是否有效
func (si *SSTIterator) Valid() bool { 
	if si.blockIt == nil {
		return false
	}

	return si.blockIt.Valid()
}

// Next: 迭代器前进
func (si *SSTIterator) Next() { 
	if si.blockIt == nil {
		return
	}

	si.blockIt.Next()

	if !si.blockIt.Valid() {
		start := int(si.blockIdx) + 1
		si.seekFromBlock(start, "")
	}
}

// Key: 获取当前key
func (si *SSTIterator) Key() string { 
	if si.blockIt == nil {
		return ""
	}

	return si.blockIt.Key()
}

// Value: 获取当前value
func (si *SSTIterator) Value() string { 
	if si.blockIt == nil {
		return ""
	}

	return si.blockIt.Value()
}

// TrancID 获取当前事务ID
func (si *SSTIterator) TrancID() uint64 {
	if si.blockIt == nil {
		return 0
	}
	return si.blockIt.TrancID()
}

// Close: 释放迭代器
func (si *SSTIterator) Close() error { 
	if si.blockIt != nil {
		si.blockIt.Close()
	}

	return nil
}

// SeekFirst: 迭代器指向第一个元素
func (si *SSTIterator) SeekFirst() bool {
	if si.sst.metas == nil || len(si.sst.metas) == 0 {
		si.blockIt = nil
		return false
	}

	return si.seekFromBlock(0, si.sst.firstKey)
}

// Seek: 迭代器指向指定元素
func (si *SSTIterator) Seek(key string) bool {
	if si.sst.metas == nil || len(si.sst.metas) == 0 {
		si.blockIt = nil
		return false
	}

	blockIdx := si.sst.FindBlockIdx(key)
	if blockIdx < 0 { 
		si.blockIt = nil
		return false
	}
	return si.seekFromBlock(blockIdx, key)
}

// seekFromBlock finds the first block (starting at idx) that has a visible entry.
// If key is empty, it seeks using each block's firstKey.
func (si *SSTIterator) seekFromBlock(idx int, key string) bool {
	if si.sst == nil || si.sst.metas == nil || idx < 0 {
		si.blockIt = nil
		return false
	}

	for i := idx; i < len(si.sst.metas); i++ {
		seekKey := key
		if i != idx || seekKey == "" {
			seekKey = si.sst.metas[i].firstKey
		}
		block := si.sst.ReadBlock(int64(i))
		if block == nil {
			continue
		}
		it := NewBlockIterator(block, seekKey, si.maxTrancID)
		if it != nil && it.Valid() {
			si.blockIdx = uint16(i)
			si.blockIt = it
			return true
		}
	}

	si.blockIt = nil
	return false
}


