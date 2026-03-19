package lsm

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"strings"
)

type Entry struct {
	key     string
	value   string
	trancID uint64
}

/*
* Block
 */

type Block struct {
	data     []byte
	offsets  []uint16
	capacity uint16
}

func NewBlock(capacity uint16) *Block {
	return &Block{
		data:     make([]byte, 0),
		offsets:  make([]uint16, 0),
		capacity: capacity,
	}
}

// EntryCount 获取 entry 数量
func (b *Block) EntryCount() int {
	return len(b.offsets)
}

func (b *Block) Size() int {
	return len(b.data) + 2 + 2*len(b.offsets)
}

// Encode 编码
func (b *Block) Encode(withhash bool) []byte {
	buffer := new(bytes.Buffer)
	buffer.Write(b.data)

	for _, offset := range b.offsets {
		binary.Write(buffer, binary.LittleEndian, offset)
	}

	binary.Write(buffer, binary.LittleEndian, uint16(b.EntryCount()))

	res := buffer.Bytes()

	if withhash {
		checksum := crc32.ChecksumIEEE(res)
		binary.Write(buffer, binary.LittleEndian, checksum)
	}

	return buffer.Bytes()
}

// Decode 解码
func DecodeBlock(raw []byte, capacity uint16, withhash bool) *Block {

	length := len(raw)
	// 先进行crc校验
	if withhash {
		if length < 4 {
			return nil // 数据长度不足以包含校验码
		}
		// 提取原始数据计算出的 Checksum
		actualData := raw[:length-4]
		expectChecksum := binary.LittleEndian.Uint32(raw[length-4:])
		
		if crc32.ChecksumIEEE(actualData) != expectChecksum {
			fmt.Println("DecodeBlock: too short")
			return nil 
		}
		// 校验通过，后续解析逻辑只针对除去 Checksum 的数据
		length -= 4
	}

	if length < 2 {
		fmt.Println("DecodeBlock: too short")
		return nil
	}
	// 读取 entry 数量
	offsetCount := int(binary.LittleEndian.Uint16(raw[length-2:]))

	// offsets 起始位置
	offsetStart := length - 2 - offsetCount*2
	if offsetStart < 0 {
		fmt.Println("DecodeBlock: too short")
		return nil
	}

	// 解析 offsets
	offsets := make([]uint16, offsetCount)
	for i := 0; i < offsetCount; i++ {
		pos := offsetStart + i*2
		offsets[i] = binary.LittleEndian.Uint16(raw[pos:])
	}

	// data
	data := make([]byte, offsetStart)
	copy(data, raw[:offsetStart])

	block := &Block{
		data:     data,
		offsets:  offsets,
		capacity: capacity,
	}

	return block
}

// AddEntry 添加 entry
func (b *Block) AddEntry(key string, value string, trancID uint64, forceWrite bool) bool {
	// entry 编码大小
	entrySize := 12 + len(key) + len(value)

	// offset 也要占 2 byte
	newSize := len(b.data) + entrySize + (len(b.offsets)+1)*2 + 2

	// 不允许超过 block size
	if !forceWrite && newSize > int(b.capacity) {
		return false
	}

	// 当前 entry 在 data 中的位置
	offset := uint16(len(b.data))
	b.offsets = append(b.offsets, offset)

	buf := new(bytes.Buffer)

	binary.Write(buf, binary.LittleEndian, uint16(len(key)))
	buf.Write([]byte(key))
	binary.Write(buf, binary.LittleEndian, uint16(len(value)))
	buf.Write([]byte(value))
	binary.Write(buf, binary.LittleEndian, trancID)

	b.data = append(b.data, buf.Bytes()...)

	return true
}

// 从指定编号获取数据
// GetKeyat 从指定编号获取键
func (b *Block) GetKeyAt(index int) (string, bool) {
	if index >= len(b.offsets) {
		fmt.Printf("GetKeyAt: index out of range in block")
		return "", false
	}

	offset := int(b.offsets[index])
	keyLen := binary.LittleEndian.Uint16(b.data[offset:])

	keyStart := offset + 2
	keyEnd := keyStart + int(keyLen)

	return string(b.data[keyStart:keyEnd]), true
}

// GetValueat 从指定编号获取值
func (b *Block) GetValueAt(index int) (string, bool) {
	if index >= len(b.offsets) {
		fmt.Printf("GetValueAt: index out of range in block")
		return "", false
	}

	offset := int(b.offsets[index])

	// key
	keyLen := binary.LittleEndian.Uint16(b.data[offset:])
	keyStart := offset + 2
	keyEnd := keyStart + int(keyLen)

	// value
	valueLen := binary.LittleEndian.Uint16(b.data[keyEnd:])

	valueStart := keyEnd + 2
	valueEnd := valueStart + int(valueLen)

	return string(b.data[valueStart:valueEnd]), true
}

// GetTrancIDat 从指定编号获取事务编号
func (b *Block) GetTrancIDAt(index int) (uint64, bool) {
	if index >= len(b.offsets) {
		fmt.Printf("GetTrancIDAt: index out of range in block")
		return 0, false
	}

	offset := int(b.offsets[index])

	keyLen := binary.LittleEndian.Uint16(b.data[offset:])
	keyStart := offset + 2
	keyEnd := keyStart + int(keyLen)

	valueLen := binary.LittleEndian.Uint16(b.data[keyEnd:])
	valueStart := keyEnd + 2
	valueEnd := valueStart + int(valueLen)

	return binary.LittleEndian.Uint64(b.data[valueEnd:]), true
}

// ParseEntry 解析 entry
func (b *Block) ParseEntry(index int) (string, string, uint64, bool) {
	if index >= len(b.offsets) {
		fmt.Printf("ParseEntry: index out of range in block")
		return "", "", 0, false
	}

	offset := int(b.offsets[index])

	keyLen := binary.LittleEndian.Uint16(b.data[offset:])
	keyStart := offset + 2
	keyEnd := keyStart + int(keyLen)

	valueLen := binary.LittleEndian.Uint16(b.data[keyEnd:])
	valueStart := keyEnd + 2
	valueEnd := valueStart + int(valueLen)

	txID := binary.LittleEndian.Uint64(b.data[valueEnd:])

	key := string(b.data[keyStart:keyEnd])
	value := string(b.data[valueStart:valueEnd])

	return key, value, txID, true
}

// BinarySearch 二分查找
func (b *Block) BinarySearch(key string, trancID uint64) int {

	left := 0
	right := len(b.offsets) - 1

	for left <= right {

		mid := (left + right) / 2

		midKey, _ := b.GetKeyAt(mid)

		if midKey == key {
			return mid
		}

		if midKey < key {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	return -1
}

// FindKeyIndex 找到第一个 key >= target 的位置
func (b *Block) FindKeyIndex(target string) int {

    left := 0
    right := len(b.offsets)

    for left < right {

        mid := (left + right) / 2

        key, _ := b.GetKeyAt(mid)

        if key < target {
            left = mid + 1
        } else {
            right = mid
        }
    }

    return left
}

// Begin 返回迭代器起始位置
func (b *Block) Begin(trancID uint64) *BlockIterator {
	it := &BlockIterator{
		block:      b,
		currentIdx: 0,
		trancID:    trancID,
		cache:      nil,
		close:      false,
	}
	it.advanceToVisible()
	return it
}

// ItersPreffix 返回指定前缀的迭代器
func (b *Block) ItersPreffix(prefix string, trancID uint64) (*BlockIterator, *BlockIterator, bool) {
	start := b.FindKeyIndex(prefix)

	len := len(b.offsets)

	if start >= len {
		fmt.Printf("ItersPreffix: prefix out of range in block")
		return nil, nil, false
	}

	k, _ := b.GetKeyAt(start)

	if !strings.HasPrefix(k, prefix) {
		return nil, nil, false
	}

	end := start

	for end < len {

		k, _ := b.GetKeyAt(end)

		if !strings.HasPrefix(k, prefix) {
			break
		}

		end++
	}

	left := &BlockIterator{
		block: b,
		currentIdx: start,
		trancID: trancID,
	}

	right := &BlockIterator{
		block: b,
		currentIdx: end,
		trancID: trancID,
	}

	return left, right, true
}

func (b *Block) ItersMonotonyPredicate(predicate PredicateFunc, trancID uint64) (*BlockIterator, *BlockIterator, bool) {
	n := len(b.offsets)

	left := 0
	right := n - 1

	found := -1

	for left <= right {

		mid := (left + right) / 2

		k, _ := b.GetKeyAt(mid)

		res := predicate(k)

		if res == 0 {
			found = mid
			break
		}

		if res > 0 {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	if found == -1 {
		fmt.Printf("ItersMonotonyPredicate: predicate out of range in block")
		return nil, nil, false
	}

	// 找左边界
	l := found
	r := found

	for l > 0 {

		k, _ := b.GetKeyAt(l-1)

		if predicate(k) != 0 {
			break
		}

		l--
	}

	// 找右边界
	for r+1 < n {

		k, _ := b.GetKeyAt(r+1)

		if predicate(k) != 0 {
			break
		}

		r++
	}

	leftIter := &BlockIterator{
		block: b,
		currentIdx: l,
		trancID: trancID,
	}

	rightIter := &BlockIterator{
		block: b,
		currentIdx: r + 1,
		trancID: trancID,
	}

	return leftIter, rightIter, true
}

/*
* Block Iterator
 */

type BlockIterator struct {
	block      *Block // 指向所属的block
	currentIdx int    // 当前指向键值对的索引
	trancID    uint64 // 事务编号
	cache      *Entry // 缓存
	close      bool   // 是否关闭
}

// NewBlockIterator 创建迭代器
func NewBlockIterator(block *Block, key string, trancID uint64) *BlockIterator {
	if block == nil {
		fmt.Printf("NewBlockIterator: block is nil")
		return nil
	}
	
	bi := &BlockIterator{
		block:      block,
		trancID:    trancID,
		cache:      nil,
		close:      false,
	}
	if !bi.Seek(key) {
		return bi
	}
	return bi
}

// UpdateCache 更新缓存
func (it *BlockIterator) UpdateCache() {
	it.cache = &Entry{}
	it.cache.key, it.cache.value, it.cache.trancID, _ = it.block.ParseEntry(it.currentIdx)
}

// Next 迭代下一个
func (it *BlockIterator) Next() {
	if it.close {
		fmt.Printf("Next: BlockIterator closed")
		return
	}

	it.currentIdx++
	it.advanceToVisible()
}

// Valid 是否还有效
func (it *BlockIterator) Valid() bool {
	if it.close {
		return false
	}

	return it.currentIdx < it.block.EntryCount()
}

// Key 获取键
func (it *BlockIterator) Key() string {
	if !it.Valid() {
		return ""
	}

	if it.cache == nil {
		it.UpdateCache()
	}

	return it.cache.key
}

// Value 获取值
func (it *BlockIterator) Value() string {
	if !it.Valid() {
		return ""
	}

	if it.cache == nil {
		it.UpdateCache()
	}

	return it.cache.value
}

// TrancID 获取当前事务ID
func (it *BlockIterator) TrancID() uint64 {
	if !it.Valid() || it.cache == nil {
		return 0
	}
	return it.cache.trancID
}

// Close 关闭迭代器
func (it *BlockIterator) Close() error {
	it.close = true
	return nil
}

// Seek 将迭代器定位到第一个大于等于 key 的位置
func (it *BlockIterator) Seek(key string) bool {
	it.currentIdx = it.block.FindKeyIndex(key)
	return it.advanceToVisible()
}

// SeekFirst 将迭代器定位到第一个元素
func (it *BlockIterator) SeekFirst() bool {
	if it.block == nil || it.block.EntryCount() == 0 {
		it.cache = nil
		it.currentIdx = 0
		return false
	}
	it.currentIdx = 0
	return it.advanceToVisible()
}

// advanceToVisible moves to the next entry visible to trancID.
// trancID == 0 means no visibility filtering.
func (it *BlockIterator) advanceToVisible() bool {
	if it.close || it.block == nil {
		it.cache = nil
		return false
	}

	if it.currentIdx >= it.block.EntryCount() {
		it.cache = nil
		return false
	}

	if it.trancID == 0 {
		it.UpdateCache()
		return true
	}

	for it.currentIdx < it.block.EntryCount() {
		tid, ok := it.block.GetTrancIDAt(it.currentIdx)
		if !ok {
			it.cache = nil
			return false
		}
		if tid <= it.trancID {
			it.UpdateCache()
			return true
		}
		it.currentIdx++
	}

	it.cache = nil
	return false
}

/*
* BlockMeta Block的元信息
*/

type BlockMeta struct {
	offset 		uint32
	blockSize	uint32
	firstKey	string 
	lastKey 	string
}

// NewBlockMeta 创建元信息
func NewBlockMeta(offset uint32, blockSize uint32, firstKey string, lastKey string) BlockMeta {
	return BlockMeta{
		offset: offset,
		blockSize: blockSize,
		firstKey: firstKey,
		lastKey: lastKey,
	}
}

func EncodeMeta(metas []BlockMeta) []byte {
	buffer := bytes.NewBuffer([]byte{})
	for _, meta := range metas {
		binary.Write(buffer, binary.LittleEndian, meta.offset)
		binary.Write(buffer, binary.LittleEndian, meta.blockSize)
		binary.Write(buffer, binary.LittleEndian, uint16(len(meta.firstKey)))
		binary.Write(buffer, binary.LittleEndian, []byte(meta.firstKey))
		binary.Write(buffer, binary.LittleEndian, uint16(len(meta.lastKey)))
		binary.Write(buffer, binary.LittleEndian, []byte(meta.lastKey))
	}
	return buffer.Bytes()
}

func DecodeMeta(data []byte) []BlockMeta {
	buffer := bytes.NewBuffer(data)
	metas := make([]BlockMeta, 0)
	for buffer.Len() > 0 {
		var offset uint32
		var blockSize uint32
		var firstKeyLen uint16
		var lastKeyLen uint16
		binary.Read(buffer, binary.LittleEndian, &offset)
		binary.Read(buffer, binary.LittleEndian, &blockSize)
		binary.Read(buffer, binary.LittleEndian, &firstKeyLen)
		firstKey := make([]byte, firstKeyLen)
		binary.Read(buffer, binary.LittleEndian, firstKey)
		binary.Read(buffer, binary.LittleEndian, &lastKeyLen)
		lastKey := make([]byte, lastKeyLen)
		binary.Read(buffer, binary.LittleEndian, lastKey)
		metas = append(metas, NewBlockMeta(offset, blockSize, string(firstKey), string(lastKey)))
	}
	return metas
}
