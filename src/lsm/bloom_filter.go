package lsm

import (
	"math"
	"hash/fnv"
	"encoding/binary"
)

type BloomFilter struct {
	expectedElements   uint64	// 预期元素数量
	falsePositiveRate  float64	// 允许的假阳性率

	numBits   uint64
	numHashes uint64			// 哈希函数的数量

	bits []byte					// 布隆过滤器的位数组
}

// NewBloomFilter: 创建一个布隆过滤器
func NewBloomFilter(n uint64, p float64) *BloomFilter {
	m := -float64(n) * math.Log(p) / (math.Ln2 * math.Ln2)
	k := (m / float64(n)) * math.Ln2

	numBits := uint64(math.Ceil(m))
	numHashes := uint64(math.Ceil(k))

	// 向上取整，避免为0
	if numBits == 0 {
		numBits = 1
	}
	if numHashes == 0 {
		numHashes = 1
	}

	return &BloomFilter{
		expectedElements:  n,
		falsePositiveRate: p,
		numBits:           numBits,
		numHashes:         numHashes,
		bits:              make([]byte, (numBits+7)/8),
	}
}

func (bf *BloomFilter) Hash1(key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64()
}

func (bf *BloomFilter) Hash2(key string) uint64 {
	h := fnv.New64()
	h.Write([]byte(key))
	return h.Sum64()
}

// Hash: 计算给定键的哈希值
func (bf *BloomFilter) Hash(key string, i uint64) uint64 {
	return (bf.Hash1(key) + i*bf.Hash2(key)) % bf.numBits
}

// SetBit: 设置给定位置的位
func (bf *BloomFilter) SetBit(pos uint64) {
	byteIdx := pos / 8
	bitIdx := pos % 8
	bf.bits[byteIdx] |= (1 << bitIdx)
}

// GetBit: 获取给定位置的位
func (bf *BloomFilter) GetBit(pos uint64) bool {
	byteIdx := pos / 8
	bitIdx := pos % 8
	return (bf.bits[byteIdx] & (1 << bitIdx)) != 0
}

// Add: 向布隆过滤器中添加一个键
func (bf *BloomFilter) Add(key string) {
	for i := uint64(0); i < bf.numHashes; i++ {
		pos := bf.Hash(key, i)
		bf.SetBit(pos)
	}
}

// Contains: 检查给定键是否存在于布隆过滤器中
func (bf *BloomFilter) Contains(key string) bool {
	for i := uint64(0); i < bf.numHashes; i++ {
		pos := bf.Hash(key, i)
		if !bf.GetBit(pos) {
			return false // 一定不存在
		}
	}
	return true // 可能存在
}

// Encode: 将布隆过滤器编码为字节数组
func (bf *BloomFilter) Encode() []byte {
	size := 16 + len(bf.bits)
	data := make([]byte, size)

	binary.LittleEndian.PutUint64(data[0:8], bf.numBits)
	binary.LittleEndian.PutUint64(data[8:16], bf.numHashes)

	copy(data[16:], bf.bits)

	return data
}

// DecodeBloomFilter: 从字节数组解码布隆过滤器
func DecodeBloomFilter(data []byte) *BloomFilter {
	if len(data) < 16 {
		return nil
	}

	numBits := binary.LittleEndian.Uint64(data[0:8])
	numHashes := binary.LittleEndian.Uint64(data[8:16])

	if numBits == 0 || numHashes == 0 {
		return nil
	}

	bits := make([]byte, len(data)-16)
	copy(bits, data[16:])

	return &BloomFilter{
		numBits:   numBits,
		numHashes: numHashes,
		bits:      bits,
	}
}