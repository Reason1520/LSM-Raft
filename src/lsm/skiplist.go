package lsm

import (
	"math/rand"
	"sync"
)

const (
	MAX_LEVEL   = 16
	PROBABILITY = 0.25
)

/*
* skiplist
 */

type SkipListNode struct {
	key      string          // 键
	value    string          // 值
	trancID  uint64          // 事务/版本号
	forward  []*SkipListNode // 各层向前指针数组
	backward []*SkipListNode // 各层向后指针数组
}

type SkipList struct {
	head         *SkipListNode
	maxLevel     int
	currentLevel int
	sizeByte     int
}

func NewSkipList(maxLevel int) *SkipList {
	sl := new(SkipList)
	sl.maxLevel = MAX_LEVEL
	sl.currentLevel = 1
	sl.sizeByte = 0
	sl.head = &SkipListNode{
		forward:  make([]*SkipListNode, sl.maxLevel),
		backward: make([]*SkipListNode, sl.maxLevel),
	}
	return sl
}

func (sl *SkipList) randomLevel() int {
	level := 1
	// 每一层级提升的概率 P = 0.25
	// 只有 1/4 的节点会进入下一层
	for rand.Float64() < PROBABILITY && level < sl.maxLevel {
		level++
	}
	return level
}

// 插入或更新键值对
// 顺序规则：(key ASC, trancID DESC)
func (sl *SkipList) Put(key string, value string, trancID uint64) {
	if sl.head == nil {
		sl.maxLevel = MAX_LEVEL
		sl.currentLevel = 1
		sl.head = &SkipListNode{
			forward:  make([]*SkipListNode, sl.maxLevel),
			backward: make([]*SkipListNode, sl.maxLevel),
		}
	}

	// 查找插入位置
	update := make([]*SkipListNode, sl.maxLevel)
	cur := sl.head

	for lvl := sl.currentLevel - 1; lvl >= 0; lvl-- {
		next := cur.forward[lvl]
		for next != nil && (next.key < key || (next.key == key && next.trancID > trancID)) {
			cur = next
			next = cur.forward[lvl]
		}
		update[lvl] = cur
	}

	// 相同 key+trancID：更新 value
	target := update[0].forward[0]
	if target != nil && target.key == key && target.trancID == trancID {
		sl.sizeByte += len(value) - len(target.value)
		target.value = value
		return
	}

	// 插入新节点并调整层级
	level := sl.randomLevel()
	if level > sl.currentLevel {
		for i := sl.currentLevel; i < level; i++ {
			update[i] = sl.head
		}
		sl.currentLevel = level
	}

	node := &SkipListNode{
		key:      key,
		value:    value,
		trancID:  trancID,
		forward:  make([]*SkipListNode, level),
		backward: make([]*SkipListNode, level),
	}

	for i := 0; i < level; i++ {
		next := update[i].forward[i]
		node.forward[i] = next
		node.backward[i] = update[i]
		update[i].forward[i] = node
		if next != nil {
			next.backward[i] = node
		}
	}

	sl.sizeByte += len(key) + len(value)
}

// 删除键值对（物理删除，仅供内部使用）
func (sl *SkipList) Remove(key string) {
	if sl.head == nil {
		return
	}

	update := make([]*SkipListNode, sl.maxLevel)
	cur := sl.head
	for lvl := sl.currentLevel - 1; lvl >= 0; lvl-- {
		for cur.forward[lvl] != nil && cur.forward[lvl].key < key {
			cur = cur.forward[lvl]
		}
		update[lvl] = cur
	}

	target := cur.forward[0]
	if target != nil && target.key == key {
		for i := 0; i < len(target.forward); i++ {
			update[i].forward[i] = target.forward[i]
			if target.forward[i] != nil {
				target.forward[i].backward[i] = update[i]
			}
		}

		for sl.currentLevel > 1 && sl.head.forward[sl.currentLevel-1] == nil {
			sl.currentLevel--
		}

		sl.sizeByte -= (len(target.key) + len(target.value))
	}
}

// 查找键值对
// trancID == 0 表示不做可见性限制，直接返回最新版本
func (sl *SkipList) Get(key string, trancID uint64) *SkipListIterator {
	if sl.head == nil {
		return sl.NewIterator(nil, trancID)
	}

	if trancID == 0 {
		cur := sl.head
		for lvl := sl.currentLevel - 1; lvl >= 0; lvl-- {
			for cur.forward[lvl] != nil && cur.forward[lvl].key < key {
				cur = cur.forward[lvl]
			}
		}
		target := cur.forward[0]
		if target != nil && target.key == key {
			return sl.NewIterator(target, trancID)
		}
		return sl.NewIterator(nil, trancID)
	}

	// 定位到第一个 key==target 且 trancID<=查询值 的版本
	cur := sl.head
	for lvl := sl.currentLevel - 1; lvl >= 0; lvl-- {
		for cur.forward[lvl] != nil {
			next := cur.forward[lvl]
			if next.key < key || (next.key == key && next.trancID > trancID) {
				cur = next
				continue
			}
			break
		}
	}

	target := cur.forward[0]
	if target != nil && target.key == key && target.trancID <= trancID {
		return sl.NewIterator(target, trancID)
	}
	return sl.NewIterator(nil, trancID)
}

// 范围查询
// PredicateFunc 定义谓词函数
// 返回 >0: 向右找  <0: 向左找  0: 命中区间
type PredicateFunc func(string) int

// ItersMonotonyPredicate 实现单调谓词查询
func (sl *SkipList) ItersMonotonyPredicate(predicate PredicateFunc, trancID uint64) (*SkipListIterator, *SkipListIterator, bool) {
	if sl.head == nil {
		return nil, nil, false
	}

	// 查找起始位置（第一个满足 predicate == 0 的节点）
	cur := sl.head
	for lvl := sl.currentLevel - 1; lvl >= 0; lvl-- {
		for cur.forward[lvl] != nil && predicate(cur.forward[lvl].key) > 0 {
			cur = cur.forward[lvl]
		}
	}

	startNode := cur.forward[0]
	if startNode == nil || predicate(startNode.key) != 0 {
		return nil, nil, false
	}

	// 查找结束位置（第一个满足 predicate < 0 的节点）
	endNode := startNode
	for endNode != nil && predicate(endNode.key) == 0 {
		endNode = endNode.forward[0]
	}

	return sl.NewIterator(startNode, trancID), sl.NewIterator(endNode, trancID), true
}

// BeginPrefix 返回第一个前缀匹配或大于前缀的迭代器（等同于 Seek）
func (sl *SkipList) BeginPrefix(prefix string, trancID uint64) *SkipListIterator {
	it := sl.NewIterator(nil, trancID)
	it.Seek(prefix)
	return it
}

// EndPrefix 返回第一个“完全超出”前缀范围的节点迭代器
func (sl *SkipList) EndPrefix(prefix string, trancID uint64) *SkipListIterator {
	if prefix == "" {
		return sl.NewIterator(nil, trancID)
	}

	limit := []byte(prefix)
	for i := len(limit) - 1; i >= 0; i-- {
		limit[i]++
		if limit[i] != 0 {
			break
		}
	}

	it := sl.NewIterator(nil, trancID)
	it.Seek(string(limit))
	return it
}

/*
* SkipListIterator 跳表迭代器
 */

type SkipListIterator struct {
	list    *SkipList
	current *SkipListNode
	trancID uint64
	closed  bool
	mu      sync.RWMutex
}

// NewIterator 创建一个初始指向节点的迭代器
func (sl *SkipList) NewIterator(node *SkipListNode, trancID uint64) *SkipListIterator {
	return &SkipListIterator{
		list:    sl,
		current: node,
		trancID: trancID,
		closed:  false,
	}
}

// Valid 检查迭代器当前位置是否有效
func (it *SkipListIterator) Valid() bool {
	it.mu.RLock()
	defer it.mu.RUnlock()

	if it.closed || it.current == nil {
		return false
	}
	return true
}

// Key 返回当前的 key
func (it *SkipListIterator) Key() string {
	it.mu.RLock()
	defer it.mu.RUnlock()

	if !it.Valid() {
		return ""
	}
	return it.current.key
}

// Value 返回当前的 value
func (it *SkipListIterator) Value() string {
	it.mu.RLock()
	defer it.mu.RUnlock()

	if !it.Valid() {
		return ""
	}
	return it.current.value
}

// TrancID 获取当前事务ID
func (it *SkipListIterator) TrancID() uint64 {
	it.mu.RLock()
	defer it.mu.RUnlock()

	if !it.Valid() {
		return 0
	}
	return it.current.trancID
}

// Next 移动到下一个节点
func (it *SkipListIterator) Next() {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed || it.current == nil {
		return
	}

	it.current = it.current.forward[0]
}

// Close 关闭迭代器
func (it *SkipListIterator) Close() error {
	it.mu.Lock()
	defer it.mu.Unlock()

	it.closed = true
	it.current = nil
	it.list = nil
	return nil
}

// SeekFirst 将迭代器定位到第一个元素
func (it *SkipListIterator) SeekFirst() bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.list == nil || it.list.head == nil || it.list.head.forward[0] == nil {
		it.current = nil
		return false
	}
	it.current = it.list.head.forward[0]
	return true
}

// Seek 移动到第一个大于等于 key 的节点
func (it *SkipListIterator) Seek(key string) bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.list == nil || it.list.head == nil {
		it.current = nil
		return false
	}

	cur := it.list.head
	for lvl := it.list.currentLevel - 1; lvl >= 0; lvl-- {
		for cur.forward[lvl] != nil && cur.forward[lvl].key < key {
			cur = cur.forward[lvl]
		}
	}
	it.current = cur.forward[0]
	return it.current != nil
}
