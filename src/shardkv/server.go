package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/lsm"
	"6.5840/raft"
	"6.5840/shardctrler"
)

const (
	UpConfigLoopInterval = 100 * time.Millisecond
	GetShardsInterval    = 100 * time.Millisecond
	GCInterval           = 100 * time.Millisecond
	TxnGCInterval        = 500 * time.Millisecond
	TxnTTL               = 30 * time.Second
	TxnRecoveryInterval  = 200 * time.Millisecond
)

const (
	Serving   = iota // normal service
	Pulling          // pulling shard data from other group
	BePulling        // being pulled by other group (data still local)
	GCing            // waiting for GC
)

type Op struct {
	Type       string // "Get", "Put", "Append", "Reconfig", "InsertShard", "DeleteShard"
	Key        string
	Value      string
	RangeStart string
	RangeEnd   string
	RangeLimit int
	ClientID   int64
	RPCID      int64
	TxnID      uint64
	Writes     []TxnWrite
	Reads      []TxnRead
	Isolation  IsolationLevel
	Config     shardctrler.Config
	ShardData  map[string]string
	LastOpMap  map[int64]OpResult
	GID        int
	ShardID    int
	ConfigNum  int
	CoordGID   int
	Snapshot   uint64
}

type OpResult struct {
	Err       Err
	Value     string
	KVs       []KeyValue
	RPCID     int64
	TxnID     uint64
	Snapshot  uint64
	ConfigNum int
	GID       int
	ShardID   int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	dead         int32

	mck        *shardctrler.Clerk
	config     shardctrler.Config
	lastConfig shardctrler.Config
	persister  *raft.Persister

	kvDB map[int]*lsm.LSMEngine

	shadowDB map[int]map[int]map[string]string

	shardStatus map[int]int

	lastOps map[int64]OpResult

	waitCh map[int]chan OpResult

	nextTxnID         uint64
	nextInternalRPCID int64
	lastApplied       int

	activeTxn     map[uint64]uint64
	activeTxnLast map[uint64]time.Time
	gcWatermark   uint64
	coordTxns     map[uint64]CoordTxnRecord
	branchTxns    map[uint64]BranchTxnRecord
	preparedKeys  map[string]uint64

	snapshotMu     sync.Mutex
	pendingSnap    *snapshotTask
	snapshotNotify chan struct{}
	snapshotStop   chan struct{}
	snapshotWG     sync.WaitGroup
	snapshotRefsWG sync.WaitGroup
}

type snapshotTask struct {
	index        int
	engines      map[int]*lsm.LSMEngine
	shadowDB     map[int]map[int]map[string]string
	shardState   map[int]int
	lastOps      map[int64]OpResult
	config       shardctrler.Config
	lastConfig   shardctrler.Config
	coordTxns    map[uint64]CoordTxnRecord
	branchTxns   map[uint64]BranchTxnRecord
	preparedKeys map[string]uint64
}

// Check strictly if I can serve this key
func (kv *ShardKV) canServe(shard int) bool {
	return kv.config.Shards[shard] == kv.gid && (kv.shardStatus[shard] == Serving || kv.shardStatus[shard] == GCing)
}

func (kv *ShardKV) waitAppliedLocked(index int, deadline time.Time) bool {
	for kv.lastApplied < index {
		kv.mu.Unlock()
		if time.Now().After(deadline) {
			kv.mu.Lock()
			return false
		}
		time.Sleep(10 * time.Millisecond)
		kv.mu.Lock()
	}
	return true
}

func (kv *ShardKV) getSafeReadIndex() (int, bool) {
	return kv.rf.LeaseReadIndex()
}

func (kv *ShardKV) fastGet(key string, rpcID int64) (OpResult, bool) {
	readIndex, ok := kv.getSafeReadIndex()
	if !ok {
		return OpResult{}, false
	}

	deadline := time.Now().Add(300 * time.Millisecond)
	shard := key2shard(key)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !kv.waitAppliedLocked(readIndex, deadline) {
		return OpResult{Err: ErrTimeout, RPCID: rpcID}, true
	}
	if !kv.canServe(shard) {
		return OpResult{Err: ErrWrongGroup, RPCID: rpcID}, true
	}

	engine := kv.kvDB[shard]
	if engine == nil {
		return OpResult{Err: ErrNoKey, RPCID: rpcID}, true
	}
	enc, _ := engine.Get(key, uint64(readIndex))
	val, ok := decodeValue(enc)
	if !ok {
		return OpResult{Err: ErrNoKey, RPCID: rpcID}, true
	}
	return OpResult{Err: OK, Value: val, RPCID: rpcID}, true
}

func (kv *ShardKV) fastRange(start, end string, limit, shard int, rpcID int64) (OpResult, bool) {
	readIndex, ok := kv.getSafeReadIndex()
	if !ok {
		return OpResult{}, false
	}

	deadline := time.Now().Add(300 * time.Millisecond)
	if limit < 0 {
		limit = 0
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !kv.waitAppliedLocked(readIndex, deadline) {
		return OpResult{Err: ErrTimeout, RPCID: rpcID}, true
	}
	if !kv.canServe(shard) {
		return OpResult{Err: ErrWrongGroup, RPCID: rpcID}, true
	}

	engine := kv.kvDB[shard]
	if engine == nil {
		return OpResult{Err: OK, RPCID: rpcID, KVs: nil}, true
	}

	pred := func(k string) int {
		if k < start {
			return 1
		}
		if end != "" && k >= end {
			return -1
		}
		return 0
	}

	startIt := engine.ScanFrom(uint64(readIndex), start)
	if startIt == nil {
		return OpResult{Err: OK, RPCID: rpcID, KVs: nil}, true
	}
	defer startIt.Close()

	out := make([]KeyValue, 0)
	for startIt.Valid() {
		k := startIt.Key()
		if pred(k) != 0 {
			break
		}
		if v, ok := decodeValue(startIt.Value()); ok {
			out = append(out, KeyValue{Key: k, Value: v})
			if limit > 0 && len(out) >= limit {
				break
			}
		}
		startIt.Next()
	}

	return OpResult{Err: OK, RPCID: rpcID, KVs: out}, true
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	shard := key2shard(args.Key)
	kv.mu.Lock()
	if !kv.canServe(shard) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	if res, ok := kv.fastGet(args.Key, args.RPCID); ok {
		reply.Err = res.Err
		reply.Value = res.Value
		return
	}

	op := Op{
		Type:     GET,
		Key:      args.Key,
		ClientID: args.ClientID,
		RPCID:    args.RPCID,
	}

	res := kv.startOp(op)
	reply.Err = res.Err
	reply.Value = res.Value
}

func (kv *ShardKV) Range(args *RangeArgs, reply *RangeReply) {
	shard := args.ShardID
	kv.mu.Lock()
	if !kv.canServe(shard) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	if res, ok := kv.fastRange(args.Start, args.End, args.Limit, args.ShardID, args.RPCID); ok {
		reply.Err = res.Err
		reply.KVs = res.KVs
		return
	}

	op := Op{
		Type:       RANGE,
		RangeStart: args.Start,
		RangeEnd:   args.End,
		RangeLimit: args.Limit,
		ShardID:    args.ShardID,
		ClientID:   args.ClientID,
		RPCID:      args.RPCID,
	}

	res := kv.startOp(op)
	reply.Err = res.Err
	reply.KVs = res.KVs
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	shard := key2shard(args.Key)
	kv.mu.Lock()
	if !kv.canServe(shard) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		Type:     args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientID: args.ClientID,
		RPCID:    args.RPCID,
	}

	res := kv.startOp(op)
	reply.Err = res.Err
}

func (kv *ShardKV) TxnBegin(args *TxnBeginArgs, reply *TxnBeginReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Type:     TXNBEGIN,
		ClientID: args.ClientID,
		RPCID:    args.RPCID,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := make(chan OpResult, 1)
	kv.waitCh[index] = ch
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.waitCh, index)
		kv.mu.Unlock()
	}()

	select {
	case res := <-ch:
		if res.Err != OK {
			reply.Err = res.Err
			return
		}
		txnID := atomic.AddUint64(&kv.nextTxnID, 1)
		reply.Err = OK
		reply.TxnID = txnID
		reply.Snapshot = uint64(index)
		kv.mu.Lock()
		kv.registerTxnLocked(txnID, reply.Snapshot)
		kv.mu.Unlock()
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}
}

func (kv *ShardKV) TxnCoordBegin(args *TxnCoordBeginArgs, reply *TxnCoordBeginReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	if args.AnchorKey == "" {
		reply.Err = ErrWrongGroup
		return
	}

	txnID := atomic.AddUint64(&kv.nextTxnID, 1)
	op := Op{
		Type:      TXNCOORDBEGIN,
		TxnID:     txnID,
		ClientID:  args.ClientID,
		RPCID:     args.RPCID,
		Isolation: args.Isolation,
		ConfigNum: args.ConfigNum,
		ShardID:   key2shard(args.AnchorKey),
		CoordGID:  kv.gid,
	}

	res := kv.startOp(op)
	reply.Err = res.Err
	reply.TxnID = res.TxnID
	reply.CoordGID = res.GID
	reply.ConfigNum = res.ConfigNum
}

func (kv *ShardKV) TxnCoordEnlist(args *TxnCoordEnlistArgs, reply *TxnCoordEnlistReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Type:      TXNCOORDENLIST,
		TxnID:     args.TxnID,
		ClientID:  args.ClientID,
		RPCID:     args.RPCID,
		ConfigNum: args.ConfigNum,
		GID:       args.GID,
		ShardID:   args.ShardID,
		Snapshot:  args.Snapshot,
		CoordGID:  kv.gid,
	}

	res := kv.startOp(op)
	reply.Err = res.Err
}

func (kv *ShardKV) TxnBranchBegin(args *TxnBranchBeginArgs, reply *TxnBranchBeginReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Type:      TXNBRANCHBEGIN,
		TxnID:     args.TxnID,
		ClientID:  args.ClientID,
		RPCID:     args.RPCID,
		Isolation: args.Isolation,
		ConfigNum: args.ConfigNum,
		CoordGID:  args.CoordGID,
		ShardID:   args.ShardID,
	}

	res := kv.startOp(op)
	reply.Err = res.Err
	reply.Snapshot = res.Snapshot
	reply.GID = res.GID
	reply.ShardID = res.ShardID
}

func (kv *ShardKV) TxnCoordPrepare(args *TxnCoordPrepareArgs, reply *TxnCoordPrepareReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Type:      TXNCOORDPREPARE,
		TxnID:     args.TxnID,
		ClientID:  args.ClientID,
		RPCID:     args.RPCID,
		ConfigNum: args.ConfigNum,
		GID:       args.GID,
		ShardID:   args.ShardID,
		Snapshot:  args.Snapshot,
		Reads:     args.Reads,
		Writes:    args.Writes,
	}

	res := kv.startOp(op)
	reply.Err = res.Err
}

func (kv *ShardKV) TxnCoordAbort(args *TxnCoordAbortArgs, reply *TxnCoordAbortReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Type:      TXNCOORDABORT,
		TxnID:     args.TxnID,
		ClientID:  args.ClientID,
		RPCID:     args.RPCID,
		ConfigNum: args.ConfigNum,
	}

	res := kv.startOp(op)
	reply.Err = res.Err
}

func (kv *ShardKV) TxnCoordCommit(args *TxnCoordCommitArgs, reply *TxnCoordCommitReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Type:      TXNCOORDCOMMIT,
		TxnID:     args.TxnID,
		ClientID:  args.ClientID,
		RPCID:     args.RPCID,
		ConfigNum: args.ConfigNum,
	}

	res := kv.startOp(op)
	reply.Err = res.Err
}

func (kv *ShardKV) TxnCoordFinish(args *TxnCoordFinishArgs, reply *TxnCoordFinishReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Type:      TXNCOORDFINISH,
		TxnID:     args.TxnID,
		ClientID:  args.ClientID,
		RPCID:     args.RPCID,
		ConfigNum: args.ConfigNum,
	}

	res := kv.startOp(op)
	reply.Err = res.Err
}

func (kv *ShardKV) TxnCoordStatus(args *TxnCoordStatusArgs, reply *TxnCoordStatusReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	rec, ok := kv.coordTxns[args.TxnID]
	if !ok {
		reply.Err = ErrNoKey
		return
	}
	if args.ConfigNum != 0 && rec.ConfigNum != args.ConfigNum {
		reply.Err = ErrConfigNotReady
		return
	}
	reply.Err = OK
	reply.State = rec.State
}

func (kv *ShardKV) TxnBranchPrepare(args *TxnBranchPrepareArgs, reply *TxnBranchPrepareReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Type:      TXNBRANCHPREPARE,
		TxnID:     args.TxnID,
		ClientID:  args.ClientID,
		RPCID:     args.RPCID,
		ConfigNum: args.ConfigNum,
		CoordGID:  args.CoordGID,
		ShardID:   args.ShardID,
		Snapshot:  args.Snapshot,
		Isolation: args.Isolation,
		Reads:     args.Reads,
		Writes:    args.Writes,
	}

	res := kv.startOp(op)
	reply.Err = res.Err
}

func (kv *ShardKV) TxnBranchAbort(args *TxnBranchAbortArgs, reply *TxnBranchAbortReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Type:      TXNBRANCHABORT,
		TxnID:     args.TxnID,
		ClientID:  args.ClientID,
		RPCID:     args.RPCID,
		ConfigNum: args.ConfigNum,
		ShardID:   args.ShardID,
	}

	res := kv.startOp(op)
	reply.Err = res.Err
}

func (kv *ShardKV) TxnBranchCommit(args *TxnBranchCommitArgs, reply *TxnBranchCommitReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Type:      TXNBRANCHCOMMIT,
		TxnID:     args.TxnID,
		ClientID:  args.ClientID,
		RPCID:     args.RPCID,
		ConfigNum: args.ConfigNum,
		ShardID:   args.ShardID,
	}

	res := kv.startOp(op)
	reply.Err = res.Err
}

func (kv *ShardKV) TxnGet(args *TxnGetArgs, reply *TxnGetReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	shard := key2shard(args.Key)
	kv.mu.Lock()
	if !kv.canServe(shard) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.touchTxnLocked(args.TxnID)
	engine := kv.kvDB[shard]
	kv.mu.Unlock()

	if engine == nil {
		reply.Err = ErrNoKey
		reply.Version = 0
		return
	}

	enc, tid := engine.Get(args.Key, args.Snapshot)
	val, ok := decodeValue(enc)
	reply.Version = tid
	if !ok {
		reply.Err = ErrNoKey
		return
	}
	reply.Err = OK
	reply.Value = val
}

func (kv *ShardKV) TxnCommit(args *TxnCommitArgs, reply *TxnCommitReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	shard := -1
	for _, w := range args.Writes {
		s := key2shard(w.Key)
		if shard == -1 {
			shard = s
		} else if shard != s {
			reply.Err = ErrWrongGroup
			return
		}
	}
	for _, r := range args.Reads {
		s := key2shard(r.Key)
		if shard == -1 {
			shard = s
		} else if shard != s {
			reply.Err = ErrWrongGroup
			return
		}
	}

	if shard != -1 {
		kv.mu.Lock()
		if !kv.canServe(shard) {
			reply.Err = ErrWrongGroup
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
	}

	op := Op{
		Type:      TXNCOMMIT,
		ClientID:  args.ClientID,
		RPCID:     args.RPCID,
		TxnID:     args.TxnID,
		Writes:    args.Writes,
		Reads:     args.Reads,
		Isolation: args.Isolation,
		ShardID:   shard,
	}

	res := kv.startOp(op)
	reply.Err = res.Err
}

func (kv *ShardKV) TxnAbort(args *TxnAbortArgs, reply *TxnAbortReply) {
	kv.mu.Lock()
	if rec, ok := kv.branchTxns[args.TxnID]; ok {
		kv.releasePreparedKeysLocked(rec.Writes, args.TxnID)
	}
	kv.unregisterTxnLocked(args.TxnID)
	delete(kv.coordTxns, args.TxnID)
	delete(kv.branchTxns, args.TxnID)
	kv.mu.Unlock()
	reply.Err = OK
}

func (kv *ShardKV) TxnRange(args *TxnRangeArgs, reply *TxnRangeReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	shard := args.ShardID
	kv.mu.Lock()
	if !kv.canServe(shard) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.touchTxnLocked(args.TxnID)
	engine := kv.kvDB[shard]
	kv.mu.Unlock()

	if engine == nil {
		reply.Err = OK
		reply.KVs = nil
		return
	}

	start := args.Start
	end := args.End
	limit := args.Limit
	if limit < 0 {
		limit = 0
	}

	pred := func(k string) int {
		if k < start {
			return 1
		}
		if end != "" && k >= end {
			return -1
		}
		return 0
	}

	startIt := engine.ScanFrom(args.Snapshot, start)
	if startIt == nil {
		reply.Err = OK
		reply.KVs = nil
		return
	}
	defer startIt.Close()

	out := make([]TxnRangeKV, 0)
	for startIt.Valid() {
		k := startIt.Key()
		if pred(k) != 0 {
			break
		}
		if v, ok := decodeValue(startIt.Value()); ok {
			out = append(out, TxnRangeKV{
				Key:     k,
				Value:   v,
				Version: startIt.TrancID(),
			})
			if limit > 0 && len(out) >= limit {
				break
			}
		}
		startIt.Next()
	}

	reply.Err = OK
	reply.KVs = out
}

// startOp submits an Op to Raft and waits for its result.
func (kv *ShardKV) startOp(op Op) OpResult {
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return OpResult{Err: ErrWrongLeader}
	}

	kv.mu.Lock()
	ch := make(chan OpResult, 1)
	kv.waitCh[index] = ch
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.waitCh, index)
		kv.mu.Unlock()
	}()

	select {
	case res := <-ch:
		return res
	case <-time.After(500 * time.Millisecond):
		return OpResult{Err: ErrTimeout}
	}
}

// PullData handles shard data pull requests from other groups.
func (kv *ShardKV) PullData(args *PullDataArgs, reply *PullDataReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.ConfigNum >= kv.config.Num {
		reply.Err = ErrNotReady
		return
	}
	if kv.hasPreparedTxnOnShardLocked(args.ShardIndex) {
		reply.Err = ErrNotReady
		return
	}

	if shards, ok := kv.shadowDB[args.ConfigNum]; ok {
		if shardData, ok := shards[args.ShardIndex]; ok {
			// Deep Copy Data
			reply.ShardData = make(map[string]string)
			for k, v := range shardData {
				reply.ShardData[k] = v
			}
			// Deep Copy LastOps (Client deduplication map)
			reply.LastOpMap = make(map[int64]OpResult)
			for k, v := range kv.lastOps {
				reply.LastOpMap[k] = v
			}
			reply.Err = OK
			return
		}
	}

	reply.Err = ErrNoKey
}

// DeleteShard handles GC confirmation from the new owner.
func (kv *ShardKV) DeleteShard(args *PullDataArgs, reply *PullDataReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	if args.ConfigNum >= kv.config.Num {
		reply.Err = ErrNotReady
		kv.mu.Unlock()
		return
	}
	if kv.hasPreparedTxnOnShardLocked(args.ShardIndex) {
		reply.Err = ErrNotReady
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		Type:      DELETESHARD,
		ConfigNum: args.ConfigNum,
		ShardID:   args.ShardIndex,
	}

	res := kv.startOp(op)
	reply.Err = res.Err
}

// applier handles committed Raft log entries and snapshots.
func (kv *ShardKV) applier() {
	for msg := range kv.applyCh {
		if kv.killed() {
			return
		}

		if msg.CommandValid {
			kv.mu.Lock()
			op := msg.Command.(Op)
			index := msg.CommandIndex

			var res OpResult
			res.RPCID = op.RPCID

			switch op.Type {
			case PUT, APPEND:
				res = kv.applyPutAppend(op, index)
			case GET:
				res = kv.applyGet(op)
			case RANGE:
				res = kv.applyRange(op, index)
			case TXNBEGIN:
				res = OpResult{Err: OK, RPCID: op.RPCID}
			case TXNCOORDBEGIN:
				res = kv.applyTxnCoordBegin(op)
			case TXNCOORDENLIST:
				res = kv.applyTxnCoordEnlist(op)
			case TXNBRANCHBEGIN:
				res = kv.applyTxnBranchBegin(op, index)
			case TXNCOORDPREPARE:
				res = kv.applyTxnCoordPrepare(op)
			case TXNCOORDABORT:
				res = kv.applyTxnCoordAbort(op)
			case TXNCOORDCOMMIT:
				res = kv.applyTxnCoordCommit(op)
			case TXNCOORDFINISH:
				res = kv.applyTxnCoordFinish(op)
			case TXNBRANCHPREPARE:
				res = kv.applyTxnBranchPrepare(op)
			case TXNBRANCHABORT:
				res = kv.applyTxnBranchAbort(op)
			case TXNBRANCHCOMMIT:
				res = kv.applyTxnBranchCommit(op, index)
			case RECONFIG:
				res = kv.applyReconfig(op)
			case INSERTSHARD:
				res = kv.applyInsertShard(op)
			case DELETESHARD:
				res = kv.applyDeleteShard(op)
			case TXNCOMMIT:
				res = kv.applyTxnCommit(op, index)
			}

			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
				kv.enqueueSnapshotLocked(index)
			}

			kv.lastApplied = index
			kv.updateGCWatermarkLocked()

			if ch, ok := kv.waitCh[index]; ok {
				ch <- res
			}
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			kv.mu.Lock()
			kv.readSnapshot(msg.Snapshot)
			kv.lastApplied = msg.SnapshotIndex
			kv.updateGCWatermarkLocked()
			kv.mu.Unlock()
		}
	}
}

// --- Applier helper functions (must hold lock) ---

func (kv *ShardKV) applyPutAppend(op Op, index int) OpResult {
	shard := key2shard(op.Key)
	if !kv.canServe(shard) {
		return OpResult{Err: ErrWrongGroup}
	}

	if lastRes, ok := kv.lastOps[op.ClientID]; ok && op.RPCID <= lastRes.RPCID {
		return lastRes
	}

	engine := kv.ensureShardEngine(shard)
	trancID := uint64(index)
	if op.Type == PUT {
		engine.Put(op.Key, encodeValue(op.Value), trancID)
	} else {
		curEnc, _ := engine.Get(op.Key, 0)
		curVal, _ := decodeValue(curEnc)
		engine.Put(op.Key, encodeValue(curVal+op.Value), trancID)
	}

	res := OpResult{Err: OK, RPCID: op.RPCID}
	kv.lastOps[op.ClientID] = res
	return res
}

func (kv *ShardKV) applyGet(op Op) OpResult {
	shard := key2shard(op.Key)
	if !kv.canServe(shard) {
		return OpResult{Err: ErrWrongGroup}
	}

	engine := kv.kvDB[shard]
	if engine == nil {
		return OpResult{Err: ErrNoKey, RPCID: op.RPCID}
	}
	enc, _ := engine.Get(op.Key, 0)
	val, ok := decodeValue(enc)
	if !ok {
		return OpResult{Err: ErrNoKey, RPCID: op.RPCID}
	}
	return OpResult{Err: OK, Value: val, RPCID: op.RPCID}
}

func (kv *ShardKV) applyRange(op Op, index int) OpResult {
	start := op.RangeStart
	end := op.RangeEnd
	limit := op.RangeLimit
	if limit < 0 {
		limit = 0
	}

	shard := op.ShardID
	if !kv.canServe(shard) {
		return OpResult{Err: ErrWrongGroup, RPCID: op.RPCID}
	}

	if lastRes, ok := kv.lastOps[op.ClientID]; ok && op.RPCID <= lastRes.RPCID {
		return lastRes
	}

	engine := kv.kvDB[shard]
	if engine == nil {
		res := OpResult{Err: OK, RPCID: op.RPCID, KVs: nil}
		kv.lastOps[op.ClientID] = res
		return res
	}

	pred := func(k string) int {
		if k < start {
			return 1
		}
		if end != "" && k >= end {
			return -1
		}
		return 0
	}

	startIt := engine.ScanFrom(uint64(index), start)
	if startIt == nil {
		res := OpResult{Err: OK, RPCID: op.RPCID, KVs: nil}
		kv.lastOps[op.ClientID] = res
		return res
	}
	defer startIt.Close()

	out := make([]KeyValue, 0)
	for startIt.Valid() {
		k := startIt.Key()
		if pred(k) != 0 {
			break
		}
		if v, ok := decodeValue(startIt.Value()); ok {
			out = append(out, KeyValue{Key: k, Value: v})
			if limit > 0 && len(out) >= limit {
				break
			}
		}
		startIt.Next()
	}

	res := OpResult{Err: OK, RPCID: op.RPCID, KVs: out}
	kv.lastOps[op.ClientID] = res
	return res
}

func (kv *ShardKV) applyReconfig(op Op) OpResult {
	if op.Config.Num == kv.config.Num+1 {
		kv.lastConfig = kv.config
		kv.config = op.Config

		for i := 0; i < shardctrler.NShards; i++ {
			if kv.config.Shards[i] == kv.gid {
				if kv.lastConfig.Shards[i] != kv.gid && kv.lastConfig.Num != 0 {
					kv.shardStatus[i] = Pulling
				} else {
					kv.shardStatus[i] = Serving
					kv.ensureShardEngine(i)
				}
			} else {
				if kv.lastConfig.Shards[i] == kv.gid {
					kv.shardStatus[i] = BePulling

					if kv.shadowDB[kv.lastConfig.Num] == nil {
						kv.shadowDB[kv.lastConfig.Num] = make(map[int]map[string]string)
					}
					if engine := kv.kvDB[i]; engine != nil {
						kv.snapshotRefsWG.Wait()
						kv.shadowDB[kv.lastConfig.Num][i] = kv.exportShardDataAt(engine, 0)
						engine.Close()
					} else {
						kv.shadowDB[kv.lastConfig.Num][i] = make(map[string]string)
					}
					kv.kvDB[i] = nil // clear local DB
				} else {
				}
			}
		}
	}
	return OpResult{Err: OK}
}

func (kv *ShardKV) applyInsertShard(op Op) OpResult {
	if op.ConfigNum == kv.config.Num && kv.shardStatus[op.ShardID] == Pulling {
		engine := kv.ensureShardEngine(op.ShardID)
		if len(op.ShardData) > 0 {
			batch := make([]lsm.KV, 0, len(op.ShardData))
			for k, v := range op.ShardData {
				batch = append(batch, lsm.KV{Key: k, Value: encodeValue(v)})
			}
			engine.PutBatch(batch, 0)
		}

		for clientId, otherRes := range op.LastOpMap {
			if localRes, ok := kv.lastOps[clientId]; !ok || otherRes.RPCID > localRes.RPCID {
				kv.lastOps[clientId] = otherRes
			}
		}

		kv.shardStatus[op.ShardID] = GCing
	}
	return OpResult{Err: OK}
}

func (kv *ShardKV) applyDeleteShard(op Op) OpResult {
	if op.ConfigNum < kv.config.Num {
		if shards, ok := kv.shadowDB[op.ConfigNum]; ok {
			delete(shards, op.ShardID)
			if len(shards) == 0 {
				delete(kv.shadowDB, op.ConfigNum)
			}
		}
	}
	return OpResult{Err: OK}
}

func (kv *ShardKV) applyTxnCommit(op Op, index int) OpResult {
	defer kv.unregisterTxnLocked(op.TxnID)
	if lastRes, ok := kv.lastOps[op.ClientID]; ok && op.RPCID <= lastRes.RPCID {
		return lastRes
	}

	if op.ShardID != -1 && !kv.canServe(op.ShardID) {
		return OpResult{Err: ErrWrongGroup, RPCID: op.RPCID}
	}

	shard := op.ShardID
	for _, w := range op.Writes {
		s := key2shard(w.Key)
		if shard == -1 {
			shard = s
		} else if shard != s {
			return OpResult{Err: ErrWrongGroup, RPCID: op.RPCID}
		}
	}
	for _, r := range op.Reads {
		s := key2shard(r.Key)
		if shard == -1 {
			shard = s
		} else if shard != s {
			return OpResult{Err: ErrWrongGroup, RPCID: op.RPCID}
		}
	}

	if shard == -1 {
		res := OpResult{Err: OK, RPCID: op.RPCID}
		kv.lastOps[op.ClientID] = res
		return res
	}

	engine := kv.kvDB[shard]
	if engine == nil {
		return OpResult{Err: ErrNoKey, RPCID: op.RPCID}
	}

	if op.Isolation == RepeatableRead || op.Isolation == Serializable {
		for _, r := range op.Reads {
			_, tid := engine.Get(r.Key, 0)
			if tid != r.Version {
				res := OpResult{Err: ErrConflict, RPCID: op.RPCID}
				kv.lastOps[op.ClientID] = res
				return res
			}
		}
	}

	trancID := uint64(index)
	if len(op.Writes) > 0 {
		putBatch := make([]lsm.KV, 0, len(op.Writes))
		deleteBatch := make([]string, 0, len(op.Writes))
		for _, w := range op.Writes {
			if w.Delete {
				deleteBatch = append(deleteBatch, w.Key)
			} else {
				putBatch = append(putBatch, lsm.KV{Key: w.Key, Value: encodeValue(w.Value)})
			}
		}
		if len(putBatch) > 0 {
			engine.PutBatch(putBatch, trancID)
		}
		if len(deleteBatch) > 0 {
			engine.RemoveBatch(deleteBatch, trancID)
		}
	}

	res := OpResult{Err: OK, RPCID: op.RPCID}
	kv.lastOps[op.ClientID] = res
	return res
}

func (kv *ShardKV) applyTxnCoordBegin(op Op) OpResult {
	if lastRes, ok := kv.lastOps[op.ClientID]; ok && op.RPCID <= lastRes.RPCID {
		return lastRes
	}
	if op.ConfigNum != kv.config.Num {
		return OpResult{Err: ErrConfigNotReady, RPCID: op.RPCID}
	}
	if !kv.canServe(op.ShardID) {
		return OpResult{Err: ErrWrongGroup, RPCID: op.RPCID}
	}
	if _, ok := kv.coordTxns[op.TxnID]; !ok {
		kv.coordTxns[op.TxnID] = CoordTxnRecord{
			TxnID:        op.TxnID,
			ConfigNum:    op.ConfigNum,
			Isolation:    op.Isolation,
			CoordGID:     kv.gid,
			AnchorShard:  op.ShardID,
			State:        CoordTxnBegun,
			Participants: make(map[int]CoordTxnParticipant),
			Branches:     make(map[int]CoordTxnBranchRecord),
		}
	}
	res := OpResult{
		Err:       OK,
		RPCID:     op.RPCID,
		TxnID:     op.TxnID,
		ConfigNum: op.ConfigNum,
		GID:       kv.gid,
	}
	kv.lastOps[op.ClientID] = res
	return res
}

func (kv *ShardKV) applyTxnCoordEnlist(op Op) OpResult {
	if lastRes, ok := kv.lastOps[op.ClientID]; ok && op.RPCID <= lastRes.RPCID {
		return lastRes
	}
	if op.ConfigNum != kv.config.Num {
		return OpResult{Err: ErrConfigNotReady, RPCID: op.RPCID}
	}
	rec, ok := kv.coordTxns[op.TxnID]
	if !ok {
		return OpResult{Err: ErrNotReady, RPCID: op.RPCID}
	}
	if rec.Participants == nil {
		rec.Participants = make(map[int]CoordTxnParticipant)
	}
	rec.Participants[op.GID] = CoordTxnParticipant{
		GID:      op.GID,
		ShardID:  op.ShardID,
		Snapshot: op.Snapshot,
	}
	kv.coordTxns[op.TxnID] = rec
	res := OpResult{Err: OK, RPCID: op.RPCID}
	kv.lastOps[op.ClientID] = res
	return res
}

func (kv *ShardKV) applyTxnBranchBegin(op Op, index int) OpResult {
	if lastRes, ok := kv.lastOps[op.ClientID]; ok && op.RPCID <= lastRes.RPCID {
		return lastRes
	}
	if op.ConfigNum != kv.config.Num {
		return OpResult{Err: ErrConfigNotReady, RPCID: op.RPCID}
	}
	if !kv.canServe(op.ShardID) {
		return OpResult{Err: ErrWrongGroup, RPCID: op.RPCID}
	}
	if rec, ok := kv.branchTxns[op.TxnID]; ok {
		res := OpResult{
			Err:       OK,
			RPCID:     op.RPCID,
			TxnID:     op.TxnID,
			Snapshot:  rec.Snapshot,
			ConfigNum: rec.ConfigNum,
			GID:       rec.GID,
			ShardID:   rec.ShardID,
		}
		kv.lastOps[op.ClientID] = res
		return res
	}

	snapshot := uint64(index)
	kv.branchTxns[op.TxnID] = BranchTxnRecord{
		TxnID:     op.TxnID,
		ConfigNum: op.ConfigNum,
		CoordGID:  op.CoordGID,
		GID:       kv.gid,
		ShardID:   op.ShardID,
		Snapshot:  snapshot,
		Isolation: op.Isolation,
		State:     BranchTxnBegun,
	}
	kv.registerTxnLocked(op.TxnID, snapshot)

	res := OpResult{
		Err:       OK,
		RPCID:     op.RPCID,
		TxnID:     op.TxnID,
		Snapshot:  snapshot,
		ConfigNum: op.ConfigNum,
		GID:       kv.gid,
		ShardID:   op.ShardID,
	}
	kv.lastOps[op.ClientID] = res
	return res
}

func (kv *ShardKV) applyTxnCoordPrepare(op Op) OpResult {
	if lastRes, ok := kv.lastOps[op.ClientID]; ok && op.RPCID <= lastRes.RPCID {
		return lastRes
	}
	if op.ConfigNum != kv.config.Num {
		return OpResult{Err: ErrConfigNotReady, RPCID: op.RPCID}
	}
	rec, ok := kv.coordTxns[op.TxnID]
	if !ok {
		return OpResult{Err: ErrNotReady, RPCID: op.RPCID}
	}
	if rec.Branches == nil {
		rec.Branches = make(map[int]CoordTxnBranchRecord)
	}
	rec.State = CoordTxnPreparing
	rec.Branches[op.GID] = CoordTxnBranchRecord{
		GID:      op.GID,
		ShardID:  op.ShardID,
		Snapshot: op.Snapshot,
		Reads:    append([]TxnRead(nil), op.Reads...),
		Writes:   append([]TxnWrite(nil), op.Writes...),
		Prepared: false,
	}
	kv.coordTxns[op.TxnID] = rec
	res := OpResult{Err: OK, RPCID: op.RPCID}
	kv.lastOps[op.ClientID] = res
	return res
}

func (kv *ShardKV) applyTxnCoordAbort(op Op) OpResult {
	if lastRes, ok := kv.lastOps[op.ClientID]; ok && op.RPCID <= lastRes.RPCID {
		return lastRes
	}
	if rec, ok := kv.coordTxns[op.TxnID]; ok {
		rec.State = CoordTxnAborted
		kv.coordTxns[op.TxnID] = rec
		delete(kv.coordTxns, op.TxnID)
	}
	res := OpResult{Err: OK, RPCID: op.RPCID}
	kv.lastOps[op.ClientID] = res
	return res
}

func (kv *ShardKV) applyTxnCoordCommit(op Op) OpResult {
	if lastRes, ok := kv.lastOps[op.ClientID]; ok && op.RPCID <= lastRes.RPCID {
		return lastRes
	}
	if op.ConfigNum != kv.config.Num {
		return OpResult{Err: ErrConfigNotReady, RPCID: op.RPCID}
	}
	rec, ok := kv.coordTxns[op.TxnID]
	if !ok {
		return OpResult{Err: ErrNotReady, RPCID: op.RPCID}
	}
	rec.State = CoordTxnCommitted
	kv.coordTxns[op.TxnID] = rec
	res := OpResult{Err: OK, RPCID: op.RPCID}
	kv.lastOps[op.ClientID] = res
	return res
}

func (kv *ShardKV) applyTxnCoordFinish(op Op) OpResult {
	if lastRes, ok := kv.lastOps[op.ClientID]; ok && op.RPCID <= lastRes.RPCID {
		return lastRes
	}
	delete(kv.coordTxns, op.TxnID)
	res := OpResult{Err: OK, RPCID: op.RPCID}
	kv.lastOps[op.ClientID] = res
	return res
}

func (kv *ShardKV) applyTxnBranchPrepare(op Op) OpResult {
	if lastRes, ok := kv.lastOps[op.ClientID]; ok && op.RPCID <= lastRes.RPCID {
		return lastRes
	}
	if op.ConfigNum != kv.config.Num {
		return OpResult{Err: ErrConfigNotReady, RPCID: op.RPCID}
	}
	if !kv.canServe(op.ShardID) {
		return OpResult{Err: ErrWrongGroup, RPCID: op.RPCID}
	}
	rec, ok := kv.branchTxns[op.TxnID]
	if !ok {
		return OpResult{Err: ErrNotReady, RPCID: op.RPCID}
	}
	if rec.Snapshot != op.Snapshot {
		return OpResult{Err: ErrConflict, RPCID: op.RPCID}
	}
	if rec.Prepared {
		res := OpResult{Err: OK, RPCID: op.RPCID}
		kv.lastOps[op.ClientID] = res
		return res
	}
	engine := kv.ensureShardEngine(op.ShardID)
	if op.Isolation == RepeatableRead || op.Isolation == Serializable {
		for _, r := range op.Reads {
			_, tid := engine.Get(r.Key, 0)
			if tid != r.Version {
				res := OpResult{Err: ErrConflict, RPCID: op.RPCID}
				kv.lastOps[op.ClientID] = res
				return res
			}
		}
	}
	for _, w := range op.Writes {
		if owner, ok := kv.preparedKeys[w.Key]; ok && owner != op.TxnID {
			res := OpResult{Err: ErrConflict, RPCID: op.RPCID}
			kv.lastOps[op.ClientID] = res
			return res
		}
	}
	for _, w := range op.Writes {
		kv.preparedKeys[w.Key] = op.TxnID
	}
	rec.State = BranchTxnPrepared
	rec.Prepared = true
	rec.Reads = append([]TxnRead(nil), op.Reads...)
	rec.Writes = append([]TxnWrite(nil), op.Writes...)
	kv.branchTxns[op.TxnID] = rec
	res := OpResult{Err: OK, RPCID: op.RPCID}
	kv.lastOps[op.ClientID] = res
	return res
}

func (kv *ShardKV) applyTxnBranchCommit(op Op, index int) OpResult {
	if lastRes, ok := kv.lastOps[op.ClientID]; ok && op.RPCID <= lastRes.RPCID {
		return lastRes
	}
	if op.ConfigNum != kv.config.Num {
		return OpResult{Err: ErrConfigNotReady, RPCID: op.RPCID}
	}
	if !kv.canServe(op.ShardID) {
		return OpResult{Err: ErrWrongGroup, RPCID: op.RPCID}
	}
	rec, ok := kv.branchTxns[op.TxnID]
	if !ok {
		return OpResult{Err: ErrNotReady, RPCID: op.RPCID}
	}
	if !rec.Prepared {
		return OpResult{Err: ErrNotReady, RPCID: op.RPCID}
	}

	engine := kv.ensureShardEngine(op.ShardID)
	trancID := uint64(index)
	if len(rec.Writes) > 0 {
		putBatch := make([]lsm.KV, 0, len(rec.Writes))
		deleteBatch := make([]string, 0, len(rec.Writes))
		for _, w := range rec.Writes {
			if w.Delete {
				deleteBatch = append(deleteBatch, w.Key)
			} else {
				putBatch = append(putBatch, lsm.KV{Key: w.Key, Value: encodeValue(w.Value)})
			}
		}
		if len(putBatch) > 0 {
			engine.PutBatch(putBatch, trancID)
		}
		if len(deleteBatch) > 0 {
			engine.RemoveBatch(deleteBatch, trancID)
		}
	}
	kv.releasePreparedKeysLocked(rec.Writes, op.TxnID)
	delete(kv.branchTxns, op.TxnID)
	kv.unregisterTxnLocked(op.TxnID)

	res := OpResult{Err: OK, RPCID: op.RPCID}
	kv.lastOps[op.ClientID] = res
	return res
}

func (kv *ShardKV) applyTxnBranchAbort(op Op) OpResult {
	if lastRes, ok := kv.lastOps[op.ClientID]; ok && op.RPCID <= lastRes.RPCID {
		return lastRes
	}
	if rec, ok := kv.branchTxns[op.TxnID]; ok {
		kv.releasePreparedKeysLocked(rec.Writes, op.TxnID)
		delete(kv.branchTxns, op.TxnID)
	}
	kv.unregisterTxnLocked(op.TxnID)
	res := OpResult{Err: OK, RPCID: op.RPCID}
	kv.lastOps[op.ClientID] = res
	return res
}

func (kv *ShardKV) releasePreparedKeysLocked(writes []TxnWrite, txnID uint64) {
	for _, w := range writes {
		if owner, ok := kv.preparedKeys[w.Key]; ok && owner == txnID {
			delete(kv.preparedKeys, w.Key)
		}
	}
}

func (kv *ShardKV) hasPreparedTxnLocked() bool {
	for _, rec := range kv.branchTxns {
		if rec.Prepared {
			return true
		}
	}
	return false
}

func (kv *ShardKV) hasBlockingCoordTxnLocked() bool {
	for _, rec := range kv.coordTxns {
		if rec.State == CoordTxnBegun || rec.State == CoordTxnPreparing {
			return true
		}
	}
	return false
}

func (kv *ShardKV) hasPreparedTxnOnShardLocked(shard int) bool {
	for _, rec := range kv.branchTxns {
		if rec.Prepared && rec.ShardID == shard {
			return true
		}
	}
	return false
}

// --- MVCC GC helpers (must hold lock) ---

func (kv *ShardKV) registerTxnLocked(txnID uint64, snapshot uint64) {
	if txnID == 0 {
		return
	}
	kv.activeTxn[txnID] = snapshot
	kv.activeTxnLast[txnID] = time.Now()
	kv.updateGCWatermarkLocked()
}

func (kv *ShardKV) touchTxnLocked(txnID uint64) {
	if txnID == 0 {
		return
	}
	if _, ok := kv.activeTxn[txnID]; ok {
		kv.activeTxnLast[txnID] = time.Now()
	}
}

func (kv *ShardKV) unregisterTxnLocked(txnID uint64) {
	if txnID == 0 {
		return
	}
	if _, ok := kv.activeTxn[txnID]; ok {
		delete(kv.activeTxn, txnID)
		delete(kv.activeTxnLast, txnID)
		kv.updateGCWatermarkLocked()
	}
}

func (kv *ShardKV) updateGCWatermarkLocked() {
	minSnap := uint64(kv.lastApplied)
	if len(kv.activeTxn) > 0 {
		minSnap = ^uint64(0)
		for _, s := range kv.activeTxn {
			if s < minSnap {
				minSnap = s
			}
		}
	}
	if minSnap == kv.gcWatermark {
		return
	}
	kv.gcWatermark = minSnap
	for _, engine := range kv.kvDB {
		if engine != nil {
			engine.SetGCWatermark(minSnap)
		}
	}
}

func (kv *ShardKV) monitorTxnGC() {
	for !kv.killed() {
		time.Sleep(TxnGCInterval)
		kv.mu.Lock()
		if len(kv.activeTxnLast) > 0 {
			now := time.Now()
			changed := false
			for id, ts := range kv.activeTxnLast {
				if now.Sub(ts) > TxnTTL {
					delete(kv.activeTxnLast, id)
					delete(kv.activeTxn, id)
					changed = true
				}
			}
			if changed {
				kv.updateGCWatermarkLocked()
			}
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) internalClientID() int64 {
	return -int64(kv.gid*1000 + kv.me + 1)
}

func (kv *ShardKV) allocInternalRPCID() int64 {
	return atomic.AddInt64(&kv.nextInternalRPCID, 1)
}

func (kv *ShardKV) coordinatorStatus(rec BranchTxnRecord) (CoordTxnState, Err) {
	cfg := kv.mck.Query(rec.ConfigNum)
	servers, ok := cfg.Groups[rec.CoordGID]
	if !ok || rec.CoordGID == 0 || len(servers) == 0 {
		return 0, ErrWrongGroup
	}
	args := TxnCoordStatusArgs{
		TxnID:     rec.TxnID,
		ConfigNum: rec.ConfigNum,
	}
	for _, server := range servers {
		srv := kv.make_end(server)
		var reply TxnCoordStatusReply
		ok := srv.Call("ShardKV.TxnCoordStatus", &args, &reply)
		if ok && (reply.Err == OK || reply.Err == "") {
			return reply.State, OK
		}
	}
	return 0, ErrTimeout
}

func (kv *ShardKV) monitorTxnRecovery() {
	for !kv.killed() {
		time.Sleep(TxnRecoveryInterval)
		if _, isLeader := kv.rf.GetState(); !isLeader {
			continue
		}

		kv.mu.Lock()
		pending := make([]BranchTxnRecord, 0)
		for _, rec := range kv.branchTxns {
			if rec.Prepared {
				pending = append(pending, rec)
			}
		}
		kv.mu.Unlock()

		for _, rec := range pending {
			state, err := kv.coordinatorStatus(rec)
			if err != OK {
				continue
			}

			switch state {
			case CoordTxnCommitted:
				kv.startOp(Op{
					Type:      TXNBRANCHCOMMIT,
					TxnID:     rec.TxnID,
					ClientID:  kv.internalClientID(),
					RPCID:     kv.allocInternalRPCID(),
					ConfigNum: rec.ConfigNum,
					ShardID:   rec.ShardID,
				})
			case CoordTxnAborted:
				kv.startOp(Op{
					Type:      TXNBRANCHABORT,
					TxnID:     rec.TxnID,
					ClientID:  kv.internalClientID(),
					RPCID:     kv.allocInternalRPCID(),
					ConfigNum: rec.ConfigNum,
					ShardID:   rec.ShardID,
				})
			}
		}
	}
}

// --- LSM helpers ---

const lsmValuePrefix byte = 0x01

func encodeValue(v string) string {
	b := make([]byte, 1+len(v))
	b[0] = lsmValuePrefix
	copy(b[1:], v)
	return string(b)
}

func decodeValue(v string) (string, bool) {
	if v == "" {
		return "", false
	}
	if v[0] == lsmValuePrefix {
		return v[1:], true
	}
	// fallback for legacy/unknown encoding
	return v, true
}

func (kv *ShardKV) shardRootDir() string {
	return filepath.Join(os.TempDir(), fmt.Sprintf("shardkv-%d-%d", kv.gid, kv.me))
}

func (kv *ShardKV) shardDir(shard int) string {
	return filepath.Join(kv.shardRootDir(), fmt.Sprintf("shard-%d", shard))
}

func (kv *ShardKV) newShardEngine(shard int) *lsm.LSMEngine {
	dir := kv.shardDir(shard)
	_ = os.RemoveAll(dir)
	_ = os.MkdirAll(dir, os.ModePerm)
	engine := lsm.NewLSMEngine(dir)
	engine.SetGCWatermark(kv.gcWatermark)
	return engine
}

func (kv *ShardKV) ensureShardEngine(shard int) *lsm.LSMEngine {
	if kv.kvDB[shard] == nil {
		kv.kvDB[shard] = kv.newShardEngine(shard)
	}
	return kv.kvDB[shard]
}

func (kv *ShardKV) exportShardDataAt(engine *lsm.LSMEngine, snapshot uint64) map[string]string {
	out := make(map[string]string)
	if engine == nil {
		return out
	}
	start := engine.ScanFrom(snapshot, "")
	if start == nil {
		return out
	}
	defer start.Close()
	for start.Valid() {
		k := start.Key()
		v, ok := decodeValue(start.Value())
		if ok {
			out[k] = v
		}
		start.Next()
	}
	return out
}

func (kv *ShardKV) exportAllShardsAt(snapshot uint64, engines map[int]*lsm.LSMEngine) map[int]map[string]string {
	out := make(map[int]map[string]string)
	for shard, engine := range engines {
		if engine != nil {
			out[shard] = kv.exportShardDataAt(engine, snapshot)
		}
	}
	return out
}

func cloneIntStringMap(src map[string]string) map[string]string {
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func cloneShadowDB(src map[int]map[int]map[string]string) map[int]map[int]map[string]string {
	dst := make(map[int]map[int]map[string]string, len(src))
	for cfgNum, shards := range src {
		shardCopy := make(map[int]map[string]string, len(shards))
		for shardID, data := range shards {
			shardCopy[shardID] = cloneIntStringMap(data)
		}
		dst[cfgNum] = shardCopy
	}
	return dst
}

func cloneShardStatus(src map[int]int) map[int]int {
	dst := make(map[int]int, len(src))
	for shard, status := range src {
		dst[shard] = status
	}
	return dst
}

func cloneLastOps(src map[int64]OpResult) map[int64]OpResult {
	dst := make(map[int64]OpResult, len(src))
	for clientID, res := range src {
		dst[clientID] = res
	}
	return dst
}

func cloneTxnReads(src []TxnRead) []TxnRead {
	if len(src) == 0 {
		return nil
	}
	dst := make([]TxnRead, len(src))
	copy(dst, src)
	return dst
}

func cloneTxnWrites(src []TxnWrite) []TxnWrite {
	if len(src) == 0 {
		return nil
	}
	dst := make([]TxnWrite, len(src))
	copy(dst, src)
	return dst
}

func cloneCoordTxns(src map[uint64]CoordTxnRecord) map[uint64]CoordTxnRecord {
	dst := make(map[uint64]CoordTxnRecord, len(src))
	for txnID, rec := range src {
		recCopy := rec
		if rec.Participants != nil {
			recCopy.Participants = make(map[int]CoordTxnParticipant, len(rec.Participants))
			for gid, p := range rec.Participants {
				recCopy.Participants[gid] = p
			}
		}
		if rec.Branches != nil {
			recCopy.Branches = make(map[int]CoordTxnBranchRecord, len(rec.Branches))
			for gid, b := range rec.Branches {
				bCopy := b
				bCopy.Reads = cloneTxnReads(b.Reads)
				bCopy.Writes = cloneTxnWrites(b.Writes)
				recCopy.Branches[gid] = bCopy
			}
		}
		dst[txnID] = recCopy
	}
	return dst
}

func cloneBranchTxns(src map[uint64]BranchTxnRecord) map[uint64]BranchTxnRecord {
	dst := make(map[uint64]BranchTxnRecord, len(src))
	for txnID, rec := range src {
		recCopy := rec
		recCopy.Reads = cloneTxnReads(rec.Reads)
		recCopy.Writes = cloneTxnWrites(rec.Writes)
		dst[txnID] = recCopy
	}
	return dst
}

func clonePreparedKeys(src map[string]uint64) map[string]uint64 {
	dst := make(map[string]uint64, len(src))
	for key, txnID := range src {
		dst[key] = txnID
	}
	return dst
}

func (kv *ShardKV) captureSnapshotTaskLocked(index int) *snapshotTask {
	engines := make(map[int]*lsm.LSMEngine, len(kv.kvDB))
	for shard, engine := range kv.kvDB {
		if engine != nil {
			engines[shard] = engine
		}
	}
	kv.snapshotRefsWG.Add(1)
	return &snapshotTask{
		index:        index,
		engines:      engines,
		shadowDB:     cloneShadowDB(kv.shadowDB),
		shardState:   cloneShardStatus(kv.shardStatus),
		lastOps:      cloneLastOps(kv.lastOps),
		config:       kv.config,
		lastConfig:   kv.lastConfig,
		coordTxns:    cloneCoordTxns(kv.coordTxns),
		branchTxns:   cloneBranchTxns(kv.branchTxns),
		preparedKeys: clonePreparedKeys(kv.preparedKeys),
	}
}

func (kv *ShardKV) releaseSnapshotTask(task *snapshotTask) {
	if task != nil {
		kv.snapshotRefsWG.Done()
	}
}

func (kv *ShardKV) enqueueSnapshotLocked(index int) {
	task := kv.captureSnapshotTaskLocked(index)

	var dropped *snapshotTask
	kv.snapshotMu.Lock()
	if kv.pendingSnap != nil && kv.pendingSnap.index >= task.index {
		dropped = task
	} else {
		dropped = kv.pendingSnap
		kv.pendingSnap = task
	}
	kv.snapshotMu.Unlock()

	if dropped != nil {
		kv.releaseSnapshotTask(dropped)
	}

	select {
	case kv.snapshotNotify <- struct{}{}:
	default:
	}
}

func (kv *ShardKV) restoreAllShards(snapshot map[int]map[string]string) {
	kv.snapshotRefsWG.Wait()
	for _, engine := range kv.kvDB {
		if engine != nil {
			engine.Close()
		}
	}
	kv.kvDB = make(map[int]*lsm.LSMEngine)
	for shard, data := range snapshot {
		engine := kv.newShardEngine(shard)
		for k, v := range data {
			engine.Put(k, encodeValue(v), 0)
		}
		kv.kvDB[shard] = engine
	}
}

// --- Background tasks ---

func (kv *ShardKV) monitorConfig() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			canNext := true
			for _, status := range kv.shardStatus {
				if status == Pulling || status == GCing {
					canNext = false
					break
				}
			}
			if canNext && (kv.hasBlockingCoordTxnLocked() || len(kv.branchTxns) > 0 || kv.hasPreparedTxnLocked()) {
				canNext = false
			}
			curNum := kv.config.Num
			kv.mu.Unlock()

			if canNext {
				nextConfig := kv.mck.Query(curNum + 1)
				if nextConfig.Num == curNum+1 {
					kv.startOp(Op{
						Type:   RECONFIG,
						Config: nextConfig,
					})
				}
			}
		}
		time.Sleep(UpConfigLoopInterval)
	}
}

func (kv *ShardKV) monitorMigration() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			var wg sync.WaitGroup

			for shardID, status := range kv.shardStatus {
				if status == Pulling {
					gid := kv.lastConfig.Shards[shardID]
					servers := kv.lastConfig.Groups[gid]
					configNum := kv.lastConfig.Num // data produced under lastConfig

					wg.Add(1)
					go func(sID, cNum int, gServers []string) {
						defer wg.Done()
						args := PullDataArgs{ConfigNum: cNum, ShardIndex: sID}

						for _, server := range gServers {
							srv := kv.make_end(server)
							var reply PullDataReply
							if srv.Call("ShardKV.PullData", &args, &reply) && reply.Err == OK {
								kv.startOp(Op{
									Type:      INSERTSHARD,
									ConfigNum: kv.config.Num, // use current config number
									ShardID:   sID,
									ShardData: reply.ShardData,
									LastOpMap: reply.LastOpMap,
								})
								return
							}
						}
					}(shardID, configNum, servers)
				}
			}
			kv.mu.Unlock()
			wg.Wait()
		}
		time.Sleep(GetShardsInterval)
	}
}

func (kv *ShardKV) monitorGC() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			var wg sync.WaitGroup

			for shardID, status := range kv.shardStatus {
				if status == GCing {
					gid := kv.lastConfig.Shards[shardID]
					servers := kv.lastConfig.Groups[gid]
					configNum := kv.lastConfig.Num

					wg.Add(1)
					go func(sID, cNum int, gServers []string) {
						defer wg.Done()
						args := PullDataArgs{ConfigNum: cNum, ShardIndex: sID}
						var reply PullDataReply

						for _, server := range gServers {
							srv := kv.make_end(server)
							if srv.Call("ShardKV.DeleteShard", &args, &reply) && reply.Err == OK {
								kv.mu.Lock()
								if kv.shardStatus[sID] == GCing {
									kv.shardStatus[sID] = Serving
								}
								kv.mu.Unlock()
								return
							}
						}
					}(shardID, configNum, servers)
				}
			}
			kv.mu.Unlock()
			wg.Wait()
		}
		time.Sleep(GCInterval)
	}
}

// --- Snapshot ---

func (kv *ShardKV) runSnapshotTask(task *snapshotTask) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kvdbSnapshot := kv.exportAllShardsAt(uint64(task.index), task.engines)
	e.Encode(kvdbSnapshot)
	e.Encode(task.shadowDB)
	e.Encode(task.shardState)
	e.Encode(task.lastOps)
	e.Encode(task.config)
	e.Encode(task.lastConfig)
	e.Encode(task.coordTxns)
	e.Encode(task.branchTxns)
	e.Encode(task.preparedKeys)

	kv.rf.Snapshot(task.index, w.Bytes())
}

func (kv *ShardKV) snapshotWorker() {
	defer kv.snapshotWG.Done()
	for {
		select {
		case <-kv.snapshotNotify:
		case <-kv.snapshotStop:
			kv.snapshotMu.Lock()
			pending := kv.pendingSnap
			kv.pendingSnap = nil
			kv.snapshotMu.Unlock()
			kv.releaseSnapshotTask(pending)
			return
		}

		for {
			kv.snapshotMu.Lock()
			task := kv.pendingSnap
			kv.pendingSnap = nil
			kv.snapshotMu.Unlock()
			if task == nil {
				break
			}
			kv.runSnapshotTask(task)
			kv.releaseSnapshotTask(task)
		}
	}
}

func (kv *ShardKV) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		kv.coordTxns = make(map[uint64]CoordTxnRecord)
		kv.branchTxns = make(map[uint64]BranchTxnRecord)
		kv.preparedKeys = make(map[string]uint64)
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var kvDB map[int]map[string]string
	var shadowDB map[int]map[int]map[string]string
	var shardStatus map[int]int
	var lastOps map[int64]OpResult
	var config shardctrler.Config
	var lastConfig shardctrler.Config
	var coordTxns map[uint64]CoordTxnRecord
	var branchTxns map[uint64]BranchTxnRecord
	var preparedKeys map[string]uint64

	if d.Decode(&kvDB) != nil ||
		d.Decode(&shadowDB) != nil ||
		d.Decode(&shardStatus) != nil ||
		d.Decode(&lastOps) != nil ||
		d.Decode(&config) != nil ||
		d.Decode(&lastConfig) != nil ||
		d.Decode(&coordTxns) != nil ||
		d.Decode(&branchTxns) != nil ||
		d.Decode(&preparedKeys) != nil {
		log.Fatal("ReadSnapshot decode error")
	} else {
		kv.snapshotRefsWG.Wait()
		kv.restoreAllShards(kvDB)
		kv.shadowDB = shadowDB
		kv.shardStatus = shardStatus
		kv.lastOps = lastOps
		kv.config = config
		kv.lastConfig = lastConfig
		if coordTxns == nil {
			coordTxns = make(map[uint64]CoordTxnRecord)
		}
		if branchTxns == nil {
			branchTxns = make(map[uint64]BranchTxnRecord)
		}
		if preparedKeys == nil {
			preparedKeys = make(map[string]uint64)
		}
		kv.coordTxns = coordTxns
		kv.branchTxns = branchTxns
		kv.preparedKeys = preparedKeys
		kv.activeTxn = make(map[uint64]uint64)
		kv.activeTxnLast = make(map[uint64]time.Time)
		now := time.Now()
		for txnID, rec := range branchTxns {
			kv.activeTxn[txnID] = rec.Snapshot
			kv.activeTxnLast[txnID] = now
		}
	}
}

func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	if kv.snapshotStop != nil {
		close(kv.snapshotStop)
		kv.snapshotWG.Wait()
	}
	kv.rf.Kill()
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	labgob.Register(Op{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(OpResult{})
	labgob.Register(map[string]string{})
	labgob.Register(map[int64]OpResult{})
	labgob.Register(TxnWrite{})
	labgob.Register(TxnRead{})
	labgob.Register([]TxnWrite{})
	labgob.Register([]TxnRead{})
	labgob.Register(CoordTxnParticipant{})
	labgob.Register(CoordTxnBranchRecord{})
	labgob.Register(CoordTxnRecord{})
	labgob.Register(BranchTxnRecord{})
	labgob.Register(map[uint64]CoordTxnRecord{})
	labgob.Register(map[uint64]BranchTxnRecord{})
	labgob.Register(map[string]uint64{})
	labgob.Register(KeyValue{})
	labgob.Register([]KeyValue{})
	labgob.Register(TxnRangeKV{})
	labgob.Register([]TxnRangeKV{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.persister = persister

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.kvDB = make(map[int]*lsm.LSMEngine)
	kv.shadowDB = make(map[int]map[int]map[string]string)
	kv.shardStatus = make(map[int]int)
	kv.lastOps = make(map[int64]OpResult)
	kv.waitCh = make(map[int]chan OpResult)
	kv.activeTxn = make(map[uint64]uint64)
	kv.activeTxnLast = make(map[uint64]time.Time)
	kv.coordTxns = make(map[uint64]CoordTxnRecord)
	kv.branchTxns = make(map[uint64]BranchTxnRecord)
	kv.preparedKeys = make(map[string]uint64)
	kv.snapshotNotify = make(chan struct{}, 1)
	kv.snapshotStop = make(chan struct{})

	kv.readSnapshot(persister.ReadSnapshot())

	kv.snapshotWG.Add(1)
	go kv.snapshotWorker()
	go kv.applier()
	go kv.monitorConfig()
	go kv.monitorMigration()
	go kv.monitorGC()
	go kv.monitorTxnGC()
	go kv.monitorTxnRecovery()

	return kv
}
