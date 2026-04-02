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
	ShardID    int
	ConfigNum  int
}

type OpResult struct {
	Err   Err
	Value string
	KVs   []KeyValue
	RPCID int64
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

	nextTxnID   uint64
	lastApplied int

	activeTxn     map[uint64]uint64
	activeTxnLast map[uint64]time.Time
	gcWatermark   uint64
}

// Check strictly if I can serve this key
func (kv *ShardKV) canServe(shard int) bool {
	return kv.config.Shards[shard] == kv.gid && (kv.shardStatus[shard] == Serving || kv.shardStatus[shard] == GCing)
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

	startIt, endIt, ok := engine.LsmItersMonotonyPredicate(args.Snapshot, pred)
	if !ok || startIt == nil {
		reply.Err = OK
		reply.KVs = nil
		return
	}
	defer startIt.Close()
	if endIt != nil {
		defer endIt.Close()
	}

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
				kv.snapshot(index)
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

	startIt, endIt, ok := engine.LsmItersMonotonyPredicate(uint64(index), pred)
	if !ok || startIt == nil {
		res := OpResult{Err: OK, RPCID: op.RPCID, KVs: nil}
		kv.lastOps[op.ClientID] = res
		return res
	}
	defer startIt.Close()
	if endIt != nil {
		defer endIt.Close()
	}

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
						kv.shadowDB[kv.lastConfig.Num][i] = kv.exportShardData(engine)
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
		for k, v := range op.ShardData {
			engine.Put(k, encodeValue(v), 0)
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
	for _, w := range op.Writes {
		if w.Delete {
			engine.Remove(w.Key, trancID)
		} else {
			engine.Put(w.Key, encodeValue(w.Value), trancID)
		}
	}

	res := OpResult{Err: OK, RPCID: op.RPCID}
	kv.lastOps[op.ClientID] = res
	return res
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

func (kv *ShardKV) exportShardData(engine *lsm.LSMEngine) map[string]string {
	out := make(map[string]string)
	if engine == nil {
		return out
	}
	start, end, ok := engine.LsmItersMonotonyPredicate(0, func(string) int { return 0 })
	if !ok || start == nil {
		return out
	}
	defer start.Close()
	if end != nil {
		defer end.Close()
	}
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

func (kv *ShardKV) exportAllShards() map[int]map[string]string {
	out := make(map[int]map[string]string)
	for shard, engine := range kv.kvDB {
		if engine != nil {
			out[shard] = kv.exportShardData(engine)
		}
	}
	return out
}

func (kv *ShardKV) restoreAllShards(snapshot map[int]map[string]string) {
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

func (kv *ShardKV) snapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	kvdbSnapshot := kv.exportAllShards()
	e.Encode(kvdbSnapshot)
	e.Encode(kv.shadowDB)
	e.Encode(kv.shardStatus)
	e.Encode(kv.lastOps)
	e.Encode(kv.config)
	e.Encode(kv.lastConfig)

	kv.rf.Snapshot(index, w.Bytes())
}

func (kv *ShardKV) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
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

	if d.Decode(&kvDB) != nil ||
		d.Decode(&shadowDB) != nil ||
		d.Decode(&shardStatus) != nil ||
		d.Decode(&lastOps) != nil ||
		d.Decode(&config) != nil ||
		d.Decode(&lastConfig) != nil {
		log.Fatal("ReadSnapshot decode error")
	} else {
		kv.restoreAllShards(kvDB)
		kv.shadowDB = shadowDB
		kv.shardStatus = shardStatus
		kv.lastOps = lastOps
		kv.config = config
		kv.lastConfig = lastConfig
	}
}

func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
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

	kv.readSnapshot(persister.ReadSnapshot())

	go kv.applier()
	go kv.monitorConfig()
	go kv.monitorMigration()
	go kv.monitorGC()
	go kv.monitorTxnGC()

	return kv
}
