package shardkv

import (
	"sort"
	"time"

	"6.5840/shardctrler"
)

type readTxnGroupState struct {
	txnID    uint64
	snapshot uint64
}

// ReadTxn is a client-side cross-shard read-only transaction.
// It pins a shardctrler configuration and captures one snapshot per target group.
type ReadTxn struct {
	ck        *Clerk
	isolation IsolationLevel
	config    shardctrler.Config
	groups    map[int]readTxnGroupState
	invalid   bool
}

// BeginReadTxn starts a cross-shard read-only transaction on a fixed config.
func (ck *Clerk) BeginReadTxn(level IsolationLevel) *ReadTxn {
	cfg := ck.sm.Query(-1)
	tx := &ReadTxn{
		ck:        ck,
		isolation: level,
		config:    cfg,
		groups:    make(map[int]readTxnGroupState),
	}
	if !tx.beginGroups() {
		tx.invalid = true
	}
	return tx
}

func (tx *ReadTxn) beginGroups() bool {
	gids := tx.targetGIDs()
	for _, gid := range gids {
		txnID, snapshot, err := tx.ck.txnBeginOnGroup(tx.config, gid, tx.isolation)
		if err != OK {
			return false
		}
		tx.groups[gid] = readTxnGroupState{
			txnID:    txnID,
			snapshot: snapshot,
		}
	}
	return true
}

func (tx *ReadTxn) targetGIDs() []int {
	seen := make(map[int]struct{})
	out := make([]int, 0)
	for _, gid := range tx.config.Shards {
		if gid == 0 {
			continue
		}
		if _, ok := tx.config.Groups[gid]; !ok {
			continue
		}
		if _, ok := seen[gid]; ok {
			continue
		}
		seen[gid] = struct{}{}
		out = append(out, gid)
	}
	sort.Ints(out)
	return out
}

func (tx *ReadTxn) stateForShard(shard int) (readTxnGroupState, bool) {
	gid := tx.config.Shards[shard]
	if gid == 0 {
		return readTxnGroupState{}, false
	}
	state, ok := tx.groups[gid]
	return state, ok
}

// Get reads a key using the pinned per-group snapshot.
func (tx *ReadTxn) Get(key string) (string, bool) {
	if tx.invalid {
		return "", false
	}

	shard := key2shard(key)
	state, ok := tx.stateForShard(shard)
	if !ok {
		tx.invalid = true
		return "", false
	}

	val, _, err := tx.ck.txnGetOnShardWithConfig(tx.config, shard, key, state.snapshot, state.txnID)
	switch err {
	case OK:
		return val, true
	case ErrNoKey:
		return "", false
	default:
		tx.invalid = true
		return "", false
	}
}

// Range reads [start, end) from the pinned cross-shard snapshots.
func (tx *ReadTxn) Range(start, end string, limit int) ([]KeyValue, bool) {
	if tx.invalid {
		return nil, false
	}

	targetShards := rangeTargetShards(start, end)
	if len(targetShards) == 0 {
		return nil, true
	}

	all := make([]KeyValue, 0)
	for _, shard := range targetShards {
		state, ok := tx.stateForShard(shard)
		if !ok {
			tx.invalid = true
			return nil, false
		}
		kvs, err := tx.ck.txnRangeOnShardWithConfig(tx.config, shard, start, end, 0, state.snapshot, state.txnID)
		if err != OK && err != ErrNoKey {
			tx.invalid = true
			return nil, false
		}
		for _, kv := range kvs {
			all = append(all, KeyValue{Key: kv.Key, Value: kv.Value})
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Key < all[j].Key })
	if limit > 0 && len(all) > limit {
		all = all[:limit]
	}
	return all, true
}

func (tx *ReadTxn) cleanupRemote() {
	for gid, state := range tx.groups {
		servers := tx.config.Groups[gid]
		tx.ck.txnAbortOnServers(gid, servers, state.txnID)
	}
}

// Commit is a no-op for read-only transactions.
func (tx *ReadTxn) Commit() bool {
	ok := !tx.invalid
	tx.cleanupRemote()
	tx.invalid = true
	tx.groups = nil
	return ok
}

// Abort marks the transaction invalid locally.
func (tx *ReadTxn) Abort() {
	tx.cleanupRemote()
	tx.invalid = true
	tx.groups = nil
}

func (ck *Clerk) txnBeginOnGroup(cfg shardctrler.Config, gid int, level IsolationLevel) (uint64, uint64, Err) {
	servers, ok := cfg.Groups[gid]
	if !ok || gid == 0 || len(servers) == 0 {
		return 0, 0, ErrWrongGroup
	}

	args := TxnBeginArgs{
		ClientID:  ck.ClientID,
		RPCID:     ck.allocRPCID(),
		Isolation: level,
	}

	for {
		for _, si := range ck.serverOrder(gid, len(servers)) {
			srv := ck.makeEnd(servers[si])
			var reply TxnBeginReply
			ok := srv.Call("ShardKV.TxnBegin", &args, &reply)
			if ok && reply.Err == OK {
				ck.rememberLeader(gid, si)
				return reply.TxnID, reply.Snapshot, OK
			}
			if ok && reply.Err == ErrWrongGroup {
				ck.forgetLeader(gid, si)
				return 0, 0, ErrWrongGroup
			}
			ck.forgetLeader(gid, si)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) txnGetOnShardWithConfig(cfg shardctrler.Config, shard int, key string, snapshot uint64, txnID uint64) (string, uint64, Err) {
	gid := cfg.Shards[shard]
	servers, ok := cfg.Groups[gid]
	if !ok || gid == 0 || len(servers) == 0 {
		return "", 0, ErrWrongGroup
	}

	args := TxnGetArgs{
		Key:      key,
		Snapshot: snapshot,
		TxnID:    txnID,
		ClientID: ck.ClientID,
		RPCID:    ck.allocRPCID(),
	}

	for {
		for _, si := range ck.serverOrder(gid, len(servers)) {
			srv := ck.makeEnd(servers[si])
			var reply TxnGetReply
			ok := srv.Call("ShardKV.TxnGet", &args, &reply)
			if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
				ck.rememberLeader(gid, si)
				return reply.Value, reply.Version, reply.Err
			}
			if ok && reply.Err == ErrWrongGroup {
				ck.forgetLeader(gid, si)
				return "", 0, ErrWrongGroup
			}
			ck.forgetLeader(gid, si)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) txnRangeOnShardWithConfig(cfg shardctrler.Config, shard int, start, end string, limit int, snapshot uint64, txnID uint64) ([]TxnRangeKV, Err) {
	gid := cfg.Shards[shard]
	servers, ok := cfg.Groups[gid]
	if !ok || gid == 0 || len(servers) == 0 {
		return nil, ErrWrongGroup
	}

	args := TxnRangeArgs{
		Start:    start,
		End:      end,
		Limit:    limit,
		ShardID:  shard,
		Snapshot: snapshot,
		TxnID:    txnID,
		ClientID: ck.ClientID,
		RPCID:    ck.allocRPCID(),
	}

	for {
		for _, si := range ck.serverOrder(gid, len(servers)) {
			srv := ck.makeEnd(servers[si])
			var reply TxnRangeReply
			ok := srv.Call("ShardKV.TxnRange", &args, &reply)
			if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
				ck.rememberLeader(gid, si)
				return reply.KVs, reply.Err
			}
			if ok && reply.Err == ErrWrongGroup {
				ck.forgetLeader(gid, si)
				return nil, ErrWrongGroup
			}
			ck.forgetLeader(gid, si)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
