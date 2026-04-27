package shardkv

import (
	"sort"
	"time"

	"6.5840/shardctrler"
)

type crossTxnBranch struct {
	gid      int
	shard    int
	snapshot uint64
	reads    map[string]uint64
	writes   map[string]*string
}

// CrossShardTxn is the phase-1 client-side skeleton for cross-shard write transactions.
// It fixes a shardctrler config, allocates a coordinator, and lazily begins one branch per group.
type CrossShardTxn struct {
	ck        *Clerk
	txnID     uint64
	coordGID  int
	config    shardctrler.Config
	isolation IsolationLevel
	branches  map[int]*crossTxnBranch
	prepared  bool
	invalid   bool
}

// BeginCrossShardTxn fixes a config and allocates the coordinator transaction record.
func (ck *Clerk) BeginCrossShardTxn(level IsolationLevel, anchorKey string) *CrossShardTxn {
	cfg := ck.sm.Query(-1)
	tx := &CrossShardTxn{
		ck:        ck,
		config:    cfg,
		isolation: level,
		branches:  make(map[int]*crossTxnBranch),
	}
	if anchorKey == "" {
		tx.invalid = true
		return tx
	}

	coordShard := key2shard(anchorKey)
	coordGID := cfg.Shards[coordShard]
	if coordGID == 0 {
		tx.invalid = true
		return tx
	}

	txnID, err := ck.txnCoordBeginOnGroup(cfg, coordGID, anchorKey, level)
	if err != OK {
		tx.invalid = true
		return tx
	}
	tx.txnID = txnID
	tx.coordGID = coordGID
	return tx
}

func (tx *CrossShardTxn) ensureBranchForKey(key string) (*crossTxnBranch, bool) {
	shard := key2shard(key)
	return tx.ensureBranchForShard(shard)
}

func (tx *CrossShardTxn) ensureBranchForShard(shard int) (*crossTxnBranch, bool) {
	if tx.invalid {
		return nil, false
	}
	gid := tx.config.Shards[shard]
	if gid == 0 {
		tx.invalid = true
		return nil, false
	}
	if branch, ok := tx.branches[gid]; ok {
		return branch, true
	}

	snapshot, err := tx.ck.txnBranchBeginOnGroup(tx.config, gid, shard, tx.txnID, tx.coordGID, tx.isolation)
	if err != OK {
		tx.invalid = true
		return nil, false
	}
	if err := tx.ck.txnCoordEnlistOnGroup(tx.config, tx.coordGID, tx.txnID, gid, shard, snapshot); err != OK {
		tx.invalid = true
		return nil, false
	}

	branch := &crossTxnBranch{
		gid:      gid,
		shard:    shard,
		snapshot: snapshot,
		reads:    make(map[string]uint64),
		writes:   make(map[string]*string),
	}
	tx.branches[gid] = branch
	return branch, true
}

// Get reads a key from the fixed config and branch snapshot.
func (tx *CrossShardTxn) Get(key string) (string, bool) {
	branch, ok := tx.ensureBranchForKey(key)
	if !ok {
		return "", false
	}
	if v, ok := branch.writes[key]; ok {
		if v == nil {
			return "", false
		}
		return *v, true
	}

	shard := key2shard(key)
	val, ver, err := tx.ck.txnGetOnShardWithConfig(tx.config, shard, key, branch.snapshot, tx.txnID)
	if err == OK {
		branch.reads[key] = ver
		return val, true
	}
	if err == ErrNoKey {
		branch.reads[key] = 0
	}
	if err != ErrNoKey {
		tx.invalid = true
	}
	return "", false
}

// Put buffers a write in the branch that owns the key.
func (tx *CrossShardTxn) Put(key, value string) bool {
	branch, ok := tx.ensureBranchForKey(key)
	if !ok {
		return false
	}
	v := value
	branch.writes[key] = &v
	return true
}

// Remove buffers a delete in the branch that owns the key.
func (tx *CrossShardTxn) Remove(key string) bool {
	branch, ok := tx.ensureBranchForKey(key)
	if !ok {
		return false
	}
	branch.writes[key] = nil
	return true
}

// Range reads [start, end) over the fixed config snapshots and overlays local writes.
func (tx *CrossShardTxn) Range(start, end string, limit int) ([]KeyValue, bool) {
	if tx.invalid {
		return nil, false
	}

	targetShards := rangeTargetShards(start, end)
	if len(targetShards) == 0 {
		return nil, true
	}

	all := make([]KeyValue, 0)
	for _, shard := range targetShards {
		branch, ok := tx.ensureBranchForShard(shard)
		if !ok {
			return nil, false
		}
		kvs, err := tx.ck.txnRangeOnShardWithConfig(tx.config, shard, start, end, 0, branch.snapshot, tx.txnID)
		if err != OK && err != ErrNoKey {
			tx.invalid = true
			return nil, false
		}
		for _, kv := range kvs {
			if _, hasWrite := branch.writes[kv.Key]; !hasWrite {
				branch.reads[kv.Key] = kv.Version
			}
			all = append(all, KeyValue{Key: kv.Key, Value: kv.Value})
		}
	}

	base := make(map[string]string, len(all))
	for _, kv := range all {
		base[kv.Key] = kv.Value
	}

	inRange := func(k string) bool {
		if k < start {
			return false
		}
		if end != "" && k >= end {
			return false
		}
		return true
	}
	for _, branch := range tx.branches {
		for k, v := range branch.writes {
			if !inRange(k) {
				continue
			}
			if v == nil {
				delete(base, k)
			} else {
				base[k] = *v
			}
		}
	}

	out := make([]KeyValue, 0, len(base))
	for k, v := range base {
		out = append(out, KeyValue{Key: k, Value: v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, true
}

func (tx *CrossShardTxn) prepare() bool {
	if tx.invalid {
		return false
	}
	if tx.prepared {
		return true
	}
	gids := make([]int, 0, len(tx.branches))
	for gid := range tx.branches {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	for _, gid := range gids {
		branch := tx.branches[gid]
		if err := tx.ck.txnCoordPrepareOnGroup(tx.config, tx.coordGID, tx.txnID, branch); err != OK {
			tx.Abort()
			return false
		}
	}
	for _, gid := range gids {
		branch := tx.branches[gid]
		if err := tx.ck.txnBranchPrepareOnGroup(tx.config, tx.txnID, tx.coordGID, tx.isolation, branch); err != OK {
			tx.Abort()
			return false
		}
	}
	tx.prepared = true
	return true
}

func (tx *CrossShardTxn) finishCommitted() {
	tx.invalid = true
	tx.prepared = false
	tx.branches = nil
}

// Commit runs prepare, records the global commit decision, applies each prepared branch, then clears coordinator metadata.
func (tx *CrossShardTxn) Commit() bool {
	if tx.invalid {
		tx.Abort()
		return false
	}
	if len(tx.branches) == 0 {
		tx.Abort()
		return true
	}
	if !tx.prepare() {
		return false
	}
	if err := tx.ck.txnCoordCommitOnGroup(tx.config, tx.coordGID, tx.txnID); err != OK {
		return false
	}

	gids := make([]int, 0, len(tx.branches))
	for gid := range tx.branches {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	for _, gid := range gids {
		branch := tx.branches[gid]
		if err := tx.ck.txnBranchCommitOnGroup(tx.config, gid, tx.txnID, branch.shard); err != OK {
			return false
		}
	}
	if err := tx.ck.txnCoordFinishOnGroup(tx.config, tx.coordGID, tx.txnID); err != OK {
		return false
	}
	tx.finishCommitted()
	return true
}

// Abort releases all branch/coordinator state created so far.
func (tx *CrossShardTxn) Abort() {
	gids := make([]int, 0, len(tx.branches))
	for gid := range tx.branches {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	for _, gid := range gids {
		branch := tx.branches[gid]
		if err := tx.ck.txnBranchAbortOnGroup(tx.config, gid, tx.txnID, branch.shard); err != OK {
			servers := tx.config.Groups[gid]
			tx.ck.txnAbortOnServers(gid, servers, tx.txnID)
		}
	}
	if tx.coordGID != 0 {
		if err := tx.ck.txnCoordAbortOnGroup(tx.config, tx.coordGID, tx.txnID); err != OK {
			servers := tx.config.Groups[tx.coordGID]
			tx.ck.txnAbortOnServers(tx.coordGID, servers, tx.txnID)
		}
	}
	tx.invalid = true
	tx.prepared = false
	tx.branches = nil
}

func (ck *Clerk) txnCoordBeginOnGroup(cfg shardctrler.Config, gid int, anchorKey string, level IsolationLevel) (uint64, Err) {
	servers, ok := cfg.Groups[gid]
	if !ok || gid == 0 || len(servers) == 0 {
		return 0, ErrWrongGroup
	}

	args := TxnCoordBeginArgs{
		ClientID:  ck.ClientID,
		RPCID:     ck.allocRPCID(),
		Isolation: level,
		ConfigNum: cfg.Num,
		AnchorKey: anchorKey,
	}

	for {
		for _, si := range ck.serverOrder(gid, len(servers)) {
			srv := ck.makeEnd(servers[si])
			var reply TxnCoordBeginReply
			ok := srv.Call("ShardKV.TxnCoordBegin", &args, &reply)
			if ok && (reply.Err == OK || reply.Err == "") {
				ck.rememberLeader(gid, si)
				return reply.TxnID, OK
			}
			if ok && reply.Err == ErrWrongGroup {
				ck.forgetLeader(gid, si)
				return 0, reply.Err
			}
			if ok && reply.Err == ErrConfigNotReady {
				ck.forgetLeader(gid, si)
				break
			}
			ck.forgetLeader(gid, si)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) txnCoordEnlistOnGroup(cfg shardctrler.Config, coordGID int, txnID uint64, gid int, shard int, snapshot uint64) Err {
	servers, ok := cfg.Groups[coordGID]
	if !ok || coordGID == 0 || len(servers) == 0 {
		return ErrWrongGroup
	}

	args := TxnCoordEnlistArgs{
		TxnID:     txnID,
		ClientID:  ck.ClientID,
		RPCID:     ck.allocRPCID(),
		ConfigNum: cfg.Num,
		GID:       gid,
		ShardID:   shard,
		Snapshot:  snapshot,
	}

	for {
		for _, si := range ck.serverOrder(coordGID, len(servers)) {
			srv := ck.makeEnd(servers[si])
			var reply TxnCoordEnlistReply
			ok := srv.Call("ShardKV.TxnCoordEnlist", &args, &reply)
			if ok && (reply.Err == OK || reply.Err == "") {
				ck.rememberLeader(coordGID, si)
				return OK
			}
			if ok && reply.Err == ErrWrongGroup {
				ck.forgetLeader(coordGID, si)
				return reply.Err
			}
			if ok && (reply.Err == ErrConfigNotReady || reply.Err == ErrNotReady) {
				ck.forgetLeader(coordGID, si)
				break
			}
			ck.forgetLeader(coordGID, si)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) txnBranchBeginOnGroup(cfg shardctrler.Config, gid int, shard int, txnID uint64, coordGID int, level IsolationLevel) (uint64, Err) {
	servers, ok := cfg.Groups[gid]
	if !ok || gid == 0 || len(servers) == 0 {
		return 0, ErrWrongGroup
	}

	args := TxnBranchBeginArgs{
		TxnID:     txnID,
		ClientID:  ck.ClientID,
		RPCID:     ck.allocRPCID(),
		Isolation: level,
		ConfigNum: cfg.Num,
		CoordGID:  coordGID,
		ShardID:   shard,
	}

	for {
		for _, si := range ck.serverOrder(gid, len(servers)) {
			srv := ck.makeEnd(servers[si])
			var reply TxnBranchBeginReply
			ok := srv.Call("ShardKV.TxnBranchBegin", &args, &reply)
			if ok && (reply.Err == OK || reply.Err == "") {
				ck.rememberLeader(gid, si)
				return reply.Snapshot, OK
			}
			if ok && reply.Err == ErrWrongGroup {
				ck.forgetLeader(gid, si)
				return 0, reply.Err
			}
			if ok && reply.Err == ErrConfigNotReady {
				ck.forgetLeader(gid, si)
				break
			}
			ck.forgetLeader(gid, si)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) txnCoordPrepareOnGroup(cfg shardctrler.Config, coordGID int, txnID uint64, branch *crossTxnBranch) Err {
	servers, ok := cfg.Groups[coordGID]
	if !ok || coordGID == 0 || len(servers) == 0 {
		return ErrWrongGroup
	}
	reads := make([]TxnRead, 0, len(branch.reads))
	for k, v := range branch.reads {
		reads = append(reads, TxnRead{Key: k, Version: v})
	}
	writes := make([]TxnWrite, 0, len(branch.writes))
	for k, v := range branch.writes {
		if v == nil {
			writes = append(writes, TxnWrite{Key: k, Delete: true})
		} else {
			writes = append(writes, TxnWrite{Key: k, Value: *v})
		}
	}

	args := TxnCoordPrepareArgs{
		TxnID:     txnID,
		ClientID:  ck.ClientID,
		RPCID:     ck.allocRPCID(),
		ConfigNum: cfg.Num,
		GID:       branch.gid,
		ShardID:   branch.shard,
		Snapshot:  branch.snapshot,
		Reads:     reads,
		Writes:    writes,
	}

	for {
		for _, si := range ck.serverOrder(coordGID, len(servers)) {
			srv := ck.makeEnd(servers[si])
			var reply TxnCoordPrepareReply
			ok := srv.Call("ShardKV.TxnCoordPrepare", &args, &reply)
			if ok && (reply.Err == OK || reply.Err == "") {
				ck.rememberLeader(coordGID, si)
				return OK
			}
			if ok && reply.Err == ErrWrongGroup {
				ck.forgetLeader(coordGID, si)
				return reply.Err
			}
			if ok && (reply.Err == ErrConfigNotReady || reply.Err == ErrNotReady) {
				ck.forgetLeader(coordGID, si)
				break
			}
			ck.forgetLeader(coordGID, si)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) txnBranchPrepareOnGroup(cfg shardctrler.Config, txnID uint64, coordGID int, level IsolationLevel, branch *crossTxnBranch) Err {
	servers, ok := cfg.Groups[branch.gid]
	if !ok || branch.gid == 0 || len(servers) == 0 {
		return ErrWrongGroup
	}
	reads := make([]TxnRead, 0, len(branch.reads))
	for k, v := range branch.reads {
		reads = append(reads, TxnRead{Key: k, Version: v})
	}
	writes := make([]TxnWrite, 0, len(branch.writes))
	for k, v := range branch.writes {
		if v == nil {
			writes = append(writes, TxnWrite{Key: k, Delete: true})
		} else {
			writes = append(writes, TxnWrite{Key: k, Value: *v})
		}
	}

	args := TxnBranchPrepareArgs{
		TxnID:     txnID,
		ClientID:  ck.ClientID,
		RPCID:     ck.allocRPCID(),
		ConfigNum: cfg.Num,
		CoordGID:  coordGID,
		ShardID:   branch.shard,
		Snapshot:  branch.snapshot,
		Isolation: level,
		Reads:     reads,
		Writes:    writes,
	}

	for {
		for _, si := range ck.serverOrder(branch.gid, len(servers)) {
			srv := ck.makeEnd(servers[si])
			var reply TxnBranchPrepareReply
			ok := srv.Call("ShardKV.TxnBranchPrepare", &args, &reply)
			if ok && (reply.Err == OK || reply.Err == "") {
				ck.rememberLeader(branch.gid, si)
				return OK
			}
			if ok && (reply.Err == ErrWrongGroup || reply.Err == ErrConflict) {
				ck.forgetLeader(branch.gid, si)
				return reply.Err
			}
			if ok && (reply.Err == ErrConfigNotReady || reply.Err == ErrNotReady) {
				ck.forgetLeader(branch.gid, si)
				break
			}
			ck.forgetLeader(branch.gid, si)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) txnCoordAbortOnGroup(cfg shardctrler.Config, coordGID int, txnID uint64) Err {
	servers, ok := cfg.Groups[coordGID]
	if !ok || coordGID == 0 || len(servers) == 0 {
		return ErrWrongGroup
	}

	args := TxnCoordAbortArgs{
		TxnID:     txnID,
		ClientID:  ck.ClientID,
		RPCID:     ck.allocRPCID(),
		ConfigNum: cfg.Num,
	}

	for attempt := 0; attempt < 3; attempt++ {
		for _, si := range ck.serverOrder(coordGID, len(servers)) {
			srv := ck.makeEnd(servers[si])
			var reply TxnCoordAbortReply
			ok := srv.Call("ShardKV.TxnCoordAbort", &args, &reply)
			if ok && (reply.Err == OK || reply.Err == "") {
				ck.rememberLeader(coordGID, si)
				return OK
			}
			ck.forgetLeader(coordGID, si)
		}
		time.Sleep(50 * time.Millisecond)
	}
	return ErrTimeout
}

func (ck *Clerk) txnCoordCommitOnGroup(cfg shardctrler.Config, coordGID int, txnID uint64) Err {
	servers, ok := cfg.Groups[coordGID]
	if !ok || coordGID == 0 || len(servers) == 0 {
		return ErrWrongGroup
	}

	args := TxnCoordCommitArgs{
		TxnID:     txnID,
		ClientID:  ck.ClientID,
		RPCID:     ck.allocRPCID(),
		ConfigNum: cfg.Num,
	}

	for {
		for _, si := range ck.serverOrder(coordGID, len(servers)) {
			srv := ck.makeEnd(servers[si])
			var reply TxnCoordCommitReply
			ok := srv.Call("ShardKV.TxnCoordCommit", &args, &reply)
			if ok && (reply.Err == OK || reply.Err == "") {
				ck.rememberLeader(coordGID, si)
				return OK
			}
			if ok && (reply.Err == ErrWrongGroup || reply.Err == ErrNotReady) {
				ck.forgetLeader(coordGID, si)
				return reply.Err
			}
			if ok && reply.Err == ErrConfigNotReady {
				ck.forgetLeader(coordGID, si)
				break
			}
			ck.forgetLeader(coordGID, si)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) txnCoordFinishOnGroup(cfg shardctrler.Config, coordGID int, txnID uint64) Err {
	servers, ok := cfg.Groups[coordGID]
	if !ok || coordGID == 0 || len(servers) == 0 {
		return ErrWrongGroup
	}

	args := TxnCoordFinishArgs{
		TxnID:     txnID,
		ClientID:  ck.ClientID,
		RPCID:     ck.allocRPCID(),
		ConfigNum: cfg.Num,
	}

	for attempt := 0; attempt < 3; attempt++ {
		for _, si := range ck.serverOrder(coordGID, len(servers)) {
			srv := ck.makeEnd(servers[si])
			var reply TxnCoordFinishReply
			ok := srv.Call("ShardKV.TxnCoordFinish", &args, &reply)
			if ok && (reply.Err == OK || reply.Err == "") {
				ck.rememberLeader(coordGID, si)
				return OK
			}
			ck.forgetLeader(coordGID, si)
		}
		time.Sleep(50 * time.Millisecond)
	}
	return ErrTimeout
}

func (ck *Clerk) txnBranchAbortOnGroup(cfg shardctrler.Config, gid int, txnID uint64, shard int) Err {
	servers, ok := cfg.Groups[gid]
	if !ok || gid == 0 || len(servers) == 0 {
		return ErrWrongGroup
	}

	args := TxnBranchAbortArgs{
		TxnID:     txnID,
		ClientID:  ck.ClientID,
		RPCID:     ck.allocRPCID(),
		ConfigNum: cfg.Num,
		ShardID:   shard,
	}

	for attempt := 0; attempt < 3; attempt++ {
		for _, si := range ck.serverOrder(gid, len(servers)) {
			srv := ck.makeEnd(servers[si])
			var reply TxnBranchAbortReply
			ok := srv.Call("ShardKV.TxnBranchAbort", &args, &reply)
			if ok && (reply.Err == OK || reply.Err == "") {
				ck.rememberLeader(gid, si)
				return OK
			}
			ck.forgetLeader(gid, si)
		}
		time.Sleep(50 * time.Millisecond)
	}
	return ErrTimeout
}

func (ck *Clerk) txnBranchCommitOnGroup(cfg shardctrler.Config, gid int, txnID uint64, shard int) Err {
	servers, ok := cfg.Groups[gid]
	if !ok || gid == 0 || len(servers) == 0 {
		return ErrWrongGroup
	}

	args := TxnBranchCommitArgs{
		TxnID:     txnID,
		ClientID:  ck.ClientID,
		RPCID:     ck.allocRPCID(),
		ConfigNum: cfg.Num,
		ShardID:   shard,
	}

	for {
		for _, si := range ck.serverOrder(gid, len(servers)) {
			srv := ck.makeEnd(servers[si])
			var reply TxnBranchCommitReply
			ok := srv.Call("ShardKV.TxnBranchCommit", &args, &reply)
			if ok && (reply.Err == OK || reply.Err == "") {
				ck.rememberLeader(gid, si)
				return OK
			}
			if ok && (reply.Err == ErrWrongGroup || reply.Err == ErrNotReady) {
				ck.forgetLeader(gid, si)
				return reply.Err
			}
			if ok && reply.Err == ErrConfigNotReady {
				ck.forgetLeader(gid, si)
				break
			}
			ck.forgetLeader(gid, si)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
