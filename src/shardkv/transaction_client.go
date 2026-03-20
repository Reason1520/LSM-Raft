package shardkv

import "time"

// Txn is a client-side single-shard transaction.
type Txn struct {
	ck        *Clerk
	txnID     uint64
	isolation IsolationLevel
	snapshot  uint64
	shard     int
	started   bool
	invalid   bool

	writes map[string]*string
	reads  map[string]uint64
}

// BeginTxn starts a new transaction (lazy begin on first key access).
func (ck *Clerk) BeginTxn(level IsolationLevel) *Txn {
	return &Txn{
		ck:        ck,
		isolation: level,
		shard:     -1,
		writes:    make(map[string]*string),
		reads:     make(map[string]uint64),
	}
}

func (tx *Txn) ensureBegin(key string) bool {
	shard := key2shard(key)
	if tx.shard != -1 && tx.shard != shard {
		tx.invalid = true
		return false
	}
	if tx.started {
		return true
	}
	txnID, snapshot := tx.ck.txnBeginOnShard(shard, tx.isolation)
	tx.shard = shard
	tx.txnID = txnID
	tx.snapshot = snapshot
	tx.started = true
	return true
}

// Get reads within the transaction.
func (tx *Txn) Get(key string) (string, bool) {
	if tx.invalid {
		return "", false
	}
	if !tx.ensureBegin(key) {
		return "", false
	}
	if v, ok := tx.writes[key]; ok {
		if v == nil {
			return "", false
		}
		return *v, true
	}
	val, ver, err := tx.ck.txnGetOnShard(tx.shard, key, tx.snapshot)
	if err == OK {
		tx.reads[key] = ver
		return val, true
	}
	if err == ErrNoKey {
		tx.reads[key] = 0
	}
	return "", false
}

// Put buffers a write.
func (tx *Txn) Put(key, value string) bool {
	if tx.invalid {
		return false
	}
	if !tx.ensureBegin(key) {
		return false
	}
	v := value
	tx.writes[key] = &v
	return true
}

// Remove buffers a delete.
func (tx *Txn) Remove(key string) bool {
	if tx.invalid {
		return false
	}
	if !tx.ensureBegin(key) {
		return false
	}
	tx.writes[key] = nil
	return true
}

// Commit submits the transaction as a single Raft log entry.
func (tx *Txn) Commit() bool {
	if tx.invalid {
		return false
	}
	if !tx.started && len(tx.writes) == 0 && len(tx.reads) == 0 {
		return true
	}
	if !tx.started {
		return false
	}
	writes := make([]TxnWrite, 0, len(tx.writes))
	for k, v := range tx.writes {
		if v == nil {
			writes = append(writes, TxnWrite{Key: k, Delete: true})
		} else {
			writes = append(writes, TxnWrite{Key: k, Value: *v})
		}
	}
	reads := make([]TxnRead, 0, len(tx.reads))
	for k, v := range tx.reads {
		reads = append(reads, TxnRead{Key: k, Version: v})
	}

	args := TxnCommitArgs{
		TxnID:     tx.txnID,
		ClientID:  tx.ck.ClientID,
		RPCID:     tx.ck.allocRPCID(),
		Isolation: tx.isolation,
		Writes:    writes,
		Reads:     reads,
	}

	err := tx.ck.txnCommitOnShard(tx.shard, &args)
	if err == OK {
		return true
	}
	if err == ErrConflict {
		return false
	}
	return false
}

// Abort discards buffered operations.
func (tx *Txn) Abort() {
	tx.writes = make(map[string]*string)
	tx.reads = make(map[string]uint64)
}

func (ck *Clerk) txnBeginOnShard(shard int, level IsolationLevel) (uint64, uint64) {
	args := TxnBeginArgs{
		ClientID:  ck.ClientID,
		RPCID:     ck.allocRPCID(),
		Isolation: level,
	}
	for {
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.makeEnd(servers[si])
				var reply TxnBeginReply
				if srv.Call("ShardKV.TxnBegin", &args, &reply) && reply.Err == OK {
					return reply.TxnID, reply.Snapshot
				}
				if reply.Err == ErrWrongGroup {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) txnGetOnShard(shard int, key string, snapshot uint64) (string, uint64, Err) {
	args := TxnGetArgs{
		Key:      key,
		Snapshot: snapshot,
		ClientID: ck.ClientID,
		RPCID:    ck.allocRPCID(),
	}
	for {
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.makeEnd(servers[si])
				var reply TxnGetReply
				ok := srv.Call("ShardKV.TxnGet", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value, reply.Version, reply.Err
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) txnCommitOnShard(shard int, args *TxnCommitArgs) Err {
	for {
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.makeEnd(servers[si])
				var reply TxnCommitReply
				ok := srv.Call("ShardKV.TxnCommit", args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrConflict) {
					return reply.Err
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		ck.config = ck.sm.Query(-1)
	}
}
