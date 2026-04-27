package shardkv

import (
	"testing"
	"time"
)

func TestCrossShardTxnBeginStoresCoordinator(t *testing.T) {
	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.ctl)
	cfg.join(0)
	cfg.join(1)

	cfgNow := waitConfigWithGroups(t, ck, cfg.groups[0].gid, cfg.groups[1].gid)
	anchorShard := shardOwnedByGID(t, cfgNow, cfg.groups[0].gid)
	tx := ck.BeginCrossShardTxn(RepeatableRead, keyForShard(anchorShard, "coord"))
	if tx.invalid {
		t.Fatalf("cross-shard txn begin failed")
	}
	if tx.txnID == 0 {
		t.Fatalf("cross-shard txn id not assigned")
	}
	if tx.coordGID != cfg.groups[0].gid {
		t.Fatalf("coord gid = %d, want %d", tx.coordGID, cfg.groups[0].gid)
	}

	waitCoordTxnParticipants(t, cfg, tx.coordGID, tx.txnID, 0)
	tx.Abort()
}

func TestCrossShardTxnEnlistsBranchesAcrossGroups(t *testing.T) {
	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.ctl)
	cfg.join(0)
	cfg.join(1)

	cfgNow := waitConfigWithGroups(t, ck, cfg.groups[0].gid, cfg.groups[1].gid)
	shardA := shardOwnedByGID(t, cfgNow, cfg.groups[0].gid)
	shardB := shardOwnedByGID(t, cfgNow, cfg.groups[1].gid)
	keyA := keyForShard(shardA, "a")
	keyB := keyForShard(shardB, "b")

	ck.Put(keyA, "v1")
	ck.Put(keyB, "v2")

	tx := ck.BeginCrossShardTxn(RepeatableRead, keyA)
	if tx.invalid {
		t.Fatalf("cross-shard txn begin failed")
	}

	if v, ok := tx.Get(keyA); !ok || v != "v1" {
		t.Fatalf("cross-shard txn get keyA = (%q,%v), want (v1,true)", v, ok)
	}
	waitCoordTxnParticipants(t, cfg, tx.coordGID, tx.txnID, 1)
	waitBranchTxnPresent(t, cfg, cfg.groups[0].gid, tx.txnID)

	if v, ok := tx.Get(keyB); !ok || v != "v2" {
		t.Fatalf("cross-shard txn get keyB = (%q,%v), want (v2,true)", v, ok)
	}
	waitCoordTxnParticipants(t, cfg, tx.coordGID, tx.txnID, 2)
	waitBranchTxnPresent(t, cfg, cfg.groups[1].gid, tx.txnID)

	if len(tx.branches) != 2 {
		t.Fatalf("cross-shard txn branches = %d, want 2", len(tx.branches))
	}
	tx.Abort()
}

func TestCrossShardTxnAbortReleasesSkeletonState(t *testing.T) {
	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.ctl)
	cfg.join(0)
	cfg.join(1)

	cfgNow := waitConfigWithGroups(t, ck, cfg.groups[0].gid, cfg.groups[1].gid)
	shardA := shardOwnedByGID(t, cfgNow, cfg.groups[0].gid)
	shardB := shardOwnedByGID(t, cfgNow, cfg.groups[1].gid)
	keyA := keyForShard(shardA, "aa")
	keyB := keyForShard(shardB, "bb")

	tx := ck.BeginCrossShardTxn(RepeatableRead, keyA)
	if tx.invalid {
		t.Fatalf("cross-shard txn begin failed")
	}
	if !tx.Put(keyA, "v1") {
		t.Fatalf("cross-shard txn put keyA failed")
	}
	if !tx.Put(keyB, "v2") {
		t.Fatalf("cross-shard txn put keyB failed")
	}

	waitCoordTxnParticipants(t, cfg, tx.coordGID, tx.txnID, 2)
	waitBranchTxnPresent(t, cfg, cfg.groups[0].gid, tx.txnID)
	waitBranchTxnPresent(t, cfg, cfg.groups[1].gid, tx.txnID)

	tx.Abort()

	waitCoordTxnGone(t, cfg, tx.txnID)
	waitBranchTxnGone(t, cfg, tx.txnID)
	waitActiveTxnExactly(t, cfg, 0)
}

func TestCrossShardTxnPrepareLocksWrites(t *testing.T) {
	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.ctl)
	cfg.join(0)
	cfg.join(1)

	cfgNow := waitConfigWithGroups(t, ck, cfg.groups[0].gid, cfg.groups[1].gid)
	shardA := shardOwnedByGID(t, cfgNow, cfg.groups[0].gid)
	shardB := shardOwnedByGID(t, cfgNow, cfg.groups[1].gid)
	keyA := keyForShard(shardA, "pa")
	keyB := keyForShard(shardB, "pb")

	tx := ck.BeginCrossShardTxn(RepeatableRead, keyA)
	if tx.invalid {
		t.Fatalf("cross-shard txn begin failed")
	}
	if !tx.Put(keyA, "v1") || !tx.Put(keyB, "v2") {
		t.Fatalf("cross-shard txn put failed")
	}

	if !tx.prepare() {
		t.Fatalf("cross-shard txn prepare failed")
	}
	waitBranchPrepared(t, cfg, cfg.groups[0].gid, tx.txnID)
	waitBranchPrepared(t, cfg, cfg.groups[1].gid, tx.txnID)
	waitPreparedKeyOwner(t, cfg, keyA, tx.txnID)
	waitPreparedKeyOwner(t, cfg, keyB, tx.txnID)

	tx.Abort()
	waitPreparedKeyGone(t, cfg, keyA)
	waitPreparedKeyGone(t, cfg, keyB)
}

func TestCrossShardTxnPrepareConflictTriggersAbort(t *testing.T) {
	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.ctl)
	cfg.join(0)
	cfg.join(1)

	cfgNow := waitConfigWithGroups(t, ck, cfg.groups[0].gid, cfg.groups[1].gid)
	shardA := shardOwnedByGID(t, cfgNow, cfg.groups[0].gid)
	shardB := shardOwnedByGID(t, cfgNow, cfg.groups[1].gid)
	keyA := keyForShard(shardA, "ca")
	keyB := keyForShard(shardB, "cb")

	tx1 := ck.BeginCrossShardTxn(RepeatableRead, keyA)
	if tx1.invalid {
		t.Fatalf("tx1 begin failed")
	}
	_ = tx1.Put(keyA, "v1")
	_ = tx1.Put(keyB, "v2")
	if !tx1.prepare() {
		t.Fatalf("tx1 prepare failed")
	}
	waitPreparedKeyOwner(t, cfg, keyB, tx1.txnID)

	tx2 := ck.BeginCrossShardTxn(RepeatableRead, keyA)
	if tx2.invalid {
		t.Fatalf("tx2 begin failed")
	}
	_ = tx2.Put(keyB, "vv")
	if tx2.prepare() {
		t.Fatalf("tx2 prepare unexpectedly succeeded")
	}

	waitCoordTxnGone(t, cfg, tx2.txnID)
	waitBranchTxnGone(t, cfg, tx2.txnID)
	waitPreparedKeyOwner(t, cfg, keyB, tx1.txnID)

	tx1.Abort()
	waitPreparedKeyGone(t, cfg, keyB)
}

func TestCrossShardTxnCommitAppliesWrites(t *testing.T) {
	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.ctl)
	cfg.join(0)
	cfg.join(1)

	cfgNow := waitConfigWithGroups(t, ck, cfg.groups[0].gid, cfg.groups[1].gid)
	shardA := shardOwnedByGID(t, cfgNow, cfg.groups[0].gid)
	shardB := shardOwnedByGID(t, cfgNow, cfg.groups[1].gid)
	keyA := keyForShard(shardA, "ma")
	keyB := keyForShard(shardB, "mb")

	tx := ck.BeginCrossShardTxn(RepeatableRead, keyA)
	if tx.invalid {
		t.Fatalf("cross-shard txn begin failed")
	}
	if !tx.Put(keyA, "v1") || !tx.Put(keyB, "v2") {
		t.Fatalf("cross-shard txn put failed")
	}

	if !tx.Commit() {
		t.Fatalf("cross-shard txn commit failed")
	}

	if got := ck.Get(keyA); got != "v1" {
		t.Fatalf("Get(%v) = %v, want v1", keyA, got)
	}
	if got := ck.Get(keyB); got != "v2" {
		t.Fatalf("Get(%v) = %v, want v2", keyB, got)
	}

	waitCoordTxnGone(t, cfg, tx.txnID)
	waitBranchTxnGone(t, cfg, tx.txnID)
	waitPreparedKeyGone(t, cfg, keyA)
	waitPreparedKeyGone(t, cfg, keyB)
	waitActiveTxnExactly(t, cfg, 0)
}

func TestCrossShardTxnPreparedBlocksReconfigUntilAbort(t *testing.T) {
	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.ctl)
	cfg.join(0)
	cfg.join(1)

	cfgNow := waitConfigWithGroups(t, ck, cfg.groups[0].gid, cfg.groups[1].gid)
	shardA := shardOwnedByGID(t, cfgNow, cfg.groups[0].gid)
	shardB := shardOwnedByGID(t, cfgNow, cfg.groups[1].gid)
	keyA := keyForShard(shardA, "ra")
	keyB := keyForShard(shardB, "rb")

	tx := ck.BeginCrossShardTxn(RepeatableRead, keyA)
	if tx.invalid {
		t.Fatalf("cross-shard txn begin failed")
	}
	_ = tx.Put(keyA, "v1")
	_ = tx.Put(keyB, "v2")
	if !tx.prepare() {
		t.Fatalf("cross-shard txn prepare failed")
	}

	oldNum := cfgNow.Num
	cfg.leave(1)
	time.Sleep(500 * time.Millisecond)

	if got := maxObservedConfigNumForGroups(cfg, cfg.groups[0].gid, cfg.groups[1].gid); got != oldNum {
		t.Fatalf("config advanced to %d while prepared txn active, want stay at %d", got, oldNum)
	}

	tx.Abort()
	waitConfigAdvanceForGroups(t, cfg, oldNum+1, cfg.groups[0].gid, cfg.groups[1].gid)
}

func TestCrossShardTxnStateRecoversAfterRestart(t *testing.T) {
	cfg := make_config(t, 3, false, 800)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.ctl)
	cfg.join(0)
	cfg.join(1)

	cfgNow := waitConfigWithGroups(t, ck, cfg.groups[0].gid, cfg.groups[1].gid)
	shardA := shardOwnedByGID(t, cfgNow, cfg.groups[0].gid)
	shardB := shardOwnedByGID(t, cfgNow, cfg.groups[1].gid)
	keyA := keyForShard(shardA, "sa")
	keyB := keyForShard(shardB, "sb")

	tx := ck.BeginCrossShardTxn(RepeatableRead, keyA)
	if tx.invalid {
		t.Fatalf("cross-shard txn begin failed")
	}
	_ = tx.Put(keyA, "v1")
	_ = tx.Put(keyB, "v2")
	if !tx.prepare() {
		t.Fatalf("cross-shard txn prepare failed")
	}

	for i := 0; i < 40; i++ {
		ck.Put(keyForShard(shardA, "filla"+string(rune('a'+(i%26)))), "x")
		ck.Put(keyForShard(shardB, "fillb"+string(rune('a'+(i%26)))), "y")
	}
	waitAnySnapshotPersisted(t, cfg)

	cfg.ShutdownGroup(0)
	cfg.StartGroup(0)
	waitBranchPrepared(t, cfg, cfg.groups[0].gid, tx.txnID)
	waitPreparedKeyOwner(t, cfg, keyA, tx.txnID)

	cfg.ShutdownGroup(1)
	cfg.StartGroup(1)
	waitBranchPrepared(t, cfg, cfg.groups[1].gid, tx.txnID)
	waitPreparedKeyOwner(t, cfg, keyB, tx.txnID)

	tx.Abort()
	waitPreparedKeyGone(t, cfg, keyA)
	waitPreparedKeyGone(t, cfg, keyB)
}

func TestCrossShardTxnPreparedBranchReplaysCommitAfterRestart(t *testing.T) {
	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.ctl)
	cfg.join(0)
	cfg.join(1)

	cfgNow := waitConfigWithGroups(t, ck, cfg.groups[0].gid, cfg.groups[1].gid)
	shardA := shardOwnedByGID(t, cfgNow, cfg.groups[0].gid)
	shardB := shardOwnedByGID(t, cfgNow, cfg.groups[1].gid)
	keyA := keyForShard(shardA, "qa")
	keyB := keyForShard(shardB, "qb")

	tx := ck.BeginCrossShardTxn(RepeatableRead, keyA)
	if tx.invalid {
		t.Fatalf("cross-shard txn begin failed")
	}
	_ = tx.Put(keyA, "v1")
	_ = tx.Put(keyB, "v2")
	if !tx.prepare() {
		t.Fatalf("cross-shard txn prepare failed")
	}

	if err := ck.txnCoordCommitOnGroup(tx.config, tx.coordGID, tx.txnID); err != OK {
		t.Fatalf("coord commit failed: %v", err)
	}

	cfg.ShutdownGroup(1)
	cfg.StartGroup(1)

	waitKeyValue(t, ck, keyA, "v1")
	waitKeyValue(t, ck, keyB, "v2")
	waitBranchTxnGone(t, cfg, tx.txnID)
	waitPreparedKeyGone(t, cfg, keyA)
	waitPreparedKeyGone(t, cfg, keyB)
}

func waitCoordTxnParticipants(t *testing.T, cfg *config, gid int, txnID uint64, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if participantsForCoordTxn(cfg, gid, txnID) == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("coord txn %d on gid %d never reached %d participants, got %d", txnID, gid, want, participantsForCoordTxn(cfg, gid, txnID))
}

func waitBranchTxnPresent(t *testing.T, cfg *config, gid int, txnID uint64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if hasBranchTxn(cfg, gid, txnID) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("branch txn %d never appeared on gid %d", txnID, gid)
}

func waitCoordTxnGone(t *testing.T, cfg *config, txnID uint64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if totalCoordTxn(cfg, txnID) == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("coord txn %d never disappeared, copies=%d", txnID, totalCoordTxn(cfg, txnID))
}

func waitBranchTxnGone(t *testing.T, cfg *config, txnID uint64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if totalBranchTxn(cfg, txnID) == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("branch txn %d never disappeared, copies=%d", txnID, totalBranchTxn(cfg, txnID))
}

func participantsForCoordTxn(cfg *config, gid int, txnID uint64) int {
	for _, group := range cfg.groups {
		if group.gid != gid {
			continue
		}
		for _, server := range group.servers {
			if server == nil {
				continue
			}
			server.mu.Lock()
			rec, ok := server.coordTxns[txnID]
			server.mu.Unlock()
			if ok {
				return len(rec.Participants)
			}
		}
	}
	return -1
}

func hasBranchTxn(cfg *config, gid int, txnID uint64) bool {
	for _, group := range cfg.groups {
		if group.gid != gid {
			continue
		}
		for _, server := range group.servers {
			if server == nil {
				continue
			}
			server.mu.Lock()
			_, ok := server.branchTxns[txnID]
			server.mu.Unlock()
			if ok {
				return true
			}
		}
	}
	return false
}

func totalCoordTxn(cfg *config, txnID uint64) int {
	total := 0
	for _, group := range cfg.groups {
		for _, server := range group.servers {
			if server == nil {
				continue
			}
			server.mu.Lock()
			if _, ok := server.coordTxns[txnID]; ok {
				total++
			}
			server.mu.Unlock()
		}
	}
	return total
}

func totalBranchTxn(cfg *config, txnID uint64) int {
	total := 0
	for _, group := range cfg.groups {
		for _, server := range group.servers {
			if server == nil {
				continue
			}
			server.mu.Lock()
			if _, ok := server.branchTxns[txnID]; ok {
				total++
			}
			server.mu.Unlock()
		}
	}
	return total
}

func waitBranchPrepared(t *testing.T, cfg *config, gid int, txnID uint64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if branchPreparedOnGID(cfg, gid, txnID) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("branch txn %d on gid %d never became prepared", txnID, gid)
}

func waitPreparedKeyOwner(t *testing.T, cfg *config, key string, owner uint64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if preparedKeyOwner(cfg, key) == owner {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("prepared key %q never became owned by txn %d, got %d", key, owner, preparedKeyOwner(cfg, key))
}

func waitPreparedKeyGone(t *testing.T, cfg *config, key string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if preparedKeyOwner(cfg, key) == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("prepared key %q never released, owner=%d", key, preparedKeyOwner(cfg, key))
}

func branchPreparedOnGID(cfg *config, gid int, txnID uint64) bool {
	for _, group := range cfg.groups {
		if group.gid != gid {
			continue
		}
		for _, server := range group.servers {
			if server == nil {
				continue
			}
			server.mu.Lock()
			rec, ok := server.branchTxns[txnID]
			server.mu.Unlock()
			if ok && rec.Prepared {
				return true
			}
		}
	}
	return false
}

func preparedKeyOwner(cfg *config, key string) uint64 {
	shard := key2shard(key)
	gid := 0
	for _, group := range cfg.groups {
		for _, server := range group.servers {
			if server == nil {
				continue
			}
			server.mu.Lock()
			if server.config.Shards[shard] == server.gid {
				if owner, ok := server.preparedKeys[key]; ok {
					server.mu.Unlock()
					return owner
				}
				gid = server.gid
			}
			server.mu.Unlock()
		}
	}
	_ = gid
	return 0
}

func maxObservedConfigNum(cfg *config) int {
	maxNum := 0
	for _, group := range cfg.groups {
		for _, server := range group.servers {
			if server == nil {
				continue
			}
			server.mu.Lock()
			if server.config.Num > maxNum {
				maxNum = server.config.Num
			}
			server.mu.Unlock()
		}
	}
	return maxNum
}

func maxObservedConfigNumForGroups(cfg *config, gids ...int) int {
	if len(gids) == 0 {
		return maxObservedConfigNum(cfg)
	}
	allow := make(map[int]struct{}, len(gids))
	for _, gid := range gids {
		allow[gid] = struct{}{}
	}
	maxNum := 0
	for _, group := range cfg.groups {
		if _, ok := allow[group.gid]; !ok {
			continue
		}
		for _, server := range group.servers {
			if server == nil {
				continue
			}
			server.mu.Lock()
			if server.config.Num > maxNum {
				maxNum = server.config.Num
			}
			server.mu.Unlock()
		}
	}
	return maxNum
}

func waitConfigAdvance(t *testing.T, cfg *config, wantAtLeast int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if maxObservedConfigNum(cfg) >= wantAtLeast {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("config never advanced to %d, got %d", wantAtLeast, maxObservedConfigNum(cfg))
}

func waitConfigAdvanceForGroups(t *testing.T, cfg *config, wantAtLeast int, gids ...int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if maxObservedConfigNumForGroups(cfg, gids...) >= wantAtLeast {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("config never advanced to %d on target groups, got %d", wantAtLeast, maxObservedConfigNumForGroups(cfg, gids...))
}

func waitAnySnapshotPersisted(t *testing.T, cfg *config) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, group := range cfg.groups {
			for _, saved := range group.saved {
				if saved != nil && len(saved.ReadSnapshot()) > 0 {
					return
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("no snapshot persisted before timeout")
}

func waitKeyValue(t *testing.T, ck *Clerk, key, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if got := ck.Get(key); got == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("key %q never became %q", key, want)
}
