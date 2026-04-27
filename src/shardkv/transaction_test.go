package shardkv

import (
	"testing"
	"time"

	"6.5840/shardctrler"
)

func TestTxnSingleShardCommit(t *testing.T) {
	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.ctl)
	cfg.join(0)

	tx := ck.BeginTxn(RepeatableRead)
	if ok := tx.Put("a1", "v1"); !ok {
		t.Fatalf("txn put failed")
	}
	if ok := tx.Put("a2", "v2"); !ok {
		t.Fatalf("txn put failed")
	}
	if !tx.Commit() {
		t.Fatalf("txn commit failed")
	}

	if got := ck.Get("a1"); got != "v1" {
		t.Fatalf("Get(a1) expected v1, got %v", got)
	}
	if got := ck.Get("a2"); got != "v2" {
		t.Fatalf("Get(a2) expected v2, got %v", got)
	}
}

func TestTxnRepeatableReadConflict(t *testing.T) {
	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.ctl)
	cfg.join(0)

	ck.Put("a1", "v0")

	tx1 := ck.BeginTxn(RepeatableRead)
	if v, ok := tx1.Get("a1"); !ok || v != "v0" {
		t.Fatalf("txn get expected v0, got %v ok=%v", v, ok)
	}

	tx2 := ck.BeginTxn(RepeatableRead)
	if ok := tx2.Put("a1", "v1"); !ok {
		t.Fatalf("txn2 put failed")
	}
	if !tx2.Commit() {
		t.Fatalf("txn2 commit failed")
	}

	if tx1.Commit() {
		t.Fatalf("txn1 commit should conflict but succeeded")
	}
}

func TestTxnReadCommittedNoConflict(t *testing.T) {
	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.ctl)
	cfg.join(0)

	ck.Put("a1", "v0")

	tx1 := ck.BeginTxn(ReadCommitted)
	if v, ok := tx1.Get("a1"); !ok || v != "v0" {
		t.Fatalf("txn get expected v0, got %v ok=%v", v, ok)
	}

	tx2 := ck.BeginTxn(ReadCommitted)
	if ok := tx2.Put("a1", "v1"); !ok {
		t.Fatalf("txn2 put failed")
	}
	if !tx2.Commit() {
		t.Fatalf("txn2 commit failed")
	}

	if !tx1.Commit() {
		t.Fatalf("txn1 commit should succeed under ReadCommitted")
	}
}

func TestTxnSerializableConflict(t *testing.T) {
	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.ctl)
	cfg.join(0)

	ck.Put("a1", "v0")

	tx1 := ck.BeginTxn(Serializable)
	if v, ok := tx1.Get("a1"); !ok || v != "v0" {
		t.Fatalf("txn get expected v0, got %v ok=%v", v, ok)
	}

	tx2 := ck.BeginTxn(Serializable)
	if ok := tx2.Put("a1", "v1"); !ok {
		t.Fatalf("txn2 put failed")
	}
	if !tx2.Commit() {
		t.Fatalf("txn2 commit failed")
	}

	if tx1.Commit() {
		t.Fatalf("txn1 commit should conflict under Serializable")
	}
}

func TestTxnCrossShardRejected(t *testing.T) {
	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.ctl)
	cfg.join(0)

	tx := ck.BeginTxn(RepeatableRead)
	if ok := tx.Put("a1", "v1"); !ok {
		t.Fatalf("txn put failed")
	}
	// "a1" and "b1" are different shards.
	if ok := tx.Put("b1", "v2"); ok {
		t.Fatalf("cross-shard put should fail")
	}
	if tx.Commit() {
		t.Fatalf("cross-shard transaction should not commit")
	}
}

func TestTxnRangeSnapshot(t *testing.T) {
	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.ctl)
	cfg.join(0)

	ck.Put("a1", "v1")
	ck.Put("a2", "v2")

	tx := ck.BeginTxn(RepeatableRead)
	if v, ok := tx.Get("a1"); !ok || v != "v1" {
		t.Fatalf("txn get expected v1, got %v ok=%v", v, ok)
	}

	ck.Put("a2", "v2-new")

	got, ok := tx.Range("a", "a~", 0)
	if !ok {
		t.Fatalf("txn range failed")
	}
	if len(got) != 2 || got[0] != (KeyValue{Key: "a1", Value: "v1"}) || got[1] != (KeyValue{Key: "a2", Value: "v2"}) {
		t.Fatalf("txn range snapshot got %v", got)
	}
}

func TestTxnRangeOverlayWrites(t *testing.T) {
	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.ctl)
	cfg.join(0)

	ck.Put("a1", "v1")
	ck.Put("a2", "v2")
	ck.Put("a3", "v3")

	tx := ck.BeginTxn(RepeatableRead)
	if !tx.Put("a2", "v2x") {
		t.Fatalf("txn put failed")
	}
	if !tx.Remove("a3") {
		t.Fatalf("txn remove failed")
	}

	got, ok := tx.Range("a", "a~", 0)
	if !ok {
		t.Fatalf("txn range failed")
	}
	expect := []KeyValue{
		{Key: "a1", Value: "v1"},
		{Key: "a2", Value: "v2x"},
	}
	if len(got) != len(expect) || got[0] != expect[0] || got[1] != expect[1] {
		t.Fatalf("txn range overlay got %v expect %v", got, expect)
	}
}

func TestTxnRangeCrossShardRejected(t *testing.T) {
	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.ctl)
	cfg.join(0)

	tx := ck.BeginTxn(RepeatableRead)
	if _, ok := tx.Range("a", "b", 0); ok {
		t.Fatalf("cross-shard range should fail")
	}
	if tx.Commit() {
		t.Fatalf("cross-shard txn should not commit")
	}
}

func TestReadTxnCrossShardSnapshot(t *testing.T) {
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

	ck.Put(keyA, "v1")
	ck.Put(keyB, "w1")

	tx := ck.BeginReadTxn(RepeatableRead)

	ck.Put(keyA, "v2")
	ck.Put(keyB, "w2")

	if got, ok := tx.Get(keyA); !ok || got != "v1" {
		t.Fatalf("read txn get(%q) = %q ok=%v, want v1 true", keyA, got, ok)
	}
	if got, ok := tx.Get(keyB); !ok || got != "w1" {
		t.Fatalf("read txn get(%q) = %q ok=%v, want w1 true", keyB, got, ok)
	}
	if !tx.Commit() {
		t.Fatalf("read txn commit failed")
	}
}

func TestReadTxnRangeSnapshotAcrossShards(t *testing.T) {
	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.ctl)
	cfg.join(0)
	cfg.join(1)

	cfgNow := waitConfigWithGroups(t, ck, cfg.groups[0].gid, cfg.groups[1].gid)
	shardA := shardOwnedByGID(t, cfgNow, cfg.groups[0].gid)
	shardB := shardOwnedByGID(t, cfgNow, cfg.groups[1].gid)

	keyA := keyForShard(shardA, "r1")
	keyB := keyForShard(shardB, "r2")
	keyC := keyForShard((shardA+1)%10, "r3")

	ck.Put(keyA, "v1")
	ck.Put(keyB, "v2")
	ck.Put(keyC, "v3")

	tx := ck.BeginReadTxn(RepeatableRead)

	ck.Put(keyA, "v1-new")
	ck.Put(keyB, "v2-new")
	ck.Put(keyForShard((shardB+1)%10, "later"), "v4")

	got, ok := tx.Range("", "", 0)
	if !ok {
		t.Fatalf("read txn range failed")
	}

	expect := map[string]string{
		keyA: "v1",
		keyB: "v2",
		keyC: "v3",
	}
	if len(got) != len(expect) {
		t.Fatalf("read txn range got %v entries, want %v: %v", len(got), len(expect), got)
	}
	for _, kv := range got {
		if v, ok := expect[kv.Key]; !ok || v != kv.Value {
			t.Fatalf("unexpected kv %v in read txn range, expect %v", kv, expect)
		}
		delete(expect, kv.Key)
	}
	if len(expect) != 0 {
		t.Fatalf("missing keys from read txn range: %v", expect)
	}
}

func TestTxnAbortReleasesActiveTxn(t *testing.T) {
	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.ctl)
	cfg.join(0)

	tx := ck.BeginTxn(RepeatableRead)
	if !tx.Put("a1", "v1") {
		t.Fatalf("txn put failed")
	}
	waitActiveTxnAtLeast(t, cfg, 1)

	tx.Abort()
	waitActiveTxnExactly(t, cfg, 0)
}

func TestReadTxnAbortReleasesActiveTxns(t *testing.T) {
	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.ctl)
	cfg.join(0)
	cfg.join(1)

	cfgNow := waitConfigWithGroups(t, ck, cfg.groups[0].gid, cfg.groups[1].gid)
	shardA := shardOwnedByGID(t, cfgNow, cfg.groups[0].gid)
	shardB := shardOwnedByGID(t, cfgNow, cfg.groups[1].gid)

	ck.Put(keyForShard(shardA, "ta"), "v1")
	ck.Put(keyForShard(shardB, "tb"), "v2")

	tx := ck.BeginReadTxn(RepeatableRead)
	waitActiveTxnAtLeast(t, cfg, 2)

	tx.Abort()
	waitActiveTxnExactly(t, cfg, 0)
}

func TestReadTxnCommitReleasesActiveTxns(t *testing.T) {
	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.ctl)
	cfg.join(0)
	cfg.join(1)

	cfgNow := waitConfigWithGroups(t, ck, cfg.groups[0].gid, cfg.groups[1].gid)
	shardA := shardOwnedByGID(t, cfgNow, cfg.groups[0].gid)
	shardB := shardOwnedByGID(t, cfgNow, cfg.groups[1].gid)

	ck.Put(keyForShard(shardA, "ca"), "v1")
	ck.Put(keyForShard(shardB, "cb"), "v2")

	tx := ck.BeginReadTxn(RepeatableRead)
	waitActiveTxnAtLeast(t, cfg, 2)

	if !tx.Commit() {
		t.Fatalf("read txn commit failed")
	}
	waitActiveTxnExactly(t, cfg, 0)
}

func waitConfigWithGroups(t *testing.T, ck *Clerk, gids ...int) shardctrler.Config {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ck.refreshConfig()
		cfg := ck.currentConfig()
		have := make(map[int]bool)
		for _, gid := range cfg.Shards {
			have[gid] = true
		}
		ok := true
		for _, gid := range gids {
			if !have[gid] {
				ok = false
				break
			}
		}
		if ok {
			return cfg
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for config with gids %v", gids)
	return shardctrler.Config{}
}

func shardOwnedByGID(t *testing.T, cfg shardctrler.Config, gid int) int {
	for shard, owner := range cfg.Shards {
		if owner == gid {
			return shard
		}
	}
	t.Fatalf("no shard owned by gid %d in config %+v", gid, cfg.Shards)
	return -1
}

func keyForShard(shard int, suffix string) string {
	for b := byte('!'); b <= byte('~'); b++ {
		if int(b)%shardctrler.NShards == shard {
			return string([]byte{b}) + suffix
		}
	}
	panic("no printable byte for shard")
}

func waitActiveTxnAtLeast(t *testing.T, cfg *config, want int) {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if countActiveTxn(cfg) >= want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("active txn count never reached %d, got %d", want, countActiveTxn(cfg))
}

func waitActiveTxnExactly(t *testing.T, cfg *config, want int) {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if countActiveTxn(cfg) == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("active txn count never became %d, got %d", want, countActiveTxn(cfg))
}

func countActiveTxn(cfg *config) int {
	total := 0
	for _, group := range cfg.groups {
		for _, server := range group.servers {
			if server == nil {
				continue
			}
			server.mu.Lock()
			total += len(server.activeTxn)
			server.mu.Unlock()
		}
	}
	return total
}
