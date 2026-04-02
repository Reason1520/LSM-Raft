package shardkv

import "testing"

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
