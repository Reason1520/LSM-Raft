package lsm_test

import (
	"testing"

	"6.5840/lsm"
)

func newTestEngineLocal(t *testing.T) (*lsm.LSMEngine, string) {
	t.Helper()
	dir := t.TempDir()
	return lsm.NewLSMEngine(dir), dir
}

func TestEngineMVCCGet(t *testing.T) {
	eng, _ := newTestEngineLocal(t)
	eng.SetMemtableMaxSize(1 << 20)

	eng.Put("k", "v1", 1)
	eng.Put("k", "v2", 2)

	v, tid := eng.Get("k", 1)
	if v != "v1" || tid != 1 {
		t.Fatalf("snapshot 1 got %q tid=%d", v, tid)
	}

	v, tid = eng.Get("k", 2)
	if v != "v2" || tid != 2 {
		t.Fatalf("snapshot 2 got %q tid=%d", v, tid)
	}

	v, tid = eng.Get("k", 0)
	if v != "v2" || tid != 2 {
		t.Fatalf("latest got %q tid=%d", v, tid)
	}

	eng.Remove("k", 3)

	v, tid = eng.Get("k", 2)
	if v != "v2" || tid != 2 {
		t.Fatalf("snapshot 2 after delete got %q tid=%d", v, tid)
	}

	v, tid = eng.Get("k", 3)
	if v != "" || tid != 3 {
		t.Fatalf("snapshot 3 got %q tid=%d", v, tid)
	}

	v, tid = eng.Get("k", 4)
	if v != "" || tid != 3 {
		t.Fatalf("snapshot 4 got %q tid=%d", v, tid)
	}
}

func TestEngineMVCCAcrossFlush(t *testing.T) {
	eng, dir := newTestEngineLocal(t)
	eng.SetMemtableMaxSize(1 << 20)

	eng.Put("k", "v1", 1)
	eng.Put("k", "v2", 2)
	eng.Remove("k", 3)

	v, tid := eng.Get("k", 1)
	if v != "v1" || tid != 1 {
		t.Fatalf("snapshot 1 got %q tid=%d", v, tid)
	}

	v, tid = eng.Get("k", 2)
	if v != "v2" || tid != 2 {
		t.Fatalf("snapshot 2 got %q tid=%d", v, tid)
	}

	v, tid = eng.Get("k", 3)
	if v != "" || tid != 3 {
		t.Fatalf("snapshot 3 got %q tid=%d", v, tid)
	}

	eng.Close()

	eng2 := lsm.NewLSMEngine(dir)
	defer eng2.Close()

	v, tid = eng2.Get("k", 1)
	if v != "v1" || tid != 1 {
		t.Fatalf("reopen snapshot 1 got %q tid=%d", v, tid)
	}
	v, tid = eng2.Get("k", 2)
	if v != "v2" || tid != 2 {
		t.Fatalf("reopen snapshot 2 got %q tid=%d", v, tid)
	}
	v, tid = eng2.Get("k", 3)
	if v != "" || tid != 3 {
		t.Fatalf("reopen snapshot 3 got %q tid=%d", v, tid)
	}
}

func TestEngineBatchOps(t *testing.T) {
	eng, _ := newTestEngineLocal(t)
	eng.SetMemtableMaxSize(1 << 20)

	kvs := []lsm.KV{
		{Key: "a", Value: "1"},
		{Key: "b", Value: "2"},
	}
	eng.PutBatch(kvs, 10)

	res := eng.GetBatch([]string{"a", "b", "c"}, 0)
	if res["a"].Value() != "1" || res["a"].TrancID() != 10 {
		t.Fatalf("getbatch a = %q tid=%d", res["a"].Value(), res["a"].TrancID())
	}
	if res["b"].Value() != "2" || res["b"].TrancID() != 10 {
		t.Fatalf("getbatch b = %q tid=%d", res["b"].Value(), res["b"].TrancID())
	}
	if res["c"].Value() != "" || res["c"].TrancID() != 0 {
		t.Fatalf("getbatch c = %q tid=%d", res["c"].Value(), res["c"].TrancID())
	}

	eng.RemoveBatch([]string{"a", "b"}, 11)
	v, tid := eng.Get("a", 12)
	if v != "" || tid != 11 {
		t.Fatalf("after remove batch got %q tid=%d", v, tid)
	}
}
