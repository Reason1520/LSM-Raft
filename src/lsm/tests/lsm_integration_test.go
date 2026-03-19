package lsm_test

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"

	"6.5840/lsm"
)

func predRange(start, end string) func(string) int {
	return func(s string) int {
		if s < start {
			return 1
		}
		if s >= end {
			return -1
		}
		return 0
	}
}

func collectRange(it lsm.Iterator, pred func(string) int) []string {
	out := make([]string, 0)
	for it.Valid() {
		k := it.Key()
		if pred(k) != 0 {
			break
		}
		out = append(out, k)
		it.Next()
	}
	return out
}

type kvPair struct {
	key   string
	value string
}

func collectRangeKV(it lsm.Iterator, pred func(string) int) []kvPair {
	out := make([]kvPair, 0)
	for it.Valid() {
		k := it.Key()
		if pred(k) != 0 {
			break
		}
		out = append(out, kvPair{key: k, value: it.Value()})
		it.Next()
	}
	return out
}

func TestEngineRangePredicateOnReopen(t *testing.T) {
	eng, dir := newTestEngine(t)
	eng.SetMemtableMaxSize(16)

	for _, k := range []string{"a", "b", "c", "d", "e", "f"} {
		eng.Put(k, k, 0)
	}
	eng.Close()

	eng2 := lsm.NewLSMEngine(dir)
	defer eng2.Close()

	pred := predRange("b", "e")
	start, end, ok := eng2.LsmItersMonotonyPredicate(0, pred)
	if !ok || start == nil {
		t.Fatal("LsmItersMonotonyPredicate failed")
	}
	defer start.Close()
	if end != nil {
		defer end.Close()
	}

	got := collectRange(start, pred)
	if len(got) != 3 || got[0] != "b" || got[1] != "c" || got[2] != "d" {
		t.Fatalf("got %v", got)
	}
}

func TestEngineDeleteTombstoneAfterFlush(t *testing.T) {
	eng, dir := newTestEngine(t)
	eng.SetMemtableMaxSize(1)

	eng.Put("a", "1", 0)
	eng.Put("pad1", "x", 0) // flush
	eng.Put("b", "2", 0)
	eng.Put("pad2", "y", 0) // flush
	eng.Put("c", "3", 0)
	eng.Put("pad3", "z", 0) // flush

	eng.Remove("b", 0)
	eng.Put("pad4", "w", 0) // flush tombstone

	eng.Close()

	eng2 := lsm.NewLSMEngine(dir)
	defer eng2.Close()

	v, _ := eng2.Get("b", 0)
	if v != "" {
		t.Fatalf("expected b deleted, got %q", v)
	}
}

func TestEngineCompactionKeepsKeys(t *testing.T) {
	eng, dir := newTestEngine(t)
	eng.SetMemtableMaxSize(1)

	keys := make([]string, 0)
	for i := 0; i < 8; i++ {
		k := fmt.Sprintf("k%02d", i)
		keys = append(keys, k)
		eng.Put(k, k, 0)
	}

	eng.Close()

	eng2 := lsm.NewLSMEngine(dir)
	defer eng2.Close()

	for _, k := range keys {
		v, _ := eng2.Get(k, 0)
		if v != k {
			t.Fatalf("get %s = %q", k, v)
		}
	}
}

func BenchmarkEnginePut(b *testing.B) {
	eng := lsm.NewLSMEngine(b.TempDir())
	eng.SetMemtableMaxSize(1 << 30)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		eng.Put(strconv.Itoa(i), "v", 0)
	}
	b.StopTimer()
	eng.Close()
}

func BenchmarkEngineGet(b *testing.B) {
	eng := lsm.NewLSMEngine(b.TempDir())
	eng.SetMemtableMaxSize(1 << 30)

	const nkeys = 100000
	keys := make([]string, nkeys)
	for i := 0; i < nkeys; i++ {
		keys[i] = strconv.Itoa(i)
		eng.Put(keys[i], "v", 0)
	}

	rng := rand.New(rand.NewSource(1))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		k := keys[rng.Intn(nkeys)]
		_, _ = eng.Get(k, 0)
	}
	b.StopTimer()
	eng.Close()
}

func TestRangeMVCCVisibleVersions(t *testing.T) {
	eng, _ := newTestEngine(t)
	eng.SetMemtableMaxSize(1 << 20)

	eng.Put("a", "v10", 10)
	eng.Put("a", "v20", 20)
	eng.Put("b", "v15", 15)
	eng.Put("c", "v05", 5)
	eng.Remove("c", 25)

	pred := predRange("a", "z")

	start, end, ok := eng.LsmItersMonotonyPredicate(18, pred)
	if !ok || start == nil {
		t.Fatal("LsmItersMonotonyPredicate failed")
	}
	defer start.Close()
	if end != nil {
		defer end.Close()
	}

	got := collectRangeKV(start, pred)
	if len(got) != 3 ||
		got[0] != (kvPair{key: "a", value: "v10"}) ||
		got[1] != (kvPair{key: "b", value: "v15"}) ||
		got[2] != (kvPair{key: "c", value: "v05"}) {
		t.Fatalf("snapshot 18 got %v", got)
	}

	start2, end2, ok := eng.LsmItersMonotonyPredicate(30, pred)
	if !ok || start2 == nil {
		t.Fatal("LsmItersMonotonyPredicate failed")
	}
	defer start2.Close()
	if end2 != nil {
		defer end2.Close()
	}

	got2 := collectRangeKV(start2, pred)
	if len(got2) != 2 ||
		got2[0] != (kvPair{key: "a", value: "v20"}) ||
		got2[1] != (kvPair{key: "b", value: "v15"}) {
		t.Fatalf("snapshot 30 got %v", got2)
	}
}
