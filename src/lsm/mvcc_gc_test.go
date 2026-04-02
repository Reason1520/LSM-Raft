package lsm

import (
	"fmt"
	"reflect"
	"testing"
)

func collectVersionsByKey(engine *LSMEngine, key string) []uint64 {
	engine.SSTsMutex.RLock()
	iters := make([]Iterator, 0, len(engine.SSTs))
	for _, sst := range engine.SSTs {
		it := NewSSTIterator(sst, 0)
		if it.Valid() {
			iters = append(iters, it)
		} else {
			_ = it.Close()
		}
	}
	engine.SSTsMutex.RUnlock()

	if len(iters) == 0 {
		return nil
	}

	merge := NewVersionedHeapIterator(iters, 0)
	defer merge.Close()

	var out []uint64
	for merge.Valid() {
		if merge.Key() == key {
			out = append(out, merge.TrancID())
		}
		merge.Next()
	}
	return out
}

func TestMVCCGC(t *testing.T) {
	dir := t.TempDir()
	engine := NewLSMEngine(dir)
	defer engine.Close()

	for i := 1; i <= 5; i++ {
		engine.Put("k", fmt.Sprintf("v%d", i), uint64(i))
	}
	engine.Flush(true)

	engine.SetGCWatermark(4)
	engine.FullCompact(0)

	got := collectVersionsByKey(engine, "k")
	want := []uint64{5, 4, 3}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("gc versions mismatch: got %v want %v", got, want)
	}
}
