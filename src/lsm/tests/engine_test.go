package lsm_test

import (
    "path/filepath"
    "testing"

    "6.5840/lsm"
)

func newTestEngine(t *testing.T) (*lsm.LSMEngine, string) {
    t.Helper()
    dir := t.TempDir()
    return lsm.NewLSMEngine(dir), dir
}

func TestEnginePutGetMemtable(t *testing.T) {
    eng, _ := newTestEngine(t)
    eng.SetMemtableMaxSize(1 << 20)

    eng.Put("k", "v", 0)
    v, _ := eng.Get("k", 0)
    if v != "v" {
        t.Fatalf("get k = %q", v)
    }
}

func TestEngineFlushAndGetFromSST(t *testing.T) {
    eng, dir := newTestEngine(t)
    eng.SetMemtableMaxSize(1)

    eng.Put("k1", "v1", 0)
    eng.Put("k2", "v2", 0) // triggers freeze + flush

    eng.Close()

    eng2 := lsm.NewLSMEngine(dir)
    defer eng2.Close()

    v, _ := eng2.Get("k1", 0)
    if v != "v1" {
        t.Fatalf("get k1 = %q", v)
    }
}

func TestEngineReloadLatestFromL0(t *testing.T) {
    eng, dir := newTestEngine(t)
    eng.SetMemtableMaxSize(1)

    eng.Put("k", "old", 0)
    eng.Put("a", "1", 0) // flush 1

    eng.Put("k", "new", 0)
    eng.Put("b", "1", 0) // flush 2

    eng.Close()

    eng2 := lsm.NewLSMEngine(dir)
    defer eng2.Close()
    v, _ := eng2.Get("k", 0)
    if v != "new" {
        t.Fatalf("expected latest k=new, got %q", v)
    }
}

func TestEngineOpenWithMissingLevels(t *testing.T) {
    dir := t.TempDir()

    // Manually create an SST file at level 2 so level 1 is missing.
    cache := lsm.NewBlockCache(16, 2)
    sb := lsm.NewSSTBuilder(256, nil, cache, false)
    sb.Add("a", "1", 0)
    path := filepath.Join(dir, "sst_00000000000000000000000000000000.2")
    sst := sb.Build(0, path)
    if sst == nil {
        t.Fatalf("build sst failed")
    }
    sst.Close()

    eng := lsm.NewLSMEngine(dir)
    defer eng.Close()
    // Should not panic when level 1 list is missing.
    v, _ := eng.Get("a", 0)
    if v != "1" {
        t.Fatalf("get a = %q", v)
    }
}
