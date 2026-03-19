package lsm_test

import (
    "os"
    "path/filepath"
    "testing"

    "6.5840/lsm"
)

func buildTestSST(t *testing.T) (*lsm.SSTable, string) {
    t.Helper()
    cache := lsm.NewBlockCache(16, 2)
    sb := lsm.NewSSTBuilder(256, nil, cache, false)
    sb.Add("a", "1", 0)
    sb.Add("b", "2", 0)
    sb.Add("c", "3", 0)

    dir := t.TempDir()
    path := filepath.Join(dir, "test.sst")
    sst := sb.Build(1, path)
    if sst == nil {
        t.Fatalf("build sst failed")
    }
    return sst, path
}

func TestSSTBuildOpenGet(t *testing.T) {
    sst, path := buildTestSST(t)
    defer sst.Close()

    it := sst.Get("b", 0)
    if !it.Valid() || it.Key() != "b" || it.Value() != "2" {
        t.Fatalf("get b = %q=%q", it.Key(), it.Value())
    }

    f, err := os.Open(path)
    if err != nil {
        t.Fatal(err)
    }
    defer f.Close()

    cache := lsm.NewBlockCache(16, 2)
    sst2 := lsm.OpenSST(2, f, cache, 256)
    if sst2 == nil {
        t.Fatalf("OpenSST failed")
    }
    it2 := sst2.Get("c", 0)
    if !it2.Valid() || it2.Key() != "c" || it2.Value() != "3" {
        t.Fatalf("get c = %q=%q", it2.Key(), it2.Value())
    }
}

func TestSSTIteratorSeekAndNext(t *testing.T) {
    sst, _ := buildTestSST(t)
    defer sst.Close()

    it := lsm.NewSSTIterator(sst, 0)
    it.Seek("b")
    if !it.Valid() || it.Key() != "b" {
        t.Fatalf("seek b got %q", it.Key())
    }
    it.Next()
    if it.Valid() && it.Key() != "c" {
        t.Fatalf("next got %q", it.Key())
    }
}

func TestSSTItersMonotonyPredicate(t *testing.T) {
    sst, _ := buildTestSST(t)
    defer sst.Close()

    pred := func(s string) int {
        if s < "b" {
            return 1
        }
        if s > "c" {
            return -1
        }
        return 0
    }
    left, _, ok := sst.ItersMonotonyPredicate(0, pred)
    if !ok || left == nil {
        t.Fatal("ItersMonotonyPredicate failed")
    }
    got := make([]string, 0)
    for left.Valid() {
        k := left.Key()
        r := pred(k)
        if r < 0 {
            break
        }
        if r == 0 {
            got = append(got, k)
        }
        left.Next()
    }
    if len(got) != 2 || got[0] != "b" || got[1] != "c" {
        t.Fatalf("got %v", got)
    }
}
