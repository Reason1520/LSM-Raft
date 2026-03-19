package lsm_test

import (
    "testing"

    "6.5840/lsm"
)

func TestHeapIteratorDedupOrder(t *testing.T) {
    slNew := lsm.NewSkipList(16)
    slOld := lsm.NewSkipList(16)

    slOld.Put("a", "oldA", 0)
    slOld.Put("b", "oldB", 0)

    slNew.Put("a", "newA", 0)
    slNew.Put("c", "newC", 0)

    itNew := slNew.NewIterator(nil, 0)
    itNew.Seek("")
    itOld := slOld.NewIterator(nil, 0)
    itOld.Seek("")

    hi := lsm.NewHeapIterator([]lsm.Iterator{itNew, itOld}, true, 0)
    gotKeys := make([]string, 0)
    gotVals := make([]string, 0)
    for hi.Valid() {
        gotKeys = append(gotKeys, hi.Key())
        gotVals = append(gotVals, hi.Value())
        hi.Next()
    }
    if len(gotKeys) != 3 {
        t.Fatalf("keys=%v vals=%v", gotKeys, gotVals)
    }
    if gotKeys[0] != "a" || gotVals[0] != "newA" {
        t.Fatalf("expected a=newA, got %q=%q", gotKeys[0], gotVals[0])
    }
    if gotKeys[1] != "b" || gotVals[1] != "oldB" {
        t.Fatalf("expected b=oldB, got %q=%q", gotKeys[1], gotVals[1])
    }
    if gotKeys[2] != "c" || gotVals[2] != "newC" {
        t.Fatalf("expected c=newC, got %q=%q", gotKeys[2], gotVals[2])
    }
}

func TestMemtableGetAcrossFrozen(t *testing.T) {
    mt := lsm.NewMemtable()
    mt.SetMaxTableSize(8)

    mt.Put("a", "1111", 0) // sizeByte=5
    mt.Put("b", "2222", 0) // sizeByte=10 (>8), freeze on next put
    mt.Put("c", "3333", 0) // triggers freeze before insert

    it := mt.Get("a", 0)
    if !it.Valid() || it.Key() != "a" {
        t.Fatalf("get a failed")
    }
    it = mt.Get("c", 0)
    if !it.Valid() || it.Key() != "c" {
        t.Fatalf("get c failed")
    }
}

func TestMemtableIteratorMergeLatest(t *testing.T) {
    mt := lsm.NewMemtable()
    mt.SetMaxTableSize(8)

    // Insert enough to overflow, then update k in the new table.
    mt.Put("k", "old", 0)   // size=4
    mt.Put("x", "1234", 0)  // size=9 (>8), freeze on next put
    mt.Put("k", "new", 0)   // triggers freeze and writes to new table

    it := mt.NewMemtableIterator(true, 0)
    gotK := ""
    for it.Valid() {
        if it.Key() == "k" {
            gotK = it.Value()
            break
        }
        it.Next()
    }
    if gotK != "new" {
        t.Fatalf("expected k=new, got %q", gotK)
    }
}

func TestMemtableItersMonotonyPredicate(t *testing.T) {
    mt := lsm.NewMemtable()
    for _, k := range []string{"a", "b", "c", "d"} {
        mt.Put(k, k, 0)
    }
    pred := func(s string) int {
        if s < "b" {
            return 1
        }
        if s > "c" {
            return -1
        }
        return 0
    }
    start, _, ok := mt.ItersMonotonyPredicate(pred, 0)
    if !ok || start == nil {
        t.Fatal("ItersMonotonyPredicate failed")
    }
    got := make([]string, 0)
    for start.Valid() {
        k := start.Key()
        if pred(k) != 0 {
            break
        }
        got = append(got, k)
        start.Next()
    }
    if len(got) != 2 || got[0] != "b" || got[1] != "c" {
        t.Fatalf("got %v", got)
    }
}
