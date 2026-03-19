package lsm_test

import (
    "testing"

    "6.5840/lsm"
)

func TestSkipListPutGetRemove(t *testing.T) {
    sl := lsm.NewSkipList(16)
    sl.Put("a", "1", 0)
    sl.Put("b", "2", 0)

    it := sl.Get("a", 0)
    if !it.Valid() || it.Key() != "a" || it.Value() != "1" {
        t.Fatalf("get a = %q=%q", it.Key(), it.Value())
    }

    sl.Remove("a")
    it = sl.Get("a", 0)
    if it.Valid() {
        t.Fatalf("expected a removed")
    }
}

func TestSkipListIteratorSeek(t *testing.T) {
    sl := lsm.NewSkipList(16)
    sl.Put("a", "1", 0)
    sl.Put("b", "2", 0)
    sl.Put("c", "3", 0)

    it := sl.NewIterator(nil, 0)
    it.Seek("b")
    if !it.Valid() || it.Key() != "b" {
        t.Fatalf("seek b got %q", it.Key())
    }
    it.Next()
    if it.Valid() && it.Key() != "c" {
        t.Fatalf("next got %q", it.Key())
    }
}

func TestSkipListItersMonotonyPredicate(t *testing.T) {
    sl := lsm.NewSkipList(16)
    for _, k := range []string{"a", "b", "c", "d"} {
        sl.Put(k, k, 0)
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
    left, _, ok := sl.ItersMonotonyPredicate(pred, 0)
    if !ok || left == nil {
        t.Fatal("ItersMonotonyPredicate failed")
    }
    got := make([]string, 0)
    for left.Valid() {
        k := left.Key()
        if pred(k) != 0 {
            break
        }
        got = append(got, k)
        left.Next()
    }
    if len(got) != 2 || got[0] != "b" || got[1] != "c" {
        t.Fatalf("got %v", got)
    }
}
