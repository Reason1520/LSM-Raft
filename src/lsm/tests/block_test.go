package lsm_test

import (
    "bytes"
    "strings"
    "testing"

    "6.5840/lsm"
)

func buildTestBlock(t *testing.T) *lsm.Block {
    t.Helper()
    b := lsm.NewBlock(1024)
    if !b.AddEntry("a", "1", 0, false) {
        t.Fatal("add a failed")
    }
    if !b.AddEntry("b", "22", 0, false) {
        t.Fatal("add b failed")
    }
    if !b.AddEntry("c", "333", 0, false) {
        t.Fatal("add c failed")
    }
    return b
}

func TestBlockEncodeDecodeNoHash(t *testing.T) {
    b := buildTestBlock(t)
    enc := b.Encode(false)
    dec := lsm.DecodeBlock(enc, 1024, false)
    if dec == nil {
        t.Fatal("DecodeBlock returned nil")
    }
    if dec.EntryCount() != 3 {
        t.Fatalf("entry count = %d", dec.EntryCount())
    }
    k, _ := dec.GetKeyAt(1)
    v, _ := dec.GetValueAt(1)
    if k != "b" || v != "22" {
        t.Fatalf("got %q=%q", k, v)
    }
    if dec.Size() != b.Size() {
        t.Fatalf("size mismatch %d vs %d", dec.Size(), b.Size())
    }
}

func TestBlockEncodeDecodeWithHash(t *testing.T) {
    b := buildTestBlock(t)
    enc := b.Encode(true)
    dec := lsm.DecodeBlock(enc, 1024, true)
    if dec == nil {
        t.Fatal("DecodeBlock returned nil")
    }

    // corrupt one byte
    bad := make([]byte, len(enc))
    copy(bad, enc)
    bad[0] ^= 0xFF
    if lsm.DecodeBlock(bad, 1024, true) != nil {
        t.Fatal("expected checksum mismatch")
    }
}

func TestBlockSearchAndIterator(t *testing.T) {
    b := buildTestBlock(t)

    if idx := b.BinarySearch("b", 0); idx != 1 {
        t.Fatalf("BinarySearch b = %d", idx)
    }
    if idx := b.BinarySearch("x", 0); idx != -1 {
        t.Fatalf("BinarySearch x = %d", idx)
    }
    if idx := b.FindKeyIndex("b"); idx != 1 {
        t.Fatalf("FindKeyIndex b = %d", idx)
    }
    if idx := b.FindKeyIndex("bb"); idx != 2 {
        t.Fatalf("FindKeyIndex bb = %d", idx)
    }

    it := b.Begin(0)
    if !it.Valid() || it.Key() != "a" || it.Value() != "1" {
        t.Fatalf("begin got %q=%q", it.Key(), it.Value())
    }
    it.Next()
    if it.Key() != "b" {
        t.Fatalf("next key = %q", it.Key())
    }

    it.Seek("c")
    if it.Key() != "c" {
        t.Fatalf("seek key = %q", it.Key())
    }
}

func TestBlockPrefixAndPredicate(t *testing.T) {
    b := lsm.NewBlock(1024)
    for _, kv := range []struct{ k, v string }{
        {"ab", "1"}, {"abc", "2"}, {"abd", "3"}, {"b", "4"}, {"c", "5"},
    } {
        if !b.AddEntry(kv.k, kv.v, 0, false) {
            t.Fatal("add entry failed")
        }
    }

    left, _, ok := b.ItersPreffix("ab", 0)
    if !ok || left == nil {
        t.Fatal("ItersPreffix failed")
    }
    got := make([]string, 0)
    for left.Valid() {
        k := left.Key()
        if !strings.HasPrefix(k, "ab") {
            break
        }
        got = append(got, k)
        left.Next()
    }
    if !bytes.Equal([]byte(strings.Join(got, ",")), []byte("ab,abc,abd")) {
        t.Fatalf("prefix keys = %v", got)
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
    lit, _, ok := b.ItersMonotonyPredicate(pred, 0)
    if !ok || lit == nil {
        t.Fatal("ItersMonotonyPredicate failed")
    }
    keys := make([]string, 0)
    for lit.Valid() {
        k := lit.Key()
        if pred(k) != 0 {
            break
        }
        keys = append(keys, k)
        lit.Next()
    }
    if !bytes.Equal([]byte(strings.Join(keys, ",")), []byte("b,c")) {
        t.Fatalf("predicate keys = %v", keys)
    }
}
