package shardkv

import (
	"reflect"
	"testing"
)

func TestRangeSingleShard(t *testing.T) {
	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.ctl)
	cfg.join(0)
	cfg.join(1)

	ck.Put("a1", "v1")
	ck.Put("a2", "v2")
	ck.Put("a3", "v3")

	got := ck.Range("a1", "a4", 0)
	expect := []KeyValue{
		{Key: "a1", Value: "v1"},
		{Key: "a2", Value: "v2"},
		{Key: "a3", Value: "v3"},
	}

	if !reflect.DeepEqual(got, expect) {
		t.Fatalf("range single shard got %v expect %v", got, expect)
	}
}

func TestRangeCrossShardAndLimit(t *testing.T) {
	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.ctl)
	cfg.join(0)
	cfg.join(1)

	ck.Put("a1", "va")
	ck.Put("b1", "vb")
	ck.Put("c1", "vc")
	ck.Put("d1", "vd")

	got := ck.Range("a", "e", 0)
	expect := []KeyValue{
		{Key: "a1", Value: "va"},
		{Key: "b1", Value: "vb"},
		{Key: "c1", Value: "vc"},
		{Key: "d1", Value: "vd"},
	}

	if !reflect.DeepEqual(got, expect) {
		t.Fatalf("range cross shard got %v expect %v", got, expect)
	}

	gotLimit := ck.Range("a", "e", 2)
	expectLimit := []KeyValue{
		{Key: "a1", Value: "va"},
		{Key: "b1", Value: "vb"},
	}
	if !reflect.DeepEqual(gotLimit, expectLimit) {
		t.Fatalf("range limit got %v expect %v", gotLimit, expectLimit)
	}
}
