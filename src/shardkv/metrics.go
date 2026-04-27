package shardkv

import (
	"sync/atomic"
	"time"

	"6.5840/raft"
)

type ShardKVMetricsSnapshot struct {
	GRPCGetCount   uint64
	GRPCGetTotal   time.Duration
	GRPCPutCount   uint64
	GRPCPutTotal   time.Duration
	GRPCRangeCount uint64
	GRPCRangeTotal time.Duration

	GetHandlerCount       uint64
	GetHandlerTotal       time.Duration
	GetHandlerLockTotal   time.Duration
	PutHandlerCount       uint64
	PutHandlerTotal       time.Duration
	PutHandlerLockTotal   time.Duration
	RangeHandlerCount     uint64
	RangeHandlerTotal     time.Duration
	RangeHandlerLockTotal time.Duration

	LeaseReadOK             uint64
	LeaseReadFallbackRaft   uint64
	LeaseReadWaitApplyCount uint64
	LeaseReadWaitApplyTotal time.Duration
	RaftReadCount           uint64
	PutRaftCommitCount      uint64
	PutRaftCommitTotal      time.Duration
	PutApplyWaitCount       uint64
	PutApplyWaitTotal       time.Duration
	LSMGetCount             uint64
	LSMGetTotal             time.Duration
	LSMPutCount             uint64
	LSMPutTotal             time.Duration
	LSMRangeCount           uint64
	LSMRangeTotal           time.Duration
	WaitChMissCount         uint64

	Raft raft.RaftMetricsSnapshot
}

type shardKVMetrics struct {
	grpcGetCount     uint64
	grpcGetTotalNS   uint64
	grpcPutCount     uint64
	grpcPutTotalNS   uint64
	grpcRangeCount   uint64
	grpcRangeTotalNS uint64

	getHandlerCount     uint64
	getHandlerTotalNS   uint64
	getHandlerLockNS    uint64
	putHandlerCount     uint64
	putHandlerTotalNS   uint64
	putHandlerLockNS    uint64
	rangeHandlerCount   uint64
	rangeHandlerTotalNS uint64
	rangeHandlerLockNS  uint64

	leaseReadOK             uint64
	leaseReadFallbackRaft   uint64
	leaseReadWaitApplyCount uint64
	leaseReadWaitApplyNS    uint64
	raftReadCount           uint64
	putRaftCommitCount      uint64
	putRaftCommitNS         uint64
	putApplyWaitCount       uint64
	putApplyWaitNS          uint64
	lsmGetCount             uint64
	lsmGetNS                uint64
	lsmPutCount             uint64
	lsmPutNS                uint64
	lsmRangeCount           uint64
	lsmRangeNS              uint64
	waitChMissCount         uint64
}

type proposalTrace struct {
	opType    string
	startedAt time.Time
	abandoned bool
}

func observeDuration(total *uint64, count *uint64, d time.Duration) {
	if d < 0 {
		return
	}
	atomic.AddUint64(total, uint64(d))
	atomic.AddUint64(count, 1)
}

func addDuration(total *uint64, d time.Duration) {
	if d < 0 {
		return
	}
	atomic.AddUint64(total, uint64(d))
}

func (m *shardKVMetrics) observeGRPCGet(d time.Duration) {
	observeDuration(&m.grpcGetTotalNS, &m.grpcGetCount, d)
}

func (m *shardKVMetrics) observeGRPCPut(d time.Duration) {
	observeDuration(&m.grpcPutTotalNS, &m.grpcPutCount, d)
}

func (m *shardKVMetrics) observeGRPCRange(d time.Duration) {
	observeDuration(&m.grpcRangeTotalNS, &m.grpcRangeCount, d)
}

func (m *shardKVMetrics) observeGetHandler(d time.Duration) {
	observeDuration(&m.getHandlerTotalNS, &m.getHandlerCount, d)
}

func (m *shardKVMetrics) observePutHandler(d time.Duration) {
	observeDuration(&m.putHandlerTotalNS, &m.putHandlerCount, d)
}

func (m *shardKVMetrics) observeRangeHandler(d time.Duration) {
	observeDuration(&m.rangeHandlerTotalNS, &m.rangeHandlerCount, d)
}

func (m *shardKVMetrics) observeGetLockWait(d time.Duration) {
	addDuration(&m.getHandlerLockNS, d)
}

func (m *shardKVMetrics) observePutLockWait(d time.Duration) {
	addDuration(&m.putHandlerLockNS, d)
}

func (m *shardKVMetrics) observeRangeLockWait(d time.Duration) {
	addDuration(&m.rangeHandlerLockNS, d)
}

func (m *shardKVMetrics) incLeaseReadOK() {
	atomic.AddUint64(&m.leaseReadOK, 1)
}

func (m *shardKVMetrics) incLeaseReadFallbackRaft() {
	atomic.AddUint64(&m.leaseReadFallbackRaft, 1)
}

func (m *shardKVMetrics) observeLeaseReadWaitApply(d time.Duration) {
	observeDuration(&m.leaseReadWaitApplyNS, &m.leaseReadWaitApplyCount, d)
}

func (m *shardKVMetrics) incRaftRead() {
	atomic.AddUint64(&m.raftReadCount, 1)
}

func (m *shardKVMetrics) observePutRaftCommit(d time.Duration) {
	observeDuration(&m.putRaftCommitNS, &m.putRaftCommitCount, d)
}

func (m *shardKVMetrics) observePutApplyWait(d time.Duration) {
	observeDuration(&m.putApplyWaitNS, &m.putApplyWaitCount, d)
}

func (m *shardKVMetrics) observeLSMGet(d time.Duration) {
	observeDuration(&m.lsmGetNS, &m.lsmGetCount, d)
}

func (m *shardKVMetrics) observeLSMPut(d time.Duration) {
	observeDuration(&m.lsmPutNS, &m.lsmPutCount, d)
}

func (m *shardKVMetrics) observeLSMRange(d time.Duration) {
	observeDuration(&m.lsmRangeNS, &m.lsmRangeCount, d)
}

func (m *shardKVMetrics) incWaitChMiss() {
	atomic.AddUint64(&m.waitChMissCount, 1)
}

func (kv *ShardKV) MetricsSnapshot() ShardKVMetricsSnapshot {
	snapshot := ShardKVMetricsSnapshot{
		GRPCGetCount:            atomic.LoadUint64(&kv.metrics.grpcGetCount),
		GRPCGetTotal:            time.Duration(atomic.LoadUint64(&kv.metrics.grpcGetTotalNS)),
		GRPCPutCount:            atomic.LoadUint64(&kv.metrics.grpcPutCount),
		GRPCPutTotal:            time.Duration(atomic.LoadUint64(&kv.metrics.grpcPutTotalNS)),
		GRPCRangeCount:          atomic.LoadUint64(&kv.metrics.grpcRangeCount),
		GRPCRangeTotal:          time.Duration(atomic.LoadUint64(&kv.metrics.grpcRangeTotalNS)),
		GetHandlerCount:         atomic.LoadUint64(&kv.metrics.getHandlerCount),
		GetHandlerTotal:         time.Duration(atomic.LoadUint64(&kv.metrics.getHandlerTotalNS)),
		GetHandlerLockTotal:     time.Duration(atomic.LoadUint64(&kv.metrics.getHandlerLockNS)),
		PutHandlerCount:         atomic.LoadUint64(&kv.metrics.putHandlerCount),
		PutHandlerTotal:         time.Duration(atomic.LoadUint64(&kv.metrics.putHandlerTotalNS)),
		PutHandlerLockTotal:     time.Duration(atomic.LoadUint64(&kv.metrics.putHandlerLockNS)),
		RangeHandlerCount:       atomic.LoadUint64(&kv.metrics.rangeHandlerCount),
		RangeHandlerTotal:       time.Duration(atomic.LoadUint64(&kv.metrics.rangeHandlerTotalNS)),
		RangeHandlerLockTotal:   time.Duration(atomic.LoadUint64(&kv.metrics.rangeHandlerLockNS)),
		LeaseReadOK:             atomic.LoadUint64(&kv.metrics.leaseReadOK),
		LeaseReadFallbackRaft:   atomic.LoadUint64(&kv.metrics.leaseReadFallbackRaft),
		LeaseReadWaitApplyCount: atomic.LoadUint64(&kv.metrics.leaseReadWaitApplyCount),
		LeaseReadWaitApplyTotal: time.Duration(atomic.LoadUint64(&kv.metrics.leaseReadWaitApplyNS)),
		RaftReadCount:           atomic.LoadUint64(&kv.metrics.raftReadCount),
		PutRaftCommitCount:      atomic.LoadUint64(&kv.metrics.putRaftCommitCount),
		PutRaftCommitTotal:      time.Duration(atomic.LoadUint64(&kv.metrics.putRaftCommitNS)),
		PutApplyWaitCount:       atomic.LoadUint64(&kv.metrics.putApplyWaitCount),
		PutApplyWaitTotal:       time.Duration(atomic.LoadUint64(&kv.metrics.putApplyWaitNS)),
		LSMGetCount:             atomic.LoadUint64(&kv.metrics.lsmGetCount),
		LSMGetTotal:             time.Duration(atomic.LoadUint64(&kv.metrics.lsmGetNS)),
		LSMPutCount:             atomic.LoadUint64(&kv.metrics.lsmPutCount),
		LSMPutTotal:             time.Duration(atomic.LoadUint64(&kv.metrics.lsmPutNS)),
		LSMRangeCount:           atomic.LoadUint64(&kv.metrics.lsmRangeCount),
		LSMRangeTotal:           time.Duration(atomic.LoadUint64(&kv.metrics.lsmRangeNS)),
		WaitChMissCount:         atomic.LoadUint64(&kv.metrics.waitChMissCount),
	}
	if kv.rf != nil {
		snapshot.Raft = kv.rf.MetricsSnapshot()
	}
	return snapshot
}
