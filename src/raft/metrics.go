package raft

import (
	"sync/atomic"
	"time"
)

type RaftMetricsSnapshot struct {
	PersistCount               uint64
	PersistTotal               time.Duration
	StartCount                 uint64
	StartTotal                 time.Duration
	LeaderPersistCount         uint64
	LeaderPersistTotal         time.Duration
	AppendRPCCount             uint64
	AppendRPCTotal             time.Duration
	AppendDataRPCCount         uint64
	AppendDataRPCTotal         time.Duration
	FollowerAppendCount        uint64
	FollowerAppendTotal        time.Duration
	FollowerAppendPersistCount uint64
	FollowerAppendPersistTotal time.Duration
	MajorityAckCount           uint64
	MajorityAckTotal           time.Duration
	ApplySendCount             uint64
	ApplySendTotal             time.Duration
}

func observeRaftDuration(total *uint64, count *uint64, d time.Duration) {
	if d < 0 {
		return
	}
	atomic.AddUint64(total, uint64(d))
	atomic.AddUint64(count, 1)
}

func (rf *Raft) observePersist(d time.Duration) {
	observeRaftDuration(&rf.persistTimeNS, &rf.persistCount, d)
}

func (rf *Raft) observeAppendRPC(d time.Duration) {
	observeRaftDuration(&rf.appendRPCTimeNS, &rf.appendRPCCount, d)
}

func (rf *Raft) observeAppendDataRPC(d time.Duration) {
	observeRaftDuration(&rf.appendDataRPCTimeNS, &rf.appendDataRPCCount, d)
}

func (rf *Raft) observeStart(d time.Duration) {
	observeRaftDuration(&rf.startTimeNS, &rf.startCount, d)
}

func (rf *Raft) observeLeaderPersist(d time.Duration) {
	observeRaftDuration(&rf.leaderPersistTimeNS, &rf.leaderPersistCount, d)
}

func (rf *Raft) observeFollowerAppend(d time.Duration) {
	observeRaftDuration(&rf.followerAppendTimeNS, &rf.followerAppendCount, d)
}

func (rf *Raft) observeFollowerAppendPersist(d time.Duration) {
	observeRaftDuration(&rf.followerAppendPersistTimeNS, &rf.followerAppendPersistCount, d)
}

func (rf *Raft) observeMajorityAck(d time.Duration) {
	observeRaftDuration(&rf.majorityAckTimeNS, &rf.majorityAckCount, d)
}

func (rf *Raft) observeApplySend(d time.Duration) {
	observeRaftDuration(&rf.applySendTimeNS, &rf.applySendCount, d)
}

func (rf *Raft) MetricsSnapshot() RaftMetricsSnapshot {
	return RaftMetricsSnapshot{
		PersistCount:               atomic.LoadUint64(&rf.persistCount),
		PersistTotal:               time.Duration(atomic.LoadUint64(&rf.persistTimeNS)),
		StartCount:                 atomic.LoadUint64(&rf.startCount),
		StartTotal:                 time.Duration(atomic.LoadUint64(&rf.startTimeNS)),
		LeaderPersistCount:         atomic.LoadUint64(&rf.leaderPersistCount),
		LeaderPersistTotal:         time.Duration(atomic.LoadUint64(&rf.leaderPersistTimeNS)),
		AppendRPCCount:             atomic.LoadUint64(&rf.appendRPCCount),
		AppendRPCTotal:             time.Duration(atomic.LoadUint64(&rf.appendRPCTimeNS)),
		AppendDataRPCCount:         atomic.LoadUint64(&rf.appendDataRPCCount),
		AppendDataRPCTotal:         time.Duration(atomic.LoadUint64(&rf.appendDataRPCTimeNS)),
		FollowerAppendCount:        atomic.LoadUint64(&rf.followerAppendCount),
		FollowerAppendTotal:        time.Duration(atomic.LoadUint64(&rf.followerAppendTimeNS)),
		FollowerAppendPersistCount: atomic.LoadUint64(&rf.followerAppendPersistCount),
		FollowerAppendPersistTotal: time.Duration(atomic.LoadUint64(&rf.followerAppendPersistTimeNS)),
		MajorityAckCount:           atomic.LoadUint64(&rf.majorityAckCount),
		MajorityAckTotal:           time.Duration(atomic.LoadUint64(&rf.majorityAckTimeNS)),
		ApplySendCount:             atomic.LoadUint64(&rf.applySendCount),
		ApplySendTotal:             time.Duration(atomic.LoadUint64(&rf.applySendTimeNS)),
	}
}
