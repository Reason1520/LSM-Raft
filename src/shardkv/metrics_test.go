package shardkv_test

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"6.5840/shardkv"
	"6.5840/shardkvgrpc"
	"6.5840/shardkvpb"
)

func sumMetrics(all []shardkv.ShardKVMetricsSnapshot) shardkv.ShardKVMetricsSnapshot {
	var out shardkv.ShardKVMetricsSnapshot
	for _, m := range all {
		out.GRPCGetCount += m.GRPCGetCount
		out.GRPCGetTotal += m.GRPCGetTotal
		out.GRPCPutCount += m.GRPCPutCount
		out.GRPCPutTotal += m.GRPCPutTotal
		out.GRPCRangeCount += m.GRPCRangeCount
		out.GRPCRangeTotal += m.GRPCRangeTotal
		out.GetHandlerCount += m.GetHandlerCount
		out.GetHandlerTotal += m.GetHandlerTotal
		out.GetHandlerLockTotal += m.GetHandlerLockTotal
		out.PutHandlerCount += m.PutHandlerCount
		out.PutHandlerTotal += m.PutHandlerTotal
		out.PutHandlerLockTotal += m.PutHandlerLockTotal
		out.RangeHandlerCount += m.RangeHandlerCount
		out.RangeHandlerTotal += m.RangeHandlerTotal
		out.RangeHandlerLockTotal += m.RangeHandlerLockTotal
		out.LeaseReadOK += m.LeaseReadOK
		out.LeaseReadFallbackRaft += m.LeaseReadFallbackRaft
		out.LeaseReadWaitApplyCount += m.LeaseReadWaitApplyCount
		out.LeaseReadWaitApplyTotal += m.LeaseReadWaitApplyTotal
		out.RaftReadCount += m.RaftReadCount
		out.PutRaftCommitCount += m.PutRaftCommitCount
		out.PutRaftCommitTotal += m.PutRaftCommitTotal
		out.PutApplyWaitCount += m.PutApplyWaitCount
		out.PutApplyWaitTotal += m.PutApplyWaitTotal
		out.LSMGetCount += m.LSMGetCount
		out.LSMGetTotal += m.LSMGetTotal
		out.LSMPutCount += m.LSMPutCount
		out.LSMPutTotal += m.LSMPutTotal
		out.LSMRangeCount += m.LSMRangeCount
		out.LSMRangeTotal += m.LSMRangeTotal
		out.WaitChMissCount += m.WaitChMissCount
		out.Raft.PersistCount += m.Raft.PersistCount
		out.Raft.PersistTotal += m.Raft.PersistTotal
		out.Raft.StartCount += m.Raft.StartCount
		out.Raft.StartTotal += m.Raft.StartTotal
		out.Raft.LeaderPersistCount += m.Raft.LeaderPersistCount
		out.Raft.LeaderPersistTotal += m.Raft.LeaderPersistTotal
		out.Raft.AppendRPCCount += m.Raft.AppendRPCCount
		out.Raft.AppendRPCTotal += m.Raft.AppendRPCTotal
		out.Raft.AppendDataRPCCount += m.Raft.AppendDataRPCCount
		out.Raft.AppendDataRPCTotal += m.Raft.AppendDataRPCTotal
		out.Raft.FollowerAppendCount += m.Raft.FollowerAppendCount
		out.Raft.FollowerAppendTotal += m.Raft.FollowerAppendTotal
		out.Raft.FollowerAppendPersistCount += m.Raft.FollowerAppendPersistCount
		out.Raft.FollowerAppendPersistTotal += m.Raft.FollowerAppendPersistTotal
		out.Raft.MajorityAckCount += m.Raft.MajorityAckCount
		out.Raft.MajorityAckTotal += m.Raft.MajorityAckTotal
		out.Raft.ApplySendCount += m.Raft.ApplySendCount
		out.Raft.ApplySendTotal += m.Raft.ApplySendTotal
	}
	return out
}

func avgDur(total time.Duration, count uint64) time.Duration {
	if count == 0 {
		return 0
	}
	return time.Duration(int64(total) / int64(count))
}

func logGetBreakdown(t *testing.T, label string, totalOps int, elapsed time.Duration, m shardkv.ShardKVMetricsSnapshot) {
	leaseTotal := m.LeaseReadOK + m.LeaseReadFallbackRaft
	leaseHitRate := 0.0
	if leaseTotal > 0 {
		leaseHitRate = float64(m.LeaseReadOK) / float64(leaseTotal)
	}
	t.Logf("[%s] ops=%d elapsed=%s qps=%.1f lease_ok=%d fallback_raft=%d lease_hit=%.2f%% raft_read=%d waitChMiss=%d",
		label, totalOps, elapsed, float64(totalOps)/elapsed.Seconds(), m.LeaseReadOK, m.LeaseReadFallbackRaft, leaseHitRate*100, m.RaftReadCount, m.WaitChMissCount)
	t.Logf("[%s] grpc_get_avg=%s handler_avg=%s handler_lock_avg=%s lease_wait_apply_avg=%s lsm_get_avg=%s",
		label,
		avgDur(m.GRPCGetTotal, m.GRPCGetCount),
		avgDur(m.GetHandlerTotal, m.GetHandlerCount),
		avgDur(m.GetHandlerLockTotal, m.GetHandlerCount),
		avgDur(m.LeaseReadWaitApplyTotal, m.LeaseReadWaitApplyCount),
		avgDur(m.LSMGetTotal, m.LSMGetCount))
	t.Logf("[%s] raft_start_avg=%s leader_persist_avg=%s append_rpc_avg=%s append_data_rpc_avg=%s follower_append_avg=%s follower_append_persist_avg=%s majority_ack_avg=%s apply_send_avg=%s",
		label,
		avgDur(m.Raft.StartTotal, m.Raft.StartCount),
		avgDur(m.Raft.LeaderPersistTotal, m.Raft.LeaderPersistCount),
		avgDur(m.Raft.AppendRPCTotal, m.Raft.AppendRPCCount),
		avgDur(m.Raft.AppendDataRPCTotal, m.Raft.AppendDataRPCCount),
		avgDur(m.Raft.FollowerAppendTotal, m.Raft.FollowerAppendCount),
		avgDur(m.Raft.FollowerAppendPersistTotal, m.Raft.FollowerAppendPersistCount),
		avgDur(m.Raft.MajorityAckTotal, m.Raft.MajorityAckCount),
		avgDur(m.Raft.ApplySendTotal, m.Raft.ApplySendCount))
}

func logPutBreakdown(t *testing.T, label string, totalOps int, elapsed time.Duration, m shardkv.ShardKVMetricsSnapshot) {
	t.Logf("[%s] ops=%d elapsed=%s qps=%.1f put_handler_avg=%s put_handler_lock_avg=%s",
		label, totalOps, elapsed, float64(totalOps)/elapsed.Seconds(),
		avgDur(m.PutHandlerTotal, m.PutHandlerCount),
		avgDur(m.PutHandlerLockTotal, m.PutHandlerCount))
	t.Logf("[%s] put_raft_commit_avg=%s put_apply_wait_avg=%s lsm_put_avg=%s lsm_get_avg=%s",
		label,
		avgDur(m.PutRaftCommitTotal, m.PutRaftCommitCount),
		avgDur(m.PutApplyWaitTotal, m.PutApplyWaitCount),
		avgDur(m.LSMPutTotal, m.LSMPutCount),
		avgDur(m.LSMGetTotal, m.LSMGetCount))
	t.Logf("[%s] raft_start_avg=%s leader_persist_avg=%s append_rpc_avg=%s append_data_rpc_avg=%s follower_append_avg=%s follower_append_persist_avg=%s majority_ack_avg=%s apply_send_avg=%s",
		label,
		avgDur(m.Raft.StartTotal, m.Raft.StartCount),
		avgDur(m.Raft.LeaderPersistTotal, m.Raft.LeaderPersistCount),
		avgDur(m.Raft.AppendRPCTotal, m.Raft.AppendRPCCount),
		avgDur(m.Raft.AppendDataRPCTotal, m.Raft.AppendDataRPCCount),
		avgDur(m.Raft.FollowerAppendTotal, m.Raft.FollowerAppendCount),
		avgDur(m.Raft.FollowerAppendPersistTotal, m.Raft.FollowerAppendPersistCount),
		avgDur(m.Raft.MajorityAckTotal, m.Raft.MajorityAckCount),
		avgDur(m.Raft.ApplySendTotal, m.Raft.ApplySendCount))
}

func TestMetricsLeaseReadPath(t *testing.T) {
	dc, ck := shardkv.StartDemoCluster(3)
	defer dc.Close()

	for i := 0; i < 512; i++ {
		ck.Put(fmt.Sprintf("g%04d", i), "v")
	}
	time.Sleep(300 * time.Millisecond)

	const workers = 8
	const opsPerWorker = 500
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			worker := dc.NewClerk()
			for i := 0; i < opsPerWorker; i++ {
				key := fmt.Sprintf("g%04d", (id*opsPerWorker+i)%512)
				_ = worker.Get(key)
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	logGetBreakdown(t, "internal-get", workers*opsPerWorker, elapsed, sumMetrics(dc.ServerMetrics()))
}

func TestMetricsPutPath(t *testing.T) {
	dc, _ := shardkv.StartDemoCluster(3)
	defer dc.Close()

	const workers = 8
	const opsPerWorker = 120
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			worker := dc.NewClerk()
			for i := 0; i < opsPerWorker; i++ {
				key := fmt.Sprintf("%c-p%02d-%04d", byte('0'+((id+i)%10)), id, i)
				worker.Put(key, "v")
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	logPutBreakdown(t, "internal-put", workers*opsPerWorker, elapsed, sumMetrics(dc.ServerMetrics()))
}

func TestMetricsExternalGRPCGetPath(t *testing.T) {
	gc, ck := shardkv.StartGRPCCluster(3)
	defer gc.Close()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	grpcSrv := grpc.NewServer()
	external := shardkv.NewGRPCServer(ck)
	shardkvpb.RegisterShardKVServer(grpcSrv, external)
	go grpcSrv.Serve(lis)
	defer func() {
		grpcSrv.Stop()
		_ = lis.Close()
	}()

	client, err := shardkvgrpc.Dial(lis.Addr().String())
	if err != nil {
		t.Fatalf("grpc dial failed: %v", err)
	}
	defer client.Close()

	for i := 0; i < 512; i++ {
		if err := client.Put(fmt.Sprintf("h%04d", i), "v"); err != shardkv.OK {
			t.Fatalf("preload put failed at %d: %v", i, err)
		}
	}
	time.Sleep(300 * time.Millisecond)

	const workers = 8
	const opsPerWorker = 400
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c, err := shardkvgrpc.Dial(lis.Addr().String())
			if err != nil {
				t.Errorf("grpc dial failed: %v", err)
				return
			}
			defer c.Close()
			for i := 0; i < opsPerWorker; i++ {
				key := fmt.Sprintf("h%04d", (id*opsPerWorker+i)%512)
				c.Get(key)
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	clusterMetrics := sumMetrics(gc.ServerMetrics())
	grpcMetrics := external.MetricsSnapshot()
	t.Logf("[external-grpc-get] grpc_layer_avg=%s calls=%d", avgDur(grpcMetrics.GRPCGetTotal, grpcMetrics.GRPCGetCount), grpcMetrics.GRPCGetCount)
	logGetBreakdown(t, "external-grpc-get", workers*opsPerWorker, elapsed, clusterMetrics)
}
