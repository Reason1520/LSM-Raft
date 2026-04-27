package shardkv

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	"6.5840/shardkvpb"
)

func TestGRPCTxnAbortReleasesActiveTxn(t *testing.T) {
	cluster, client, cleanup := startExternalGRPCTestServer(t)
	defer cleanup()

	begin, err := client.TxnBegin(context.Background(), &shardkvpb.TxnBeginRequest{
		Isolation: shardkvpb.IsolationLevel_REPEATABLE_READ,
		KeyHint:   "a1",
	})
	if err != nil {
		t.Fatalf("TxnBegin failed: %v", err)
	}
	if begin.Err != string(OK) {
		t.Fatalf("TxnBegin err = %v", begin.Err)
	}

	waitGRPCActiveTxnAtLeast(t, cluster, 1)

	abort, err := client.TxnAbort(context.Background(), &shardkvpb.TxnGetRequest{
		Key:   "a1",
		TxnId: begin.TxnId,
	})
	if err != nil {
		t.Fatalf("TxnAbort failed: %v", err)
	}
	if abort.Err != string(OK) {
		t.Fatalf("TxnAbort err = %v", abort.Err)
	}

	waitGRPCActiveTxnExactly(t, cluster, 0)
}

func TestGRPCTxnEmptyCommitReleasesActiveTxn(t *testing.T) {
	cluster, client, cleanup := startExternalGRPCTestServer(t)
	defer cleanup()

	begin, err := client.TxnBegin(context.Background(), &shardkvpb.TxnBeginRequest{
		Isolation: shardkvpb.IsolationLevel_REPEATABLE_READ,
		KeyHint:   "a1",
	})
	if err != nil {
		t.Fatalf("TxnBegin failed: %v", err)
	}
	if begin.Err != string(OK) {
		t.Fatalf("TxnBegin err = %v", begin.Err)
	}

	waitGRPCActiveTxnAtLeast(t, cluster, 1)

	commit, err := client.TxnCommit(context.Background(), &shardkvpb.TxnCommitRequest{
		TxnId:     begin.TxnId,
		Isolation: shardkvpb.IsolationLevel_REPEATABLE_READ,
	})
	if err != nil {
		t.Fatalf("TxnCommit failed: %v", err)
	}
	if commit.Err != string(OK) {
		t.Fatalf("TxnCommit err = %v", commit.Err)
	}

	waitGRPCActiveTxnExactly(t, cluster, 0)
}

func startExternalGRPCTestServer(t *testing.T) (*GRPCCluster, shardkvpb.ShardKVClient, func()) {
	t.Helper()

	cluster, ck := StartGRPCCluster(3)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		cluster.Close()
		t.Fatalf("listen failed: %v", err)
	}

	server := grpc.NewServer()
	shardkvpb.RegisterShardKVServer(server, NewGRPCServer(ck))
	go server.Serve(lis)

	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithInsecure())
	if err != nil {
		server.Stop()
		_ = lis.Close()
		cluster.Close()
		t.Fatalf("grpc dial failed: %v", err)
	}

	cleanup := func() {
		_ = conn.Close()
		server.Stop()
		_ = lis.Close()
		cluster.Close()
	}
	return cluster, shardkvpb.NewShardKVClient(conn), cleanup
}

func waitGRPCActiveTxnAtLeast(t *testing.T, cluster *GRPCCluster, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if countGRPCActiveTxn(cluster) >= want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("active txn count never reached %d, got %d", want, countGRPCActiveTxn(cluster))
}

func waitGRPCActiveTxnExactly(t *testing.T, cluster *GRPCCluster, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if countGRPCActiveTxn(cluster) == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("active txn count never became %d, got %d", want, countGRPCActiveTxn(cluster))
}

func countGRPCActiveTxn(cluster *GRPCCluster) int {
	total := 0
	for _, server := range cluster.groupServer {
		if server == nil {
			continue
		}
		server.mu.Lock()
		total += len(server.activeTxn)
		server.mu.Unlock()
	}
	return total
}
