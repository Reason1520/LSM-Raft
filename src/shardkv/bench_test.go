package shardkv_test

import (
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"

	"6.5840/shardkv"
	"6.5840/shardkvgrpc"
	"6.5840/shardkvpb"
)

func reportQPS(b *testing.B, ops int, start time.Time) {
	elapsed := time.Since(start).Seconds()
	if elapsed <= 0 {
		return
	}
	b.ReportMetric(float64(ops)/elapsed, "ops/s")
}

func benchLogf(format string, args ...interface{}) {
	if os.Getenv("BENCH_PROGRESS") != "1" {
		return
	}
	fmt.Fprintf(os.Stderr, format, args...)
	if len(format) == 0 || format[len(format)-1] != '\n' {
		fmt.Fprint(os.Stderr, "\n")
	}
}

func startProgress(label string) (func(), *uint64, time.Time) {
	start := time.Now()
	if os.Getenv("BENCH_PROGRESS") != "1" {
		return func() {}, nil, start
	}
	var count uint64
	stopCh := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				done := atomic.LoadUint64(&count)
				elapsed := time.Since(start).Seconds()
				qps := float64(done)
				if elapsed > 0 {
					qps = qps / elapsed
				}
				fmt.Fprintf(os.Stderr, "[bench %s] progress=%d elapsed=%.1fs qps=%.1f\n", label, done, elapsed, qps)
			case <-stopCh:
				return
			}
		}
	}()
	stop := func() {
		close(stopCh)
	}
	return stop, &count, start
}

func BenchmarkShardKVPut(b *testing.B) {
	benchLogf("[bench labrpc-put] init: start demo cluster")
	dc, ck := shardkv.StartDemoCluster(3)
	defer dc.Close()
	benchLogf("[bench labrpc-put] init: cluster ready")

	keys := make([]string, 1024)
	for i := 0; i < len(keys); i++ {
		keys[i] = fmt.Sprintf("k%04d", i)
	}

	b.ReportAllocs()
	b.ResetTimer()
	stop, counter, start := startProgress("labrpc-put")
	for i := 0; i < b.N; i++ {
		ck.Put(keys[i%len(keys)], "v")
		if counter != nil {
			atomic.AddUint64(counter, 1)
		}
	}
	stop()
	reportQPS(b, b.N, start)
}

func BenchmarkShardKVPutParallel(b *testing.B) {
	benchLogf("[bench labrpc-put-par] init: start demo cluster")
	dc, _ := shardkv.StartDemoCluster(3)
	defer dc.Close()
	benchLogf("[bench labrpc-put-par] init: cluster ready")

	var workerID uint64
	b.ReportAllocs()
	b.ResetTimer()
	stop, counter, start := startProgress("labrpc-put-par")
	b.RunParallel(func(pb *testing.PB) {
		id := int(atomic.AddUint64(&workerID, 1) - 1)
		ck := dc.NewClerk()
		seq := 0
		for pb.Next() {
			key := fmt.Sprintf("%c-put-par-w%02d-%08d", byte('0'+((id+seq)%10)), id, seq)
			ck.Put(key, "v")
			seq++
			if counter != nil {
				atomic.AddUint64(counter, 1)
			}
		}
	})
	stop()
	reportQPS(b, b.N, start)
}

func BenchmarkShardKVGet(b *testing.B) {
	benchLogf("[bench labrpc-get] init: start demo cluster")
	dc, ck := shardkv.StartDemoCluster(3)
	defer dc.Close()
	benchLogf("[bench labrpc-get] init: cluster ready")

	keys := make([]string, 1024)
	for i := 0; i < len(keys); i++ {
		keys[i] = fmt.Sprintf("k%04d", i)
		ck.Put(keys[i], "v")
	}

	b.ReportAllocs()
	b.ResetTimer()
	stop, counter, start := startProgress("labrpc-get")
	for i := 0; i < b.N; i++ {
		_ = ck.Get(keys[i%len(keys)])
		if counter != nil {
			atomic.AddUint64(counter, 1)
		}
	}
	stop()
	reportQPS(b, b.N, start)
}

func BenchmarkShardKVRange(b *testing.B) {
	benchLogf("[bench labrpc-range] init: start demo cluster")
	dc, ck := shardkv.StartDemoCluster(3)
	defer dc.Close()
	benchLogf("[bench labrpc-range] init: cluster ready")

	for i := 0; i < 5000; i++ {
		ck.Put(fmt.Sprintf("a%06d", i), "v")
	}

	b.ReportAllocs()
	b.ResetTimer()
	stop, counter, start := startProgress("labrpc-range")
	for i := 0; i < b.N; i++ {
		_ = ck.Range("a", "a~", 0)
		if counter != nil {
			atomic.AddUint64(counter, 1)
		}
	}
	stop()
	reportQPS(b, b.N, start)
}

func BenchmarkGRPCPutGet(b *testing.B) {
	benchLogf("[bench grpc-putget] init: start grpc cluster")
	dc, ck := shardkv.StartGRPCCluster(3)
	defer dc.Close()

	benchLogf("[bench grpc-putget] init: start grpc server")
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen failed: %v", err)
	}
	grpcServer := grpc.NewServer()
	shardkvpb.RegisterShardKVServer(grpcServer, shardkv.NewGRPCServer(ck))
	go grpcServer.Serve(lis)
	defer func() {
		grpcServer.Stop()
		_ = lis.Close()
	}()

	benchLogf("[bench grpc-putget] init: dial grpc client")
	client, err := shardkvgrpc.Dial(lis.Addr().String())
	if err != nil {
		b.Fatalf("grpc dial failed: %v", err)
	}
	defer client.Close()

	keys := make([]string, 1024)
	for i := 0; i < len(keys); i++ {
		keys[i] = fmt.Sprintf("k%04d", i)
	}

	b.ReportAllocs()
	b.ResetTimer()
	stop, counter, start := startProgress("grpc-putget")
	for i := 0; i < b.N; i++ {
		key := keys[i%len(keys)]
		_ = client.Put(key, "v")
		_, _ = client.Get(key)
		if counter != nil {
			atomic.AddUint64(counter, 2)
		}
	}
	stop()
	reportQPS(b, 2*b.N, start)
}
