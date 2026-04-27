package shardkv

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func resumeEnvInt(name string, def int) int {
	raw := os.Getenv(name)
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		return def
	}
	return v
}

func resumeWarmClient(ck *Clerk) {
	ck.refreshConfig()
}

func resumeKey(tag string, i int) string {
	return fmt.Sprintf("%c-%s-%06d", byte('0'+(i%10)), tag, i)
}

func resumeWriteKey(workerID, seq int) string {
	shardPrefix := byte('0' + ((workerID + seq) % 10))
	return fmt.Sprintf("%c-put-w%02d-%08d", shardPrefix, workerID, seq)
}

func resumePercentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 1 {
		return sorted[len(sorted)-1]
	}
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

func resumeWaitFor(timeout time.Duration, fn func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fn()
}

func resumeServerHasValue(kv *ShardKV, key, expected string) bool {
	if kv == nil {
		return false
	}
	kv.mu.Lock()
	engine := kv.kvDB[key2shard(key)]
	kv.mu.Unlock()
	if engine == nil {
		return false
	}
	enc, _ := engine.Get(key, 0)
	val, ok := decodeValue(enc)
	return ok && val == expected
}

func TestResumeWriteThroughputLocal(t *testing.T) {
	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	cfg.join(0)

	workers := resumeEnvInt("RESUME_WRITE_WORKERS", 8)
	seconds := resumeEnvInt("RESUME_WRITE_SECONDS", 5)
	valueBytes := resumeEnvInt("RESUME_WRITE_VALUE_BYTES", 64)
	value := strings.Repeat("w", valueBytes)

	clients := make([]*Clerk, workers)
	for i := 0; i < workers; i++ {
		clients[i] = cfg.makeClient(cfg.ctl)
		resumeWarmClient(clients[i])
	}

	var ready sync.WaitGroup
	var done sync.WaitGroup
	var total int64
	startCh := make(chan struct{})
	deadline := time.Now().Add(time.Duration(seconds) * time.Second)

	for workerID := 0; workerID < workers; workerID++ {
		ready.Add(1)
		done.Add(1)
		go func(id int) {
			defer done.Done()
			ck := clients[id]
			seq := 0
			ready.Done()
			<-startCh
			for time.Now().Before(deadline) {
				ck.Put(resumeWriteKey(id, seq), value)
				seq++
				atomic.AddInt64(&total, 1)
			}
		}(workerID)
	}

	ready.Wait()
	start := time.Now()
	close(startCh)
	done.Wait()
	elapsed := time.Since(start)
	tps := float64(total) / elapsed.Seconds()

	t.Logf("three-node write throughput: workers=%d valueBytes=%d ops=%d elapsed=%s throughput=%.1f ops/s",
		workers, valueBytes, total, elapsed.Round(time.Millisecond), tps)
}

func TestResumePointReadLatencyLocal(t *testing.T) {
	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	cfg.join(0)

	preload := resumeEnvInt("RESUME_READ_PRELOAD", 2000)
	samples := resumeEnvInt("RESUME_READ_SAMPLES", 1000)
	ck := cfg.makeClient(cfg.ctl)
	resumeWarmClient(ck)

	for i := 0; i < preload; i++ {
		ck.Put(resumeKey("read", i), fmt.Sprintf("value-%06d", i))
	}

	rng := rand.New(rand.NewSource(42))
	latencies := make([]time.Duration, 0, samples)
	for i := 0; i < samples; i++ {
		idx := rng.Intn(preload)
		key := resumeKey("read", idx)
		want := fmt.Sprintf("value-%06d", idx)
		begin := time.Now()
		got, err := ck.GetWithErr(key)
		cost := time.Since(begin)
		if err != OK {
			t.Fatalf("GetWithErr(%q) returned %v", key, err)
		}
		if got != want {
			t.Fatalf("GetWithErr(%q) = %q, want %q", key, got, want)
		}
		latencies = append(latencies, cost)
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	var total time.Duration
	for _, d := range latencies {
		total += d
	}
	mean := total / time.Duration(len(latencies))

	t.Logf("point-read latency: preload=%d samples=%d mean=%s p50=%s p95=%s p99=%s max=%s",
		preload,
		samples,
		mean.Round(time.Microsecond),
		resumePercentile(latencies, 0.50).Round(time.Microsecond),
		resumePercentile(latencies, 0.95).Round(time.Microsecond),
		resumePercentile(latencies, 0.99).Round(time.Microsecond),
		latencies[len(latencies)-1].Round(time.Microsecond))
}

func TestResumeRestartRecoveryLocal(t *testing.T) {
	cfg := make_config(t, 3, false, 2048)
	defer cfg.cleanup()

	cfg.join(0)

	keyCount := resumeEnvInt("RESUME_RECOVERY_KEYS", 2500)
	valueBytes := resumeEnvInt("RESUME_RECOVERY_VALUE_BYTES", 256)
	value := strings.Repeat("r", valueBytes)
	ck := cfg.makeClient(cfg.ctl)
	resumeWarmClient(ck)

	for i := 0; i < keyCount; i++ {
		ck.Put(resumeKey("recover", i), value)
	}

	ok := resumeWaitFor(10*time.Second, func() bool {
		for i := 0; i < cfg.n; i++ {
			if cfg.groups[0].saved[i].SnapshotSize() == 0 {
				return false
			}
		}
		return true
	})
	if !ok {
		t.Fatalf("snapshot was not generated within timeout")
	}

	sampleKeys := make([]string, 0, 10)
	for i := 0; i < 10; i++ {
		sampleKeys = append(sampleKeys, resumeKey("recover", i))
	}

	cfg.ShutdownGroup(0)
	time.Sleep(200 * time.Millisecond)

	begin := time.Now()
	cfg.StartGroup(0)

	recovered := resumeWaitFor(15*time.Second, func() bool {
		for replica := 0; replica < cfg.n; replica++ {
			server := cfg.groups[0].servers[replica]
			if server == nil {
				return false
			}
			for _, key := range sampleKeys {
				if !resumeServerHasValue(server, key, value) {
					return false
				}
			}
		}
		return true
	})
	recoveryCost := time.Since(begin)
	if !recovered {
		t.Fatalf("restarted replicas did not rebuild expected state within timeout")
	}

	for _, key := range sampleKeys {
		got, err := ck.GetWithErr(key)
		if err != OK {
			t.Fatalf("post-restart GetWithErr(%q) returned %v", key, err)
		}
		if got != value {
			t.Fatalf("post-restart GetWithErr(%q) = %q, want %q", key, got, value)
		}
	}

	t.Logf("restart recovery: keys=%d valueBytes=%d snapshotBytes=%d rebuildTime=%s",
		keyCount,
		valueBytes,
		cfg.groups[0].saved[0].SnapshotSize(),
		recoveryCost.Round(time.Millisecond))
}
