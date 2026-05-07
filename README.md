# RaftKV

RaftKV 是一个基于 **Raft + Sharding + LSM-Tree** 的分布式 KV 存储项目。项目以 MIT 6.5840 的 Raft / ShardKV 实验框架为基础，在其上扩展了 LSM 存储引擎、MVCC、范围查询、gRPC 访问层、单分片事务、跨分片只读事务，以及基于 2PC + Raft 的跨分片写事务原型。

这个项目的重点不是包装一个简单 KV API，而是把一个分布式存储系统里常见的几条链路串起来：一致性复制、分片迁移、快照恢复、存储引擎、事务冲突检测、外部访问入口和性能压测。

## 架构

```text
External Client
      |
      | gRPC
      v
gRPC Server / Client SDK
      |
      | Clerk
      v
ShardKV Group
      |
      | Raft log replication
      v
ShardKV State Machine
      |
      | per-shard engine
      v
LSM-Tree + MVCC
```

核心模块：

| 路径 | 说明 |
| --- | --- |
| `src/raft` | Raft 共识实现，包含选举、日志复制、snapshot、InstallSnapshot、leader lease read、per-follower replicator、持久化优化 |
| `src/shardctrler` | 分片配置管理，负责 group 加入、离开、迁移和 shard 分配 |
| `src/shardkv` | 分片 KV 状态机，负责读写、迁移、snapshot、事务、指标与 benchmark |
| `src/lsm` | LSM-Tree 存储引擎，包含 MemTable、SSTable、Bloom Filter、Block Cache、Compaction、MVCC |
| `src/shardkvpb` | 对外 gRPC proto 与生成代码 |
| `src/shardkvgrpc` | 对外 gRPC 客户端封装 |
| `src/labrpc` / `src/labrpcpb` | 课程测试用内存 RPC，以及可选的内部 gRPC 传输封装 |
| `src/cmd/shardkv_demo` | in-process 交互式 demo |
| `src/cmd/shardkv_grpc` | 对外 gRPC 服务启动入口 |

## 功能特性

- **分布式一致性**：基于 Raft 实现 leader election、log replication、commit/apply、snapshot 和重启恢复。
- **分片 KV**：通过 `shardctrler` 管理配置变更，ShardKV 支持 shard 迁移、迁移期间的服务状态控制和旧数据 GC。
- **LSM 存储引擎**：每个 shard 一个 LSM engine，支持 MemTable、SST、Bloom Filter、Block Cache、后台 flush / compaction。
- **MVCC**：使用 Raft log index 作为版本号，事务读按 snapshot 版本读取，compaction 时根据 GC watermark 回收旧版本。
- **普通 KV API**：支持 `Get`、`Put`、`Append`、`Range(start, end, limit)`。
- **读优化**：普通 `Get/Range` 优先走 leader lease read，拿到安全 `readIndex` 并等待本地 apply 后直接读 LSM；lease 不可用时回退到 Raft read。
- **写优化**：普通 `Put/Append` 支持短窗口 batch/group commit，多条写共享一次 Raft 提交周期。
- **事务能力**：支持单 shard 读写事务；内部 Clerk 支持跨 shard 一致性只读事务；跨 shard 写事务采用客户端编排的 2PC + Raft 原型。
- **gRPC 入口**：提供对外 gRPC server 和 Go client SDK；内部 RPC 可使用课程 `labrpc` 或 gRPC 传输封装。
- **性能验证**：包含 Go benchmark、恢复测试、ghz 外部 gRPC 压测脚本和 YCSB gRPC binding 实验。

## 一致性语义

| 操作 | 语义 |
| --- | --- |
| 单 shard `Get` | 线性一致。优先 leader lease read，失败后通过 Raft 日志路径读取 |
| 单 shard `Put/Append` | 线性一致。写入必须经 Raft commit 后 apply 到状态机 |
| 单 shard `Range` | 线性一致。按 `[start, end)` 扫描目标 shard |
| 跨 shard 普通 `Range` | 客户端 fan-out 到多个 shard 后合并结果，不承诺全局原子快照 |
| 单 shard 事务 | Begin 固定 snapshot，Commit 将读写集作为一条 Raft 日志提交并做冲突检测 |
| 跨 shard 只读事务 | 内部 Clerk 固定配置，并在各目标 group 上固定本地 snapshot |
| 跨 shard 写事务 | 内部 Clerk 使用 2PC：coordinator 记录全局决议，participant prepare 后锁定写集并等待 commit / abort |

当前对外 gRPC 事务 API 仍保持 **单 shard 事务模型**；跨 shard 只读事务和跨 shard 2PC 写事务目前主要暴露在内部 `shardkv.Clerk` 层。

## 快速开始

项目 Go module 位于 `src` 目录，下面的命令默认在仓库根目录执行。

### 运行交互式 demo

```bash
cd src
go run ./cmd/shardkv_demo
```

可用命令：

```text
get <key>
put <key> <value>
append <key> <value>
range <start> <end|-> [limit]
txndemo
exit
```

### 启动对外 gRPC 服务

```bash
cd src
go run ./cmd/shardkv_grpc -addr :50051 -n 3
```

`-n 3` 会在本地启动一个 3 副本 shardkv group，并通过 `:50051` 暴露 gRPC 服务。

### 使用 Go gRPC 客户端

```go
cli, err := shardkvgrpc.Dial("127.0.0.1:50051")
if err != nil {
    panic(err)
}
defer cli.Close()

_ = cli.Put("a1", "v1")
value, _ := cli.Get("a1")

kvs, _ := cli.Range("a", "a~", 0)

tx, _ := cli.BeginTxn(shardkv.RepeatableRead, "a1")
_ = tx.Put("a2", "v2")
_, _ = tx.Range("a", "a~", 0)
ok := tx.Commit()
```

## 测试

在 `src` 目录执行：

```bash
go test ./raft -run "TestBasicAgree3B|TestRejoin3B" -count=1
go test ./lsm ./lsm/tests -count=1
go test ./shardkv -run "TestSnapshot5B|TestTxn|TestRange|TestReadTxn|TestCrossShardTxn" -count=1
```

完整回归：

```bash
go test ./raft ./shardctrler ./shardkv ./lsm ./lsm/tests -count=1
```

恢复与性能回归：

```bash
go test ./shardkv -run TestResumeWriteThroughputLocal -count=1 -v
go test ./shardkv -run TestResumePointReadLatencyLocal -count=1 -v
go test ./shardkv -run TestResumeRestartRecoveryLocal -count=1 -v
```

Benchmark：

```bash
go test ./shardkv -run ^$ -bench BenchmarkShardKVPut -benchtime=15s -benchmem -count=1
go test ./shardkv -run ^$ -bench BenchmarkShardKVPutParallel -benchtime=15s -benchmem -count=1
go test ./shardkv -run ^$ -bench BenchmarkShardKVGet -benchtime=15s -benchmem -count=1
go test ./shardkv -run ^$ -bench BenchmarkShardKVRange -benchtime=15s -benchmem -count=1
go test ./shardkv -run ^$ -bench BenchmarkGRPCPutGet -benchtime=15s -benchmem -count=1
```

## 压测

仓库中保留了两类本地压测入口：

- `ghz_local`：用于直接压对外 gRPC API，例如 `Get`、`Put`、`Range`。
- `ycsb_local`：用于通过 YCSB gRPC binding 跑标准 KV workload，例如 Workload A/B/C。

示例：

```powershell
powershell -ExecutionPolicy Bypass -File .\ghz_local\run_standard_bench.ps1
powershell -ExecutionPolicy Bypass -File .\ycsb_local\run_ycsb_grpc.ps1
```

## 本机参考结果

以下数字来自本地 Windows / Ryzen 7 5800H 环境，只用于说明当前实现量级和优化前后对比，不代表生产 SLA。

### ghz 外部 gRPC 压测

| 场景 | 并发 | 吞吐 | 平均延迟 | p95 | p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| `Get` | 8 | `11098 req/s` | `0.65 ms` | `1.10 ms` | `1.53 ms` |
| `Get` | 32 | `14325 req/s` | `1.91 ms` | `3.19 ms` | `5.08 ms` |
| `Put` | 8 | `468 req/s` | `16.91 ms` | `28.30 ms` | `36.91 ms` |
| `Put` | 32 | `1320 req/s` | `23.88 ms` | `37.86 ms` | `43.43 ms` |
| `Range` | 8 | `676 req/s` | `11.69 ms` | `15.73 ms` | `31.14 ms` |
| `Range` | 16 | `718 req/s` | `22.11 ms` | `34.61 ms` | `48.40 ms` |

### YCSB gRPC binding

参数：`RecordCount=3000`、`OperationCount=6000`、`Threads=8`。

| Workload | 读写比例 | 吞吐 | 读平均延迟 | 读 p95 | 更新平均延迟 | 更新 p95 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| A | 50% read / 50% update | `382 ops/s` | `2.57 ms` | `4.59 ms` | `16.64 ms` | `23.34 ms` |
| B | 95% read / 5% update | `478 ops/s` | `3.26 ms` | `6.51 ms` | `40.64 ms` | `64.58 ms` |
| C | 100% read | `586 ops/s` | `2.40 ms` | `3.34 ms` | - | - |

整体趋势符合预期：读路径在 leader lease read 下比较轻，混入更新后瓶颈主要回到 `Raft commit -> apply -> LSM write` 这条写路径。

## 设计要点

### Raft 与读写路径

普通写入先进入 Raft，只有日志复制到多数派并 commit 后，ShardKV applier 才会按 log index 顺序写入 LSM。普通读优先使用 leader lease read：leader 确认当前 term 已提交过日志且近期获得多数派响应后，返回安全 `readIndex`；服务层等待 `lastApplied >= readIndex`，再用该 index 作为 MVCC snapshot 读本地 LSM。

### LSM 与 MVCC

LSM 的 MemTable 基于 SkipList，SSTable 使用有序 block、block meta 和 Bloom Filter 加速查询。前台写入 MemTable，冻结后由后台 flush；compaction 负责合并 SST，并结合 MVCC GC watermark 清理旧版本。为了事务 snapshot 读安全，GC 会保留 watermark 之后的版本，并为每个 key 保留必要的兜底旧版本。

### Snapshot 与恢复

Raft 负责持久化日志和 snapshot。ShardKV snapshot 会保存分片数据、迁移状态、去重表、配置、事务元数据和 prepared key。恢复时先从 snapshot 重建状态机和 LSM，再继续 replay snapshot 之后的 Raft 日志。

### 跨分片事务

跨 shard 写事务采用 2PC。Coordinator group 通过 Raft 记录全局事务状态和 participant 元数据；participant group 在 prepare 阶段校验读集、锁定写集并持久化 branch 状态；commit 阶段再真正 apply 写集。若 prepared branch 重启，会通过 coordinator 查询最终决议并补提交或回滚。

## 当前边界

- 对外 gRPC 事务接口目前只支持单 shard 事务。
- 普通跨 shard `Range` 是 fan-out + merge，不是全局原子快照；需要一致快照时应使用内部 `ReadTxn`。
- LSM 层本身没有独立 WAL，系统级已提交数据主要依赖 Raft log / snapshot 恢复。
- 当前启动命令偏本地演示和压测形态；要部署成真正多进程集群，还需要继续拆分配置、服务发现和节点生命周期管理。

## 来源说明

- Raft / ShardKV 基础框架来自 MIT 6.5840 分布式系统课程实验。
- LSM-Tree 实现参考了 [`tiny-lsm`](https://github.com/Vanilla-Beauty/tiny-lsm) 的设计思路，并结合本项目的 MVCC、事务和 snapshot 恢复路径做了改造。
