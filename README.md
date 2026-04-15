# ShardKV + LSMTree 项目说明

本项目实现了一个基于 **Raft + LSM-Tree** 的分片 KV 数据库，并在 `shardkv` 层支持**单 shard 事务**。  
内部 RPC 传输**可选**：`labrpc`（测试/内存网络）或 **gRPC 传输层**（真实网络），对外访问提供 gRPC 接口。  
`shardkv` 基于 MIT 6.5840 的 lab5 改进，LSM 参考 https://github.com/Vanilla-Beauty/tiny-lsm 实现。

**架构示意**
```
外部客户端
   |
   | gRPC
   v
gRPC Server (访问层)
   |
   | Clerk (内部 RPC: labrpc 或 gRPC 传输)
   v
ShardKV + Raft (一致性层)
   |
   v
LSM-Tree (存储层)
```

## 目录结构
- `src/shardkv`: 分片 KV 服务（Raft 应用层逻辑、迁移、快照、事务）
- `src/lsm`: LSM-Tree 存储引擎（SST、Compaction、Bloom、MVCC）
- `src/shardkvpb`: ShardKV gRPC proto 与生成代码
- `src/shardkvgrpc`: gRPC 客户端封装（对外使用）
- `src/raft`: Raft 共识实现
- `src/shardctrler`: 分片控制器
- `src/labrpc`: 内存网络 RPC（实验环境）
- `src/labrpcpb`: labrpc gRPC 传输层 proto（内部 RPC 可选）
- `src/labgob`: 课程提供的序列化工具
- `src/cmd/shardkv_demo`: in-process 交互式 demo
- `src/cmd/shardkv_grpc`: gRPC 服务启动入口（内部 RPC 使用 gRPC 传输）

## 功能概览
- 分片 KV（可迁移、快照）
- LSM-Tree 存储（多层 SST + Compaction）
- MVCC（版本号为 Raft 日志 index）
- 单 shard 事务（ReadCommitted / RepeatableRead / Serializable）
- leader lease 普通只读快路径（Get/Range）
- 范围查询（单 shard 线性一致，跨 shard 聚合）
- gRPC 对外访问层（内部 RPC 可选 labrpc / gRPC 传输）

## 一致性与语义
- **单 shard Get/Range**：线性一致；leader 优先走基于最近多数派确认的 lease read，本地按 `commitIndex` 快照读取，lease 不可用时回退到原 Raft 日志路径
- **单 shard Put/Append**：线性一致（经 Raft 提交后写入状态机）
- **跨 shard Range**：客户端逐 shard 查询并合并，不保证全局原子快照
- **事务**：单 shard 内提交原子性；跨 shard 事务不支持

## LSM 存储说明
- 每个 shard 对应一个 LSM 引擎实例
- 数据默认存放在系统临时目录  
  Windows: `%TEMP%\shardkv-<gid>-<me>\shard-<id>`
- Compaction 由后台队列驱动（通道触发 + 定时兜底）
- MVCC GC：Compaction 时根据 watermark 清理旧版本，每个 key 保留一个兜底旧版本

## 近期优化
- 普通 `Get/Range`：增加 leader lease 只读快路径，把大量只读请求从 Raft 主路径摘出
- Raft 复制：使用 per-follower replicator，并支持 batching / pipelining
- 客户端路由：按 `gid` 缓存最近 leader，减少无效探测
- 存储写路径：memtable 冻结后后台 flush，compaction 异步执行
- `ShardKV applier`：事务提交和 shard 导入改成批量写 LSM，减少逐条写入开销
- Snapshot：构建和编码下沉到后台 worker，前台只排队最新快照任务
- Benchmark：修正吞吐统计口径，并补充本地只读延迟测试

## 运行方式
### 1. 启动 in-process Demo（内部 labrpc）
在 `MIT6.5840/src` 目录下：
```bash
go run ./cmd/shardkv_demo
```
默认启动 1 组 3 副本，可用 `-n` 指定副本数：
```bash
go run ./cmd/shardkv_demo -n 1
```

### 2. 启动 gRPC 服务（对外访问 + 内部 gRPC 传输）
```bash
go run ./cmd/shardkv_grpc -addr :50051 -n 3
```

## gRPC 客户端使用（推荐）
项目内提供轻量封装：`src/shardkvgrpc`
```go
cli, _ := shardkvgrpc.Dial("127.0.0.1:50051")
defer cli.Close()

_ = cli.Put("a1", "v1")
v, _ := cli.Get("a1")

tx, _ := cli.BeginTxn(shardkv.RepeatableRead, "a1")
_ = tx.Put("a2", "v2")
_, _ = tx.Range("a", "a~", 0)
_ = tx.Commit()
```
> `BeginTxn` 需要 `keyHint` 确定 shard；事务内 key 必须落在同一 shard。

## gRPC Proto
`src/shardkvpb/shardkv.proto`

## 内部 RPC 传输选择
- **labrpc（默认测试）**：内存网络，可模拟丢包/延迟，测试依赖
- **gRPC 传输（真实网络）**：通过 `labrpcpb` + gRPC 封装，不影响上层代码

## 测试
在 `MIT6.5840/src` 目录下执行：
```bash
go test ./shardkv -v
go test ./lsm/tests -v
go test ./lsm -v
```

常用回归：
```bash
go test ./shardkv -run "TestSnapshot5B|TestTxn|TestRange" -count=1
go test ./raft -run "TestBasicAgree3B|TestRejoin3B" -count=1
```

本地性能回归：
```bash
go test ./shardkv -run TestResumeWriteThroughputLocal -count=1 -v
go test ./shardkv -run TestResumePointReadLatencyLocal -count=1 -v
```

## 基准测试与日志控制
基准测试示例（仅跑 Benchmark，不跑单元测试）：
```bash
go test ./shardkv -run ^$ -bench BenchmarkShardKVPut -benchtime=15s -v -count=1
```

日志开关（环境变量）：
- `BENCH_PROGRESS=1`：输出基准测试进度/QPS（默认 5s 一次）
- `BENCH_DEBUG=1`：输出更详细的调试日志（Raft/ShardCtrler/labrpc 等）

示例：
```bash
BENCH_PROGRESS=1 go test ./shardkv -run ^$ -bench BenchmarkShardKVPut -benchtime=15s -v -count=1
BENCH_DEBUG=1 go test ./shardkv -run ^$ -bench BenchmarkShardKVPut -benchtime=15s -v -count=1
```

更多 Benchmark 示例：
```bash
# ShardKV（内部 labrpc）
go test ./shardkv -run ^$ -bench BenchmarkShardKVPut -benchtime=15s -v -count=1
go test ./shardkv -run ^$ -bench BenchmarkShardKVPutParallel -benchtime=15s -v -count=1
go test ./shardkv -run ^$ -bench BenchmarkShardKVGet -benchtime=15s -v -count=1
go test ./shardkv -run ^$ -bench BenchmarkShardKVRange -benchtime=15s -v -count=1

# gRPC 对外客户端（Put+Get 混合）
go test ./shardkv -run ^$ -bench BenchmarkGRPCPutGet -benchtime=15s -v -count=1
```

Benchmark 方法说明：
- `BenchmarkShardKVPut`：3 节点 demo cluster，1024 个固定 key，计时后循环单次 `Put` 覆盖写
- `BenchmarkShardKVPutParallel`：3 节点 demo cluster，`b.RunParallel` 为每个 worker 创建独立 Clerk，并发执行 `Put`
- `BenchmarkShardKVGet`：先预热写入 1024 个 key，计时后循环单次 `Get`
- `BenchmarkShardKVRange`：先预热 5000 个连续 `a` 前缀 key，计时后循环做完整前缀 `Range("a", "a~", 0)`
- `BenchmarkGRPCPutGet`：3 节点 gRPC cluster + 对外 gRPC server，计时后每轮做一次 `Put` 和一次 `Get`

说明：
- `BenchmarkShardKVPut/Get/Range` 主要反映单请求平均成本和改动前后对比
- `BenchmarkShardKVPutParallel` 是并发 benchmark，反映多 worker 同时打 3 节点集群时的顺序 `Put` 吞吐
- 真正回答“并发写吞吐”时，仍建议同时引用 `TestResumeWriteThroughputLocal` 这类可控多 worker 压测

本机参考结果：
`2026-04-15`，`Windows / Ryzen 7 5800H`，仅供本地对比，不代表跨机器绝对指标。

- 三节点写吞吐：`567 ops/s`
- 点读延迟：`mean 83us / p95 534us / p99 1.011ms`
- `BenchmarkShardKVPut`：`144.1 ops/s`
- `BenchmarkShardKVPutParallel`：`451.5 ops/s`
- `BenchmarkShardKVGet`：`14637 ops/s`
- `BenchmarkShardKVRange`：`165.3 ops/s`

## 备注
- 测试仍基于 `labrpc`（网络可控），但已支持 gRPC 传输作为内部 RPC
- 真正跨进程部署时，使用 gRPC 传输启动各节点即可
- 跨 shard 事务需要额外协调协议（如 2PC）
