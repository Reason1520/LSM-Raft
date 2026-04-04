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
- 范围查询（单 shard 线性一致，跨 shard 聚合）
- gRPC 对外访问层（内部 RPC 可选 labrpc / gRPC 传输）

## 一致性与语义
- **单 shard Get/Put/Append/Range**：线性一致（通过 Raft log index 快照读）
- **跨 shard Range**：客户端逐 shard 查询并合并，不保证全局原子快照
- **事务**：单 shard 内提交原子性；跨 shard 事务不支持

## LSM 存储说明
- 每个 shard 对应一个 LSM 引擎实例
- 数据默认存放在系统临时目录  
  Windows: `%TEMP%\shardkv-<gid>-<me>\shard-<id>`
- Compaction 由后台队列驱动（通道触发 + 定时兜底）
- MVCC GC：Compaction 时根据 watermark 清理旧版本，每个 key 保留一个兜底旧版本

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

## 备注
- 测试仍基于 `labrpc`（网络可控），但已支持 gRPC 传输作为内部 RPC
- 真正跨进程部署时，使用 gRPC 传输启动各节点即可
- 跨 shard 事务需要额外协调协议（如 2PC）
