# ShardKV + LSMTree 项目说明

本仓库实现了基于 Go 实现的分布式 LSM-tree 键值存储系统。同时在 `shardkv` 层实现了**单 shard 事务**，以 Raft 日志为事务提交载体。`shardkv` 部分在基于MIT 6.5840 课程的 lab5 的基础上改进，` LSM-tree` 部分参考 https://github.com/Vanilla-Beauty/tiny-lsm 实现。

## 目录结构
- `src/shardkv`: 分片 KV 服务（基于 Raft），已接入 LSM
- `src/lsm`: LSM-Tree 存储引擎实现
- `src/raft` / `src/shardctrler` / `src/labrpc` / `src/labgob`: 课程提供的基础设施

## LSM 存储说明
- 每个 shard 对应一个 LSM 实例
- LSM 数据默认存放在系统临时目录：
  - Windows: `%TEMP%\shardkv-<gid>-<me>\shard-<id>`
- 为避免测试间残留数据影响，**在无持久 Raft 状态且无 snapshot 时，会清理当前节点对应目录**
- Compaction 由后台队列驱动（通道触发 + 定时兜底），避免“合并漏触发”
- MVCC 垃圾回收：Compaction 时会根据 watermark 清理旧版本，仅保留每个 key 的一个兜底旧版本

## 事务功能（单 shard）
事务实现位于 `shardkv` 层，**仅支持单 shard 内的事务**。
核心思想：**一次事务提交 = 一条 Raft 日志**。

### 事务特性
- 事务操作在客户端缓存
- Commit 时将读写集合打包成 `TxnCommit` 日志
- Apply 阶段执行冲突检测与原子写入
- 写入使用 Raft 日志 index 作为版本号（trancID）

### 支持的隔离级别
- `ReadCommitted`
- `RepeatableRead`
- `Serializable`

### 事务使用示例（客户端）
```go
tx := ck.BeginTxn(shardkv.RepeatableRead)
tx.Put("a1", "v1")
v, ok := tx.Get("a1")
_ = v; _ = ok
ok = tx.Commit() // false 表示冲突
```

### 事务范围查询（TxnRange）
事务内范围查询走同一 snapshot，保证和 Txn.Get 一致：
```go
tx := ck.BeginTxn(shardkv.RepeatableRead)
kv, ok := tx.Range("a", "a~", 0) // [a, a~) 同 shard 范围
_ = kv; _ = ok
```

### 跨 shard 事务说明
**不支持跨 shard 事务**。如果在同一事务中操作不同 shard 的 key，会直接失败。

## 设计细节
### 1. LSM 与 shardkv 的结合
- `shardkv` 对每个 shard 维护一个 LSM 实例。
- Raft apply 时将写入直接落到 LSM，引擎内部用 `trancID` 维护版本。
- 迁移时先导出 LSM 的最新可见版本到 `shadowDB`，再由新组导入。
 - Compaction 使用后台 worker + 触发队列，合并请求不会丢失。

### 2. 事务提交与一致性
- **一次提交即一条 Raft 日志**，确保所有副本以相同顺序应用。
- Apply 阶段才写入 LSM，保证跨副本一致。
- RepeatableRead/Serializable 下基于读集版本号进行冲突检测。
- 事务期间会维护活跃事务快照集合，用**最小快照**作为 LSM MVCC GC 的 watermark。

### 3. 快照与恢复
- Snapshot 不直接序列化 LSM 引擎，而是导出为 `map[int]map[string]string` 后编码。
- 恢复时重建 LSM 并批量写入数据。

### 4. 版本号策略
- 事务提交使用 Raft 日志 index 作为 `trancID`。
- 单次非事务 Put/Append 也使用 Raft index，保证单调递增。

### 5. 范围查询一致性
- **单 shard Range**：通过 Raft 日志索引快照读，线性一致。
- **跨 shard Range**：客户端逐 shard 查询并合并，保证每个 shard 线性一致，但不是全局原子快照。

## 接口说明
### 1. 基础 KV 接口
- `Get(key)`
- `Put(key, value)`
- `Append(key, value)`
- `Range(start, end, limit)`：跨 shard 范围查询（按 key 升序，`end==""` 表示开放区间）

### 2. 事务接口（单 shard）
客户端侧：
- `BeginTxn(level)` → 返回事务对象 `Txn`
- `Txn.Get(key)`
- `Txn.Range(start, end, limit)` → 事务快照范围查询（同 shard）
- `Txn.Put(key, value)`
- `Txn.Remove(key)`
- `Txn.Commit()` → `true` 表示提交成功，`false` 表示冲突或跨 shard
- `Txn.Abort()` → 丢弃本地缓存的写集合

RPC 接口：
- `TxnBegin`：申请事务 ID + snapshot 版本
- `TxnGet`：按 snapshot 读取并返回 `(value, version)`
- `TxnRange`：按 snapshot 范围读取并返回 `(key, value, version)`
- `TxnCommit`：提交读写集合，返回 `OK/ErrConflict/ErrWrongGroup`

### 3. 错误码
- `OK`：成功
- `ErrNoKey`：键不存在
- `ErrWrongGroup`：请求发到错误的 shard 组
- `ErrWrongLeader`：非 leader
- `ErrConflict`：事务冲突

## 编译与测试
在 `MIT6.5840/src` 目录下执行：

```bash
go test ./shardkv -v
```

运行事务测试：

```bash
go test ./shardkv -run TestTxn -v
```

运行范围查询测试：
```bash
go test ./shardkv -run TestRange -v
```

运行事务范围查询测试：
```bash
go test ./shardkv -run TestTxnRange -v
```

## 运行与使用（可执行 Demo）
> 说明：项目使用 MIT6.5840 的 `labrpc`（内存网络），**无法跨进程连接**。  
> 因此下面的“启动程序”会在同一进程内启动 shardctrler + shardkv 集群，并提供一个交互式客户端。

### 1. 启动 Demo 集群
在 `MIT6.5840/src` 目录下：
```bash
go run ./cmd/shardkv_demo
```
默认会启动 **1 个 shardkv 组、3 个副本**。可用 `-n` 指定副本数：
```bash
go run ./cmd/shardkv_demo -n 1
```

### 2. Demo 交互命令
启动后输入：
```
get <k>
put <k> <v>
append <k> <v>
range <start> <end|- > [limit]
txndemo
help
exit
```
其中 `range` 的 `end` 用 `-` 表示开放区间。

### 3. 在“另一个程序”里使用客户端（同进程示例）
下面是一个最小的可运行示例，直接启动集群并使用客户端：
```go
dc, ck := shardkv.StartDemoCluster(3)
defer dc.Close()

ck.Put("a1", "v1")
v := ck.Get("a1")

// 普通范围查询（跨 shard 聚合）
kvs := ck.Range("a", "z", 0)

// 事务范围查询（同 shard）
tx := ck.BeginTxn(shardkv.RepeatableRead)
_ = tx.Put("a2", "v2")
kv, ok := tx.Range("a", "a~", 0)
_ = kv; _ = ok
_ = tx.Commit()
```

> 若希望“跨进程客户端/服务端”，需要将 `labrpc` 替换为真实网络 RPC（超出本项目范围）。

## 使用说明（最小示例）
```go
ck := cfg.makeClient(cfg.ctl)
ck.Put("a1", "v1")
ck.Put("b1", "v2")

// 普通范围查询（跨 shard 聚合）
res := ck.Range("a", "z", 0)

// 事务范围查询（同 shard）
tx := ck.BeginTxn(shardkv.RepeatableRead)
_ = tx.Put("a2", "v3")
kv, ok := tx.Range("a", "a~", 0)
_ = kv; _ = ok
_ = tx.Commit()
```

## 备注
当前事务实现没有接入 WAL，仅依赖 Raft 提交保证一致性与持久化。
如需跨 shard 事务，需要引入额外协调协议（如 2PC）。
