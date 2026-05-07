package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                = "OK"
	ErrNoKey          = "ErrNoKey"
	ErrWrongGroup     = "ErrWrongGroup"
	ErrWrongLeader    = "ErrWrongLeader"
	ErrRepeated       = "ErrRepeated"
	ErrTimeout        = "ErrTimeout"
	ErrNotReady       = "ErrNotReady"
	ErrConfigNotReady = "ErrConfigNotReady"
	ErrConflict       = "ErrConflict"
)

const (
	GET              = "Get"
	PUT              = "Put"
	APPEND           = "Append"
	PUTAPPENDBATCH   = "PutAppendBatch"
	RANGE            = "Range"
	RECONFIG         = "Reconfig"
	INSERTSHARD      = "InsertShard"
	DELETESHARD      = "DeleteShard"
	TXNCOMMIT        = "TxnCommit"
	TXNBEGIN         = "TxnBegin"
	TXNCOORDBEGIN    = "TxnCoordBegin"
	TXNCOORDENLIST   = "TxnCoordEnlist"
	TXNBRANCHBEGIN   = "TxnBranchBegin"
	TXNCOORDPREPARE  = "TxnCoordPrepare"
	TXNCOORDABORT    = "TxnCoordAbort"
	TXNCOORDCOMMIT   = "TxnCoordCommit"
	TXNCOORDFINISH   = "TxnCoordFinish"
	TXNBRANCHPREPARE = "TxnBranchPrepare"
	TXNBRANCHABORT   = "TxnBranchAbort"
	TXNBRANCHCOMMIT  = "TxnBranchCommit"
)

type Err string

// IsolationLevel 定义事务隔离级别。
// 当前实现主要用 RepeatableRead/Serializable 在提交或 prepare 阶段做读集版本校验。
type IsolationLevel int

const (
	ReadUncommitted IsolationLevel = iota // 读未提交；当前事务路径基本不依赖该级别。
	ReadCommitted                         // 读已提交；提交时不校验历史读集版本。
	RepeatableRead                        // 可重复读；提交/prepare 时校验读过的 key 版本未变化。
	Serializable                          // 串行化；当前实现与 RepeatableRead 类似，预留更强校验语义。
)

// PutAppendArgs : Put or Append arguments.
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID int64
	RPCID    int64
}

type PutAppendReply struct {
	Err Err
}

type BatchedWrite struct {
	Type     string
	Key      string
	Value    string
	ClientID int64
	RPCID    int64
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID int64
	RPCID    int64
}

type GetReply struct {
	Err   Err
	Value string
}

type KeyValue struct {
	Key   string
	Value string
}

type RangeArgs struct {
	Start    string
	End      string // empty means open-ended
	Limit    int    // 0 means no limit
	ShardID  int    // target shard
	ClientID int64
	RPCID    int64
}

type RangeReply struct {
	Err Err
	KVs []KeyValue
}

type PullDataArgs struct {
	ConfigNum  int // 请求者想获得的配置版本
	ShardIndex int // 请求者想获得的分片索引
}

type PullDataReply struct {
	ShardData map[string]string
	LastOpMap map[int64]OpResult
	Err       Err
}

// Transaction RPCs (single-shard).
// 单分片事务沿用一条 Raft 日志作为提交边界：Begin 固定 snapshot，
// Get/Range 在 snapshot 上读，Commit 携带读写集进入目标 shard group 的 Raft。
type TxnBeginArgs struct {
	ClientID  int64          // 客户端 ID，用于请求去重。
	RPCID     int64          // 客户端单调递增 RPC ID，用于幂等处理。
	Isolation IsolationLevel // 本事务使用的隔离级别。
}

type TxnBeginReply struct {
	Err      Err
	TxnID    uint64 // server 分配的事务 ID。
	Snapshot uint64 // begin 对应的 Raft log index，作为 MVCC 读快照版本。
}

type TxnGetArgs struct {
	Key      string // 要读取的 key。
	Snapshot uint64 // 从哪个 MVCC 版本视角读取。
	TxnID    uint64 // 事务 ID，用于刷新活跃事务时间和保护 GC。
	ClientID int64
	RPCID    int64
}

type TxnGetReply struct {
	Err     Err
	Value   string
	Version uint64 // 实际读到的 key 版本，之后放入读集用于冲突检测。
}

type TxnWrite struct {
	Key    string // 写入或删除的 key。
	Value  string // Delete=false 时要写入的值。
	Delete bool   // true 表示删除该 key。
}

type TxnRead struct {
	Key     string // 事务曾经读过的 key。
	Version uint64 // 当时读到的版本；0 通常表示读到不存在。
}

type TxnCommitArgs struct {
	TxnID     uint64
	ClientID  int64
	RPCID     int64
	Isolation IsolationLevel
	Writes    []TxnWrite // 本事务缓存在客户端的写集。
	Reads     []TxnRead  // 本事务读过的 key/version，用于提交前校验。
}

type TxnCommitReply struct {
	Err Err
}

type TxnAbortArgs struct {
	TxnID    uint64 // 要清理的事务 ID。
	ClientID int64
	RPCID    int64
}

type TxnAbortReply struct {
	Err Err
}

// Cross-shard transaction RPCs.
// 跨分片写事务采用客户端编排的 2PC：
// coordinator group 记录全局事务状态，participant group 记录本地 branch 状态。
// 所有跨分片 RPC 都携带固定的 ConfigNum，避免事务过程中 shard ownership 漂移。
type TxnCoordBeginArgs struct {
	ClientID  int64
	RPCID     int64
	Isolation IsolationLevel // 全局事务隔离级别。
	ConfigNum int            // begin 时固定的 shardctrler 配置版本。
	AnchorKey string         // 用于选择 coordinator group 的锚点 key。
}

type TxnCoordBeginReply struct {
	Err       Err
	TxnID     uint64 // coordinator group 分配的全局事务 ID。
	CoordGID  int    // 本事务的 coordinator 所在 gid。
	ConfigNum int    // coordinator 接受的配置版本。
}

type TxnCoordEnlistArgs struct {
	TxnID     uint64 // 要注册 participant 的事务。
	ClientID  int64
	RPCID     int64
	ConfigNum int    // 必须等于 coordinator 当前事务记录里的配置版本。
	GID       int    // participant group gid。
	ShardID   int    // participant 负责的 shard。
	Snapshot  uint64 // participant branch begin 时拿到的本地 snapshot。
}

type TxnCoordEnlistReply struct {
	Err Err
}

type TxnBranchBeginArgs struct {
	TxnID     uint64 // 全局事务 ID。
	ClientID  int64
	RPCID     int64
	Isolation IsolationLevel // branch 继承的隔离级别。
	ConfigNum int            // 固定配置版本。
	CoordGID  int            // 该 branch 对应的 coordinator group。
	ShardID   int            // 要在本 group 上开启 branch 的 shard。
}

type TxnBranchBeginReply struct {
	Err      Err
	Snapshot uint64 // participant 本地 branch 的 MVCC 读快照版本。
	GID      int    // 实际处理该 branch 的 group。
	ShardID  int    // 实际处理的 shard。
}

// CoordTxnParticipant 是 coordinator 视角下的 participant 元数据。
type CoordTxnParticipant struct {
	GID      int    // participant group。
	ShardID  int    // 该 participant 上的目标 shard。
	Snapshot uint64 // branch begin 时确定的本地 snapshot。
}

// CoordTxnState 是 coordinator 侧事务状态机。
type CoordTxnState int

const (
	CoordTxnBegun     CoordTxnState = iota // 已 begin，可能还在 enlist participant。
	CoordTxnPreparing                      // 已记录 prepare payload，等待或正在执行 branch prepare。
	CoordTxnCommitted                      // commit 决议已写入 coordinator Raft。
	CoordTxnAborted                        // abort 决议已写入 coordinator Raft。
)

// BranchTxnState 是 participant 侧 branch 状态机。
type BranchTxnState int

const (
	BranchTxnBegun    BranchTxnState = iota // branch 已开启，可以在 snapshot 上读写缓冲。
	BranchTxnPrepared                       // branch 已通过 prepare，写集已被 preparedKeys 锁住。
)

// CoordTxnBranchRecord 是 coordinator 持久化的一份 branch prepare payload。
// 恢复时 coordinator 可用它知道这笔事务涉及哪些 branch 以及当时准备提交的读写集。
type CoordTxnBranchRecord struct {
	GID      int        // branch 所属 participant group。
	ShardID  int        // branch 负责的 shard。
	Snapshot uint64     // branch 的读快照。
	Reads    []TxnRead  // branch 读集。
	Writes   []TxnWrite // branch 写集。
	Prepared bool       // 预留字段；当前实现主要由 participant 自己记录 prepared 状态。
}

// CoordTxnRecord 是 coordinator group 上持久化的全局事务记录。
type CoordTxnRecord struct {
	TxnID        uint64                       // 全局事务 ID。
	ConfigNum    int                          // 事务固定的配置版本。
	Isolation    IsolationLevel               // 全局事务隔离级别。
	CoordGID     int                          // coordinator group。
	AnchorShard  int                          // anchorKey 对应的 shard。
	State        CoordTxnState                // coordinator 状态。
	Participants map[int]CoordTxnParticipant  // gid -> participant 元数据。
	Branches     map[int]CoordTxnBranchRecord // gid -> branch prepare payload。
}

// BranchTxnRecord 是 participant group 上持久化的本地事务分支记录。
type BranchTxnRecord struct {
	TxnID     uint64         // 全局事务 ID。
	ConfigNum int            // 固定配置版本。
	CoordGID  int            // 对应 coordinator group。
	GID       int            // 当前 participant group。
	ShardID   int            // 当前 branch 管理的 shard。
	Snapshot  uint64         // branch begin 时确定的 MVCC snapshot。
	Isolation IsolationLevel // branch 隔离级别。
	State     BranchTxnState // participant 状态。
	Reads     []TxnRead      // prepare 时保存的读集。
	Writes    []TxnWrite     // prepare 时保存的写集，commit 时真正落盘。
	Prepared  bool           // true 表示写集已经加锁，恢复 worker 需要查询 coordinator 决议。
}

// TxnCoordPrepareArgs 把一个 branch 的 prepare payload 先记录到 coordinator。
type TxnCoordPrepareArgs struct {
	TxnID     uint64 // 全局事务 ID。
	ClientID  int64
	RPCID     int64
	ConfigNum int        // 固定配置版本。
	GID       int        // branch 所属 participant group。
	ShardID   int        // branch 所属 shard。
	Snapshot  uint64     // branch snapshot。
	Reads     []TxnRead  // branch 读集。
	Writes    []TxnWrite // branch 写集。
}

type TxnCoordPrepareReply struct {
	Err Err
}

type TxnCoordAbortArgs struct {
	TxnID     uint64 // 要 abort 的全局事务。
	ClientID  int64
	RPCID     int64
	ConfigNum int // 固定配置版本。
}

type TxnCoordAbortReply struct {
	Err Err
}

type TxnCoordCommitArgs struct {
	TxnID     uint64 // 要 commit 的全局事务。
	ClientID  int64
	RPCID     int64
	ConfigNum int // 固定配置版本。
}

type TxnCoordCommitReply struct {
	Err Err
}

type TxnCoordFinishArgs struct {
	TxnID     uint64 // 所有 participant 已完成后要清理的事务。
	ClientID  int64
	RPCID     int64
	ConfigNum int // 固定配置版本。
}

type TxnCoordFinishReply struct {
	Err Err
}

// TxnCoordStatus 用于 participant 恢复：prepared branch 重启后查询 coordinator 最终决议。
type TxnCoordStatusArgs struct {
	TxnID     uint64 // 要查询的全局事务。
	ConfigNum int    // 固定配置版本。
}

type TxnCoordStatusReply struct {
	Err   Err
	State CoordTxnState // coordinator 当前状态，Committed/Aborted 会驱动 branch 补提交/回滚。
}

// TxnBranchPrepareArgs 是 participant 的 prepare 请求。
// participant 会校验读集、检查 preparedKeys 写锁冲突，通过后保存写集并锁住 key。
type TxnBranchPrepareArgs struct {
	TxnID     uint64 // 全局事务 ID。
	ClientID  int64
	RPCID     int64
	ConfigNum int            // 固定配置版本。
	CoordGID  int            // coordinator group。
	ShardID   int            // participant 本地 shard。
	Snapshot  uint64         // 必须匹配 branch begin 返回的 snapshot。
	Isolation IsolationLevel // 用于决定是否校验读集版本。
	Reads     []TxnRead      // 待校验读集。
	Writes    []TxnWrite     // 待锁定写集。
}

type TxnBranchPrepareReply struct {
	Err Err
}

type TxnBranchAbortArgs struct {
	TxnID     uint64 // 要回滚的 branch 所属全局事务。
	ClientID  int64
	RPCID     int64
	ConfigNum int // 固定配置版本。
	ShardID   int // branch 所属 shard。
}

type TxnBranchAbortReply struct {
	Err Err
}

type TxnBranchCommitArgs struct {
	TxnID     uint64 // 要提交的 branch 所属全局事务。
	ClientID  int64
	RPCID     int64
	ConfigNum int // 固定配置版本。
	ShardID   int // branch 所属 shard。
}

type TxnBranchCommitReply struct {
	Err Err
}

type TxnRangeArgs struct {
	Start    string // 范围起点，闭区间。
	End      string // 范围终点，开区间；空字符串表示无上界。
	Limit    int    // 最多返回数量；0 表示不限。
	ShardID  int    // 本次 range 只访问的目标 shard。
	Snapshot uint64 // MVCC 读快照。
	TxnID    uint64 // 事务 ID，用于刷新活跃事务时间。
	ClientID int64
	RPCID    int64
}

type TxnRangeKV struct {
	Key     string
	Value   string
	Version uint64 // 该 key 在 snapshot 下读到的版本，用于加入读集。
}

type TxnRangeReply struct {
	Err Err
	KVs []TxnRangeKV
}
