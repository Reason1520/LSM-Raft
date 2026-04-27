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

// IsolationLevel defines transaction isolation level.
type IsolationLevel int

const (
	ReadUncommitted IsolationLevel = iota
	ReadCommitted
	RepeatableRead
	Serializable
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
type TxnBeginArgs struct {
	ClientID  int64
	RPCID     int64
	Isolation IsolationLevel
}

type TxnBeginReply struct {
	Err      Err
	TxnID    uint64
	Snapshot uint64
}

type TxnGetArgs struct {
	Key      string
	Snapshot uint64
	TxnID    uint64
	ClientID int64
	RPCID    int64
}

type TxnGetReply struct {
	Err     Err
	Value   string
	Version uint64
}

type TxnWrite struct {
	Key    string
	Value  string
	Delete bool
}

type TxnRead struct {
	Key     string
	Version uint64
}

type TxnCommitArgs struct {
	TxnID     uint64
	ClientID  int64
	RPCID     int64
	Isolation IsolationLevel
	Writes    []TxnWrite
	Reads     []TxnRead
}

type TxnCommitReply struct {
	Err Err
}

type TxnAbortArgs struct {
	TxnID    uint64
	ClientID int64
	RPCID    int64
}

type TxnAbortReply struct {
	Err Err
}

// Cross-shard transaction RPCs (phase 1: coordinator + branch begin skeleton).
type TxnCoordBeginArgs struct {
	ClientID  int64
	RPCID     int64
	Isolation IsolationLevel
	ConfigNum int
	AnchorKey string
}

type TxnCoordBeginReply struct {
	Err       Err
	TxnID     uint64
	CoordGID  int
	ConfigNum int
}

type TxnCoordEnlistArgs struct {
	TxnID     uint64
	ClientID  int64
	RPCID     int64
	ConfigNum int
	GID       int
	ShardID   int
	Snapshot  uint64
}

type TxnCoordEnlistReply struct {
	Err Err
}

type TxnBranchBeginArgs struct {
	TxnID     uint64
	ClientID  int64
	RPCID     int64
	Isolation IsolationLevel
	ConfigNum int
	CoordGID  int
	ShardID   int
}

type TxnBranchBeginReply struct {
	Err      Err
	Snapshot uint64
	GID      int
	ShardID  int
}

type CoordTxnParticipant struct {
	GID      int
	ShardID  int
	Snapshot uint64
}

type CoordTxnState int

const (
	CoordTxnBegun CoordTxnState = iota
	CoordTxnPreparing
	CoordTxnCommitted
	CoordTxnAborted
)

type BranchTxnState int

const (
	BranchTxnBegun BranchTxnState = iota
	BranchTxnPrepared
)

type CoordTxnBranchRecord struct {
	GID      int
	ShardID  int
	Snapshot uint64
	Reads    []TxnRead
	Writes   []TxnWrite
	Prepared bool
}

type CoordTxnRecord struct {
	TxnID        uint64
	ConfigNum    int
	Isolation    IsolationLevel
	CoordGID     int
	AnchorShard  int
	State        CoordTxnState
	Participants map[int]CoordTxnParticipant
	Branches     map[int]CoordTxnBranchRecord
}

type BranchTxnRecord struct {
	TxnID     uint64
	ConfigNum int
	CoordGID  int
	GID       int
	ShardID   int
	Snapshot  uint64
	Isolation IsolationLevel
	State     BranchTxnState
	Reads     []TxnRead
	Writes    []TxnWrite
	Prepared  bool
}

type TxnCoordPrepareArgs struct {
	TxnID     uint64
	ClientID  int64
	RPCID     int64
	ConfigNum int
	GID       int
	ShardID   int
	Snapshot  uint64
	Reads     []TxnRead
	Writes    []TxnWrite
}

type TxnCoordPrepareReply struct {
	Err Err
}

type TxnCoordAbortArgs struct {
	TxnID     uint64
	ClientID  int64
	RPCID     int64
	ConfigNum int
}

type TxnCoordAbortReply struct {
	Err Err
}

type TxnCoordCommitArgs struct {
	TxnID     uint64
	ClientID  int64
	RPCID     int64
	ConfigNum int
}

type TxnCoordCommitReply struct {
	Err Err
}

type TxnCoordFinishArgs struct {
	TxnID     uint64
	ClientID  int64
	RPCID     int64
	ConfigNum int
}

type TxnCoordFinishReply struct {
	Err Err
}

type TxnCoordStatusArgs struct {
	TxnID     uint64
	ConfigNum int
}

type TxnCoordStatusReply struct {
	Err   Err
	State CoordTxnState
}

type TxnBranchPrepareArgs struct {
	TxnID     uint64
	ClientID  int64
	RPCID     int64
	ConfigNum int
	CoordGID  int
	ShardID   int
	Snapshot  uint64
	Isolation IsolationLevel
	Reads     []TxnRead
	Writes    []TxnWrite
}

type TxnBranchPrepareReply struct {
	Err Err
}

type TxnBranchAbortArgs struct {
	TxnID     uint64
	ClientID  int64
	RPCID     int64
	ConfigNum int
	ShardID   int
}

type TxnBranchAbortReply struct {
	Err Err
}

type TxnBranchCommitArgs struct {
	TxnID     uint64
	ClientID  int64
	RPCID     int64
	ConfigNum int
	ShardID   int
}

type TxnBranchCommitReply struct {
	Err Err
}

type TxnRangeArgs struct {
	Start    string
	End      string
	Limit    int
	ShardID  int
	Snapshot uint64
	TxnID    uint64
	ClientID int64
	RPCID    int64
}

type TxnRangeKV struct {
	Key     string
	Value   string
	Version uint64
}

type TxnRangeReply struct {
	Err Err
	KVs []TxnRangeKV
}
