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
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrRepeated    = "ErrRepeated"
	ErrTimeout     = "ErrTimeout"
	ErrNotReady    = "ErrNotReady"
	ErrConfigNotReady = "ErrConfigNotReady"
	ErrConflict    = "ErrConflict"
)

const (
	GET    = "Get"
	PUT    = "Put"
	APPEND = "Append"
	RECONFIG = "Reconfig"
	INSERTSHARD  = "InsertShard"
	DELETESHARD  = "DeleteShard"
	TXNCOMMIT    = "TxnCommit"
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
	RPCID int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID int64
	RPCID int64
}

type GetReply struct {
	Err   Err
	Value string
}

type PullDataArgs struct {
	ConfigNum int	// 请求者想获得的配置版本
	ShardIndex int	// 请求者想获得的分片索引
}

type PullDataReply struct {
    ShardData map[string]string
	LastOpMap map[int64]OpResult
    Err   Err
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

