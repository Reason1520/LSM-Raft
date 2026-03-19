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
)

const (
	GET    = "Get"
	PUT    = "Put"
	APPEND = "Append"
	RECONFIG = "Reconfig"
	INSERTSHARD  = "InsertShard"
	DELETESHARD  = "DeleteShard"
)

type Err string

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

