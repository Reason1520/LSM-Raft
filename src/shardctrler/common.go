package shardctrler

//
// Shard controller: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// NShards : The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.

type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK = "OK"
	ErrWrongGID = "ErrWrongGID"			// ErrGID错误，主要用于Join添加了重复的GID和Leave了不存在的GID
	ErrWrongLeader = "ErrWrongLeader"	// 当前节点不是Leader
	ErrRepeated = "ErrRepeated"			// 重复的请求
	ErrTimeout = "ErrTimeout"			// 请求超时
)

const (
	JOIN = "Join"
	LEAVE = "Leave"
	MOVE = "Move"
	QUERY = "Query"
)

type Err string

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
	ClientID int64
	RPCID int64
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs []int
	ClientID int64
	RPCID int64
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int
	ClientID int64
	RPCID int64
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number
	ClientID int64
	RPCID int64
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
