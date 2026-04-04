package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"
import "6.5840/shardctrler"
import "sort"
import "sync/atomic"
import "time"

// which shard is a key in?
// please use this function,
// and please do not change it.

func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

// Key2ShardForExternal exposes shard mapping for external clients (gRPC wrapper).
func Key2ShardForExternal(key string) int {
	return key2shard(key)
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm      *shardctrler.Clerk
	config  shardctrler.Config
	makeEnd func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	ClientID  int64
	nextRPCID int64
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.

func MakeClerk(ctrlers []*labrpc.ClientEnd, makeEnd func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.makeEnd = makeEnd
	// You'll have to add code here.
	ck.ClientID = nrand()
	ck.nextRPCID = 1
	return ck
}

func (ck *Clerk) allocRPCID() int64 {
	return atomic.AddInt64(&ck.nextRPCID, 1)
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.

func (ck *Clerk) Get(key string) string {
	val, _ := ck.GetWithErr(key)
	return val
}

// GetWithErr returns value and Err, distinguishing ErrNoKey from OK.
func (ck *Clerk) GetWithErr(key string) (string, Err) {
	args := GetArgs{}
	args.Key = key
	args.ClientID = ck.ClientID
	args.RPCID = ck.allocRPCID()

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.makeEnd(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey || reply.Err == ErrRepeated) {
					return reply.Value, reply.Err
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				// ... not ok, or ErrWrongLeader ErrTimeout
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controller for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return "", ErrTimeout
}

// shared by Put and Append.
// You will have to modify this function.

func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.ClientID = ck.ClientID
	args.RPCID = ck.allocRPCID()

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.makeEnd(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey || reply.Err == ErrRepeated) {
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader ErrTimeout
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controller for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

// Range returns key/value pairs in [start, end) within the shard of start.
// end == "" means open-ended. limit == 0 means no limit.
func (ck *Clerk) Range(start, end string, limit int) []KeyValue {
	for {
		cfg := ck.config
		perShardRPC := make([]int64, shardctrler.NShards)
		for i := 0; i < shardctrler.NShards; i++ {
			perShardRPC[i] = ck.allocRPCID()
		}

		all := make([]KeyValue, 0)
		needRefresh := false

		for shard := 0; shard < shardctrler.NShards; shard++ {
			gid := cfg.Shards[shard]
			servers, ok := cfg.Groups[gid]
			if !ok || gid == 0 || len(servers) == 0 {
				needRefresh = true
				break
			}

			args := RangeArgs{
				Start:    start,
				End:      end,
				Limit:    0, // global limit will be applied after merge
				ShardID:  shard,
				ClientID: ck.ClientID,
				RPCID:    perShardRPC[shard],
			}

			got := false
			for si := 0; si < len(servers); si++ {
				srv := ck.makeEnd(servers[si])
				var reply RangeReply
				ok := srv.Call("ShardKV.Range", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey || reply.Err == ErrRepeated) {
					all = append(all, reply.KVs...)
					got = true
					break
				}
				if ok && reply.Err == ErrWrongGroup {
					needRefresh = true
					break
				}
			}

			if needRefresh {
				break
			}
			if !got {
				needRefresh = true
				break
			}
		}

		if !needRefresh {
			sort.Slice(all, func(i, j int) bool {
				return all[i].Key < all[j].Key
			})
			if limit > 0 && len(all) > limit {
				all = all[:limit]
			}
			return all
		}

		time.Sleep(100 * time.Millisecond)
		ck.config = ck.sm.Query(-1)
	}
}
