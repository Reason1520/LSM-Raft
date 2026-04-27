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
import "sync"
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

func rangeTargetShards(start, end string) []int {
	if end != "" && start >= end {
		return nil
	}

	seen := make([]bool, shardctrler.NShards)
	out := make([]int, 0, shardctrler.NShards)
	addShard := func(b byte) {
		shard := int(b) % shardctrler.NShards
		if !seen[shard] {
			seen[shard] = true
			out = append(out, shard)
		}
	}

	lower := 0
	if len(start) > 0 {
		lower = int(start[0])
	}

	upper := 255
	includeUpper := true
	if len(end) > 0 {
		upper = int(end[0])
		includeUpper = len(end) > 1
	}

	if lower > upper {
		return nil
	}

	for b := lower; b <= upper; b++ {
		if len(end) > 0 && b == upper && !includeUpper {
			break
		}
		addShard(byte(b))
	}

	if len(out) == 0 && len(start) > 0 {
		addShard(start[0])
	}
	return out
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	mu             sync.RWMutex
	sm             *shardctrler.Clerk
	ctrlers        []*labrpc.ClientEnd
	config         shardctrler.Config
	makeEndFactory func(string) *labrpc.ClientEnd
	endMu          sync.Mutex
	ends           map[string]*labrpc.ClientEnd
	ClientID       int64
	nextRPCID      int64
	leaderHint     map[int]int
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
	ck.ctrlers = ctrlers
	ck.makeEndFactory = makeEnd
	ck.ClientID = nrand()
	ck.nextRPCID = 1
	ck.leaderHint = make(map[int]int)
	ck.ends = make(map[string]*labrpc.ClientEnd)
	ck.refreshConfig()
	return ck
}

func (ck *Clerk) Clone() *Clerk {
	cfg := ck.currentConfig()
	leaderHint := ck.snapshotLeaderHint()
	clone := &Clerk{
		sm:             shardctrler.MakeClerk(ck.ctrlers),
		ctrlers:        ck.ctrlers,
		config:         cfg,
		makeEndFactory: ck.makeEndFactory,
		ends:           make(map[string]*labrpc.ClientEnd),
		ClientID:       nrand(),
		nextRPCID:      1,
		leaderHint:     leaderHint,
	}
	return clone
}

func (ck *Clerk) Close() {
	ck.endMu.Lock()
	defer ck.endMu.Unlock()
	for name, end := range ck.ends {
		_ = end.Close()
		delete(ck.ends, name)
	}
}

func (ck *Clerk) allocRPCID() int64 {
	return atomic.AddInt64(&ck.nextRPCID, 1)
}

func (ck *Clerk) refreshConfig() {
	cfg := ck.sm.Query(-1)
	ck.mu.Lock()
	ck.config = cfg
	ck.mu.Unlock()
}

func (ck *Clerk) currentConfig() shardctrler.Config {
	ck.mu.RLock()
	defer ck.mu.RUnlock()
	return ck.config
}

func (ck *Clerk) snapshotLeaderHint() map[int]int {
	ck.mu.RLock()
	defer ck.mu.RUnlock()
	out := make(map[int]int, len(ck.leaderHint))
	for gid, hint := range ck.leaderHint {
		out[gid] = hint
	}
	return out
}

func (ck *Clerk) makeEnd(servername string) *labrpc.ClientEnd {
	ck.endMu.Lock()
	if end, ok := ck.ends[servername]; ok {
		ck.endMu.Unlock()
		return end
	}
	ck.endMu.Unlock()

	end := ck.makeEndFactory(servername)

	ck.endMu.Lock()
	defer ck.endMu.Unlock()
	if existing, ok := ck.ends[servername]; ok {
		_ = end.Close()
		return existing
	}
	ck.ends[servername] = end
	return end
}

func (ck *Clerk) serverOrder(gid int, n int) []int {
	order := make([]int, 0, n)
	if n <= 0 {
		return order
	}
	ck.mu.RLock()
	defer ck.mu.RUnlock()
	if hint, ok := ck.leaderHint[gid]; ok && hint >= 0 && hint < n {
		order = append(order, hint)
		for i := 0; i < n; i++ {
			if i != hint {
				order = append(order, i)
			}
		}
		return order
	}
	for i := 0; i < n; i++ {
		order = append(order, i)
	}
	return order
}

func (ck *Clerk) rememberLeader(gid, serverIdx int) {
	if serverIdx >= 0 {
		ck.mu.Lock()
		ck.leaderHint[gid] = serverIdx
		ck.mu.Unlock()
	}
}

func (ck *Clerk) forgetLeader(gid, serverIdx int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	if hint, ok := ck.leaderHint[gid]; ok && hint == serverIdx {
		delete(ck.leaderHint, gid)
	}
}

func (ck *Clerk) rangeOnShard(cfg shardctrler.Config, shard int, start, end string, limit int, rpcID int64) ([]KeyValue, bool, bool) {
	gid := cfg.Shards[shard]
	servers, ok := cfg.Groups[gid]
	if !ok || gid == 0 || len(servers) == 0 {
		return nil, false, true
	}

	args := RangeArgs{
		Start:    start,
		End:      end,
		Limit:    limit,
		ShardID:  shard,
		ClientID: ck.ClientID,
		RPCID:    rpcID,
	}

	for _, si := range ck.serverOrder(gid, len(servers)) {
		srv := ck.makeEnd(servers[si])
		var reply RangeReply
		ok := srv.Call("ShardKV.Range", &args, &reply)
		if ok && (reply.Err == OK || reply.Err == ErrNoKey || reply.Err == ErrRepeated) {
			ck.rememberLeader(gid, si)
			return reply.KVs, true, false
		}
		if ok && reply.Err == ErrWrongGroup {
			ck.forgetLeader(gid, si)
			return nil, false, true
		}
		ck.forgetLeader(gid, si)
	}

	return nil, false, false
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
		cfg := ck.currentConfig()
		shard := key2shard(key)
		gid := cfg.Shards[shard]
		if servers, ok := cfg.Groups[gid]; ok {
			// try each server for the shard.
			for _, si := range ck.serverOrder(gid, len(servers)) {
				srv := ck.makeEnd(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey || reply.Err == ErrRepeated) {
					ck.rememberLeader(gid, si)
					return reply.Value, reply.Err
				}
				if ok && (reply.Err == ErrWrongGroup) {
					ck.forgetLeader(gid, si)
					break
				}
				ck.forgetLeader(gid, si)
				// ... not ok, or ErrWrongLeader ErrTimeout
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controller for the latest configuration.
		ck.refreshConfig()
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
		cfg := ck.currentConfig()
		shard := key2shard(key)
		gid := cfg.Shards[shard]
		if servers, ok := cfg.Groups[gid]; ok {
			for _, si := range ck.serverOrder(gid, len(servers)) {
				srv := ck.makeEnd(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey || reply.Err == ErrRepeated) {
					ck.rememberLeader(gid, si)
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					ck.forgetLeader(gid, si)
					break
				}
				ck.forgetLeader(gid, si)
				// ... not ok, or ErrWrongLeader ErrTimeout
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controller for the latest configuration.
		ck.refreshConfig()
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
	targetShards := rangeTargetShards(start, end)
	if len(targetShards) == 0 {
		return nil
	}

	for {
		cfg := ck.currentConfig()
		if len(targetShards) == 1 {
			kvs, ok, needRefresh := ck.rangeOnShard(cfg, targetShards[0], start, end, limit, ck.allocRPCID())
			if ok {
				return kvs
			}
			if !needRefresh {
				time.Sleep(100 * time.Millisecond)
			}
			ck.refreshConfig()
			continue
		}

		all := make([]KeyValue, 0)
		needRefresh := false

		for _, shard := range targetShards {
			kvs, ok, refresh := ck.rangeOnShard(cfg, shard, start, end, 0, ck.allocRPCID())
			if refresh {
				needRefresh = true
				break
			}
			if !ok {
				needRefresh = true
				break
			}
			all = append(all, kvs...)
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
		ck.refreshConfig()
	}
}
