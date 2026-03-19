package shardkv

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

const (
	UpConfigLoopInterval = 100 * time.Millisecond
	GetShardsInterval    = 100 * time.Millisecond
	GCInterval           = 100 * time.Millisecond
)

// 分片状态
const (
	Serving   = iota // 正常服务
	Pulling          // 正在从其他组拉取数据
	BePulling        // 正在被其他组拉取（即在此配置中不属于我，但数据还在我这）
	GCing            // 等待清理
)

type Op struct {
	Type      string // "Get", "Put", "Append", "Reconfig", "InsertShard", "DeleteShard"
	Key       string
	Value     string
	ClientID  int64
	RPCID     int64
	Config    shardctrler.Config   // 用于 Reconfig
	ShardData map[string]string    // 用于 InsertShard
	LastOpMap map[int64]OpResult   // 用于 InsertShard，携带去重信息
	ShardID   int                  // 用于 DeleteShard/InsertShard
	ConfigNum int                  // 用于 DeleteShard/InsertShard 校验配置版本
}

type OpResult struct {
	Err   Err
	Value string
	RPCID int64
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	dead         int32

	mck          *shardctrler.Clerk
	config       shardctrler.Config
	lastConfig   shardctrler.Config
	persister	 *raft.Persister

	// 核心数据存储: shardID -> key -> value
	kvDB map[int]map[string]string

	// 影子存储: configNum -> shardID -> key -> value
	// 用于存储历史版本的数据，供其他组拉取
	shadowDB map[int]map[int]map[string]string

	// 分片状态: shardID -> status
	shardStatus map[int]int

	// 客户端去重: ClientID -> Last Op Result
	lastOps map[int64]OpResult

	// 通知 RPC 处理完成的 channel: Index -> Result Channel
	waitCh map[int]chan OpResult
}

// Check strictly if I can serve this key
func (kv *ShardKV) canServe(shard int) bool {
	return kv.config.Shards[shard] == kv.gid && (kv.shardStatus[shard] == Serving || kv.shardStatus[shard] == GCing)
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	shard := key2shard(args.Key)
	kv.mu.Lock()
	if !kv.canServe(shard) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		Type:     GET,
		Key:      args.Key,
		ClientID: args.ClientID,
		RPCID:    args.RPCID,
	}

	res := kv.startOp(op)
	reply.Err = res.Err
	reply.Value = res.Value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	shard := key2shard(args.Key)
	kv.mu.Lock()
	if !kv.canServe(shard) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		Type:     args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientID: args.ClientID,
		RPCID:    args.RPCID,
	}

	res := kv.startOp(op)
	reply.Err = res.Err
}

// 统一处理 Op 的提交和等待
func (kv *ShardKV) startOp(op Op) OpResult {
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return OpResult{Err: ErrWrongLeader}
	}

	kv.mu.Lock()
	ch := make(chan OpResult, 1)
	kv.waitCh[index] = ch
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.waitCh, index)
		kv.mu.Unlock()
	}()

	select {
	case res := <-ch:
		return res
	case <-time.After(500 * time.Millisecond):
		return OpResult{Err: ErrTimeout}
	}
}

// PullData RPC Handler: 其他组来拉取数据
func (kv *ShardKV) PullData(args *PullDataArgs, reply *PullDataReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.ConfigNum >= kv.config.Num {
		// 请求者的配置比我还新，我还没准备好数据
		reply.Err = ErrNotReady
		return
	}

	// 从 shadowDB 中查找对应配置版本的数据
	if shards, ok := kv.shadowDB[args.ConfigNum]; ok {
		if shardData, ok := shards[args.ShardIndex]; ok {
			// Deep Copy Data
			reply.ShardData = make(map[string]string)
			for k, v := range shardData {
				reply.ShardData[k] = v
			}
			// Deep Copy LastOps (Client deduplication map)
			reply.LastOpMap = make(map[int64]OpResult)
			for k, v := range kv.lastOps {
				reply.LastOpMap[k] = v
			}
			reply.Err = OK
			return
		}
	}

	// 如果找不到数据，可能是已经被 GC 了或者版本不对
	reply.Err = ErrNoKey 
}

// DeleteShard RPC Handler: 别的组已经拿到了数据，通知我可以删除了
func (kv *ShardKV) DeleteShard(args *PullDataArgs, reply *PullDataReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	// 只有当本组配置已经前进到请求配置之后，才能安全删除旧分片影子数据。
	// 否则不能返回 OK，否则对端会停止重试，导致 shadowDB 泄漏。
	if args.ConfigNum >= kv.config.Num {
		reply.Err = ErrNotReady
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// 这个操作也需要走 Raft，保证一致性
	op := Op{
		Type:      DELETESHARD,
		ConfigNum: args.ConfigNum,
		ShardID:   args.ShardIndex,
	}

	res := kv.startOp(op)
	reply.Err = res.Err
}

// Applier 协程: 处理 Raft ApplyMsg
func (kv *ShardKV) applier() {
	for msg := range kv.applyCh {
		if kv.killed() {
			return
		}

		if msg.CommandValid {
			kv.mu.Lock()
			op := msg.Command.(Op)
			index := msg.CommandIndex

			var res OpResult
			res.RPCID = op.RPCID

			// 处理不同类型的 Op
			switch op.Type {
			case PUT, APPEND:
				res = kv.applyPutAppend(op)
			case GET:
				res = kv.applyGet(op)
			case RECONFIG:
				res = kv.applyReconfig(op)
			case INSERTSHARD:
				res = kv.applyInsertShard(op)
			case DELETESHARD:
				res = kv.applyDeleteShard(op)
			}

			// 检查是否需要 Snapshot
			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
				kv.snapshot(index)
			}

			// 通知等待的 RPC Handler
			if ch, ok := kv.waitCh[index]; ok {
				// 只有当 Op 里的 ClientID 和 RPCID 匹配时才算当前请求成功
				// 但对于 Config/Migration 操作，通常不需要严格匹配 RPCID
				ch <- res
			}
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			kv.mu.Lock()
			kv.readSnapshot(msg.Snapshot)
			kv.mu.Unlock()
		}
	}
}

// --- Applier Helper Functions (Must hold lock) ---

func (kv *ShardKV) applyPutAppend(op Op) OpResult {
	shard := key2shard(op.Key)
	if !kv.canServe(shard) {
		return OpResult{Err: ErrWrongGroup}
	}

	// 幂等性检查：RPCID 单调递增，旧请求和重试请求都不能重复执行。
	if lastRes, ok := kv.lastOps[op.ClientID]; ok && op.RPCID <= lastRes.RPCID {
		return lastRes
	}

	if kv.kvDB[shard] == nil {
		kv.kvDB[shard] = make(map[string]string)
	}

	if op.Type == PUT {
		kv.kvDB[shard][op.Key] = op.Value
	} else {
		kv.kvDB[shard][op.Key] += op.Value
	}

	res := OpResult{Err: OK, RPCID: op.RPCID}
	kv.lastOps[op.ClientID] = res
	return res
}

func (kv *ShardKV) applyGet(op Op) OpResult {
	shard := key2shard(op.Key)
	if !kv.canServe(shard) {
		return OpResult{Err: ErrWrongGroup}
	}

	// Get 操作只读，不修改状态，但为了线性一致性，我们通常不缓存 Get 的结果给 LastOps
	// 除非需要严格的一次性语义，但 Lab 中 Get 即使重复执行也没副作用
	
	val, ok := kv.kvDB[shard][op.Key]
	if !ok {
		return OpResult{Err: ErrNoKey, RPCID: op.RPCID}
	}
	return OpResult{Err: OK, Value: val, RPCID: op.RPCID}
}

func (kv *ShardKV) applyReconfig(op Op) OpResult {
	// 只有当 Config 序号递增时才更新
	if op.Config.Num == kv.config.Num+1 {
		// 1. 更新 Config
		kv.lastConfig = kv.config
		kv.config = op.Config

		// 2. 更新分片状态
		// 遍历所有分片 (0-9)
		for i := 0; i < shardctrler.NShards; i++ {
			// 如果新配置里这个分片归我
			if kv.config.Shards[i] == kv.gid {
				// 如果旧配置里不归我，说明是新增的，需要 Pull
				// 唯一的特例是 config.Num = 1，初始状态，直接 Serving
				if kv.lastConfig.Shards[i] != kv.gid && kv.lastConfig.Num != 0 {
					kv.shardStatus[i] = Pulling
				} else {
					// 已经是我的或者初始分配，直接服务
					kv.shardStatus[i] = Serving
					if kv.kvDB[i] == nil {
						kv.kvDB[i] = make(map[string]string)
					}
				}
			} else {
				// 新配置里不归我
				if kv.lastConfig.Shards[i] == kv.gid {
					// 之前归我，现在不归我了 -> 移入 ShadowDB，等待别人来拉
					kv.shardStatus[i] = BePulling
					
					if kv.shadowDB[kv.lastConfig.Num] == nil {
						kv.shadowDB[kv.lastConfig.Num] = make(map[int]map[string]string)
					}
					// 移动数据，而不是复制，节省内存
					kv.shadowDB[kv.lastConfig.Num][i] = kv.kvDB[i]
					kv.kvDB[i] = nil // 清空当前 DB
				} else {
					// 之前不归我，现在也不归我
					// 保持原样 (可能还是 BePulling 或者无关)
				}
			}
		}
	}
	return OpResult{Err: OK}
}

func (kv *ShardKV) applyInsertShard(op Op) OpResult {
	// 只有在 Pulling 状态且配置版本匹配时才接受数据
	if op.ConfigNum == kv.config.Num && kv.shardStatus[op.ShardID] == Pulling {
		// 1. 存入数据
		kv.kvDB[op.ShardID] = make(map[string]string)
		for k, v := range op.ShardData {
			kv.kvDB[op.ShardID][k] = v
		}

		// 2. 合并去重表 (Max Logic)
		for clientId, otherRes := range op.LastOpMap {
			if localRes, ok := kv.lastOps[clientId]; !ok || otherRes.RPCID > localRes.RPCID {
				kv.lastOps[clientId] = otherRes
			}
		}

		// 3. 状态变更为 GCing (意味着可以服务了，但需要通知对方删除)
		// 为了简化，我们可以直接改为 Serving，并让后台协程去发送 DeleteShard
		kv.shardStatus[op.ShardID] = GCing
	}
	return OpResult{Err: OK}
}

func (kv *ShardKV) applyDeleteShard(op Op) OpResult {
	if op.ConfigNum < kv.config.Num {
		// 删除旧配置产生的数据
		if shards, ok := kv.shadowDB[op.ConfigNum]; ok {
			delete(shards, op.ShardID)
			if len(shards) == 0 {
				delete(kv.shadowDB, op.ConfigNum)
			}
		}
	}
	return OpResult{Err: OK}
}

// --- Background Tasks ---

// 1. 监控配置更新
func (kv *ShardKV) monitorConfig() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			// 只有当迁移流程结束后才拉取新配置。
			// 若在 GCing 时继续前进配置，会覆盖 lastConfig，导致后续 DeleteShard
			// 可能发往错误的来源组/版本，造成 shadowDB 残留。
			canNext := true
			for _, status := range kv.shardStatus {
				if status == Pulling || status == GCing {
					canNext = false
					break
				}
			}
			curNum := kv.config.Num
			kv.mu.Unlock()

			if canNext {
				// 查询下一个配置
				nextConfig := kv.mck.Query(curNum + 1)
				if nextConfig.Num == curNum+1 {
					// 提交 Reconfig
					kv.startOp(Op{
						Type:   RECONFIG,
						Config: nextConfig,
					})
				}
			}
		}
		time.Sleep(UpConfigLoopInterval)
	}
}

// 2. 监控分片拉取 (Pulling -> InsertShard)
func (kv *ShardKV) monitorMigration() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			var wg sync.WaitGroup
			
			for shardID, status := range kv.shardStatus {
				if status == Pulling {
					// 找到该分片在上一轮配置中的持有者
					gid := kv.lastConfig.Shards[shardID]
					servers := kv.lastConfig.Groups[gid]
					configNum := kv.lastConfig.Num // 我们要拉取的是生成于 lastConfig 的数据
					
					wg.Add(1)
					go func(sID, cNum int, gServers []string) {
						defer wg.Done()
						args := PullDataArgs{ConfigNum: cNum, ShardIndex: sID}
						
						// 轮询该组的所有服务器
						for _, server := range gServers {
							srv := kv.make_end(server)
							var reply PullDataReply
							if srv.Call("ShardKV.PullData", &args, &reply) && reply.Err == OK {
								// 拉取成功，提交 Raft
								kv.startOp(Op{
									Type:      INSERTSHARD,
									ConfigNum: kv.config.Num, // 这里的 ConfigNum 指当前我的配置
									ShardID:   sID,
									ShardData: reply.ShardData,
									LastOpMap: reply.LastOpMap,
								})
								return
							}
						}
					}(shardID, configNum, servers)
				}
			}
			kv.mu.Unlock()
			wg.Wait()
		}
		time.Sleep(GetShardsInterval)
	}
}

// 3. 监控垃圾回收 (GCing -> DeleteShard -> Serving)
func (kv *ShardKV) monitorGC() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			var wg sync.WaitGroup
			
			for shardID, status := range kv.shardStatus {
				if status == GCing {
					gid := kv.lastConfig.Shards[shardID]
					servers := kv.lastConfig.Groups[gid]
					configNum := kv.lastConfig.Num
					
					wg.Add(1)
					go func(sID, cNum int, gServers []string) {
						defer wg.Done()
						args := PullDataArgs{ConfigNum: cNum, ShardIndex: sID}
						var reply PullDataReply
						
						// 通知原持有者删除数据
						for _, server := range gServers {
							srv := kv.make_end(server)
							if srv.Call("ShardKV.DeleteShard", &args, &reply) && reply.Err == OK {
								// 对方删除成功后，我这里更新状态为 Serving
								// 这里不需要走 Raft，因为 GCing -> Serving 只是本地状态变更，数据已经在 InsertShard 时写进去了
								// 但为了安全起见，我们通常还是通过 Op 或者加锁直接改
								kv.mu.Lock()
								if kv.shardStatus[sID] == GCing {
									kv.shardStatus[sID] = Serving
								}
								kv.mu.Unlock()
								return
							}
						}
					}(shardID, configNum, servers)
				}
			}
			kv.mu.Unlock()
			wg.Wait()
		}
		time.Sleep(GCInterval)
	}
}

// --- Snapshot ---

func (kv *ShardKV) snapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	
	e.Encode(kv.kvDB)
	e.Encode(kv.shadowDB)
	e.Encode(kv.shardStatus)
	e.Encode(kv.lastOps)
	e.Encode(kv.config)
	e.Encode(kv.lastConfig)
	
	kv.rf.Snapshot(index, w.Bytes())
}

func (kv *ShardKV) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	
	var kvDB map[int]map[string]string
	var shadowDB map[int]map[int]map[string]string
	var shardStatus map[int]int
	var lastOps map[int64]OpResult
	var config shardctrler.Config
	var lastConfig shardctrler.Config
	
	if d.Decode(&kvDB) != nil ||
		d.Decode(&shadowDB) != nil ||
		d.Decode(&shardStatus) != nil ||
		d.Decode(&lastOps) != nil ||
		d.Decode(&config) != nil ||
		d.Decode(&lastConfig) != nil {
		log.Fatal("ReadSnapshot decode error")
	} else {
		kv.kvDB = kvDB
		kv.shadowDB = shadowDB
		kv.shardStatus = shardStatus
		kv.lastOps = lastOps
		kv.config = config
		kv.lastConfig = lastConfig
	}
}

func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	labgob.Register(Op{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(OpResult{})
	labgob.Register(map[string]string{})
	labgob.Register(map[int64]OpResult{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.persister = persister

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	
	kv.kvDB = make(map[int]map[string]string)
	kv.shadowDB = make(map[int]map[int]map[string]string)
	kv.shardStatus = make(map[int]int)
	kv.lastOps = make(map[int64]OpResult)
	kv.waitCh = make(map[int]chan OpResult)

	// 初始化所有分片状态为 Serving (默认 Config 0 不归属于任何组，但状态机初始为空)
	// 实际逻辑由 config loop 驱动

	kv.readSnapshot(persister.ReadSnapshot())

	go kv.applier()
	go kv.monitorConfig()
	go kv.monitorMigration()
	go kv.monitorGC()

	return kv
}
