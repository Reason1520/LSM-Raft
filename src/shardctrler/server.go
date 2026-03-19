package shardctrler


import "6.5840/raft"
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"
import "time"
import "sort"


type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	maxRaftState int					// snapshot if log grows this big
	LastAppliedIndex int				// 最后一次被 applied 的 index

	configs []Config 					// indexed by config num
	LastOp map[int64]OpResult			// 记录最后一次操作结果
	notifyChans map[int]chan OpResult	// 用来接收结果
}


type Op struct {
	// Your data here.
	Type string
	// Join
	Servers map[int][]string // new GID -> servers mappings
	// Leave
	GIDs []int
	// Move
	Shard int
	GID   int
	// Query
	Num int
	// 唯一标识
	ClientID int64	// 客户端ID标识
	RPCID int64		// 操作唯一标识
}

// OpResult : 操作的结果
type OpResult struct {
	WrongLeader bool
	Err   Err
	Config  Config
	// 增加身份标识，用于校验是否对应的请求
	ClientID int64
	RPCID int64
	Term int
}

// 封装统一的日志提交和等待逻辑
func (sc *ShardCtrler) waitForApplied(op Op) OpResult {
	// 提交到 Raft
	index, term, isLeader :=sc.rf.Start(op)
	if !isLeader {
		// 如果当前节点不是leader
		return OpResult{WrongLeader: true}
	}

	// 创建接收通道
	sc.mu.Lock()
	ch := make(chan OpResult, 1)
	sc.notifyChans[index] = ch
	sc.mu.Unlock()

	// 确保退出时清理 channel
	defer func() {
		sc.mu.Lock()
		delete(sc.notifyChans, index)
		sc.mu.Unlock()
	}()

	// 等待结果
	select {
	case res := <-ch:
		// 校验当前 index 执行的命令是否是当前 RPC 提交的命令
		// 如果 Term 变了，或者 ClientID/RPCID 不匹配，说明 Log 在该 index 被覆盖了
		if res.Term != term || res.ClientID != op.ClientID || res.RPCID != op.RPCID {
			return OpResult{WrongLeader: true}
		}
		return res
	case <-time.After(500 * time.Millisecond):
		// 超时
		return OpResult{Err: ErrTimeout}
	}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	// 创建操作
	op := Op{
		Type: JOIN,
		Servers: args.Servers,
		ClientID: args.ClientID,
		RPCID: args.RPCID,
	}

	// 统一的等待逻辑
	res := sc.waitForApplied(op)
	reply.WrongLeader = res.WrongLeader
	reply.Err = res.Err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		Type: LEAVE,
		GIDs: args.GIDs,
		ClientID: args.ClientID,
		RPCID: args.RPCID,
	}

	// 统一的等待逻辑
	res := sc.waitForApplied(op)
	reply.WrongLeader = res.WrongLeader
	reply.Err = res.Err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		Type: MOVE,
		Shard: args.Shard,
		GID: args.GID,
		ClientID: args.ClientID,
		RPCID: args.RPCID,
	}

	// 统一的等待逻辑
	res := sc.waitForApplied(op)
	reply.WrongLeader = res.WrongLeader
	reply.Err = res.Err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		Type: QUERY,
		Num: args.Num,
		ClientID: args.ClientID,
		RPCID: args.RPCID,
	}

	// 统一的等待逻辑
	res := sc.waitForApplied(op)
	reply.WrongLeader = res.WrongLeader
	reply.Err = res.Err
	reply.Config = res.Config
}


// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.

func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// Raft : needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].

// 深度拷贝 Config
func (sc *ShardCtrler) cloneConfig(cfg Config) Config {
	newCfg := Config{
		Num:    cfg.Num + 1,
		Shards: cfg.Shards, // 数组是值传递，但为了保险可以直接 copy
		Groups: make(map[int][]string),
	}
	// copy map
	for k, v := range cfg.Groups {
		newCfg.Groups[k] = v
	}
	return newCfg
}

// 获取排好序的 GID 列表 (保证确定性)
func getSortedGIDs(groups map[int][]string) []int {
	var gids []int
	for g := range groups {
		gids = append(gids, g)
	}
	sort.Ints(gids)
	return gids
}

// 核心负载均衡函数
func (sc *ShardCtrler) rebalance(cfg *Config) {
	gids := getSortedGIDs(cfg.Groups)
	numGroups := len(gids)

	// Case 0: 没有 Group，所有分片归零
	if numGroups == 0 {
		for i := 0; i < len(cfg.Shards); i++ {
			cfg.Shards[i] = 0
		}
		return
	}

	// 1. 统计当前状态
	// gid -> list of shard indexes
	gidToShards := make(map[int][]int) 
	for _, gid := range gids {
		gidToShards[gid] = make([]int, 0)
	}
	
	// 找出未分配的分片 (归属为0 或 归属的组已被删除)
	var unassignedShards []int
	
	for shardIndex, gid := range cfg.Shards {
		if gid == 0 {
			unassignedShards = append(unassignedShards, shardIndex)
			continue
		}
		// 检查该 GID 是否还存在于 Groups 中
		if _, exists := cfg.Groups[gid]; exists {
			gidToShards[gid] = append(gidToShards[gid], shardIndex)
		} else {
			// 组被删除了，分片变成未分配
			cfg.Shards[shardIndex] = 0
			unassignedShards = append(unassignedShards, shardIndex)
		}
	}

	// 2. 计算每个组应该持有的分片数
	// NShards = 10
	// 假设 3 个组: target 为 [4, 3, 3] (余数个组多拿1个)
	avg := len(cfg.Shards) / numGroups
	remainder := len(cfg.Shards) % numGroups

	// 3. 收集多余的分片 (Move excess to unassigned)
	// 我们按照 gids 的顺序来判断，前 remainder 个组的目标是 avg+1，后面是 avg
	for i, gid := range gids {
		target := avg
		if i < remainder {
			target = avg + 1
		}

		// 如果当前持有的 > 目标，把多出来的拿走
		if len(gidToShards[gid]) > target {
			overloadCount := len(gidToShards[gid]) - target
			// 拿走最后几个 (为了确定性，总是拿走 slice 尾部的)
			for k := 0; k < overloadCount; k++ {
				shardIdx := gidToShards[gid][len(gidToShards[gid])-1-k]
				cfg.Shards[shardIdx] = 0 // 标记为未分配
				unassignedShards = append(unassignedShards, shardIdx)
			}
			// 更新该组的缓存列表
			gidToShards[gid] = gidToShards[gid][:target]
		}
	}

	// 此时，unassignedShards 包含了所有 0 的分片 + 刚刚从多余组拿出来的分片
	// 且为了保证 Raft 一致性，建议对 unassignedShards 排序 (虽然逻辑上不强求，但有助于 debug 和确定性)
	sort.Ints(unassignedShards)

	// 4. 填充缺少的组 (Fill deficits)
	ptr := 0 // 指向 unassignedShards 的指针
	
	for i, gid := range gids {
		target := avg
		if i < remainder {
			target = avg + 1
		}

		// 如果当前持有的 < 目标，从 unassigned 填充
		for len(gidToShards[gid]) < target {
			if ptr >= len(unassignedShards) {
				break // 理论上不会发生，除非总分片数不对
			}
			shardIdx := unassignedShards[ptr]
			ptr++
			
			cfg.Shards[shardIdx] = gid
			gidToShards[gid] = append(gidToShards[gid], shardIdx)
		}
	}
}

// execute : 执行具体的逻辑 
func (sc *ShardCtrler) execute(op *Op, opRes *OpResult) {
	// 获取最新的配置作为基础
	lastCfg := sc.configs[len(sc.configs)-1]
	
	switch op.Type {
	case JOIN:
		// 创建新配置
		newCfg := sc.cloneConfig(lastCfg)
		
		// 添加新组
		for gid, servers := range op.Servers {
			newCfg.Groups[gid] = servers
		}
		
		// 重新负载均衡
		sc.rebalance(&newCfg)
		
		// 保存
		sc.configs = append(sc.configs, newCfg)
		opRes.Err = OK

	case LEAVE:
		newCfg := sc.cloneConfig(lastCfg)
		
		// 删除组
		for _, gid := range op.GIDs {
			delete(newCfg.Groups, gid)
		}
		
		// 重新负载均衡 (rebalance 会自动处理被删除组的分片)
		sc.rebalance(&newCfg)
		
		sc.configs = append(sc.configs, newCfg)
		opRes.Err = OK

	case MOVE:
		newCfg := sc.cloneConfig(lastCfg)
		
		// Move 是强制指定，不进行自动 rebalance
		// 只有在 GID 存在时才移动，虽然题目通常假设 GID 有效
		if _, exists := newCfg.Groups[op.GID]; exists {
			newCfg.Shards[op.Shard] = op.GID
		}
		
		sc.configs = append(sc.configs, newCfg)
		opRes.Err = OK

	case QUERY:
		// 如果 Num == -1 或者超出范围，返回最新的
		if op.Num == -1 || op.Num >= len(sc.configs) {
			opRes.Config = sc.configs[len(sc.configs)-1]
		} else {
			opRes.Config = sc.configs[op.Num]
		}
		opRes.Err = OK
	}
}


// 将applyChannel中的命令应用到状态机中
func (sc *ShardCtrler) applier() {
	for {
		applyMsg := <-sc.applyCh

		if applyMsg.CommandValid {

			sc.mu.Lock()

			// 如果该命令已经被快照处理过（过期），直接忽略
            if applyMsg.CommandIndex <= sc.LastAppliedIndex {
                sc.mu.Unlock()
                continue
            }

            // 更新 LastAppliedIndex (必须在处理逻辑之前或之后，但必须对所有节点生效)
            sc.LastAppliedIndex = applyMsg.CommandIndex

			// 默认结果
			op := applyMsg.Command.(Op)
			var opRes OpResult
			opRes.ClientID = op.ClientID
			opRes.RPCID = op.RPCID
			opRes.Term = 0

			// 幂等性去重
			// 这里采用最严格的一次性语义：如果已经执行过，直接返回旧结果
			if lastRes, ok := sc.LastOp[op.ClientID]; ok && lastRes.RPCID == op.RPCID {
				opRes = lastRes
				opRes.Err = OK // 确保是 OK
			} else {
				// 执行状态机逻辑
				sc.execute(&op, &opRes)
				
				// 更新 lastOp 表
				if op.Type != QUERY {
					sc.LastOp[op.ClientID] = opRes
				}
			}

			// 获取当前的 Term 用于校验
			currentTerm, isLeader := sc.rf.GetState()
			if isLeader {
				opRes.Term = currentTerm
			}

			// 通知等待的 RPC
			if ch, ok := sc.notifyChans[applyMsg.CommandIndex]; ok {
				// 非阻塞发送，防止死锁
				select {
				case ch <- opRes:
				default:
				}
			}

			sc.mu.Unlock()
		}
	}
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.LastOp = make(map[int64]OpResult)
	sc.notifyChans = make(map[int]chan OpResult)
	sc.LastAppliedIndex = 0

	go sc.applier()

	return sc
}
