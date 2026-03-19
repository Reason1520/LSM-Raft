package raft

// TODO
// 心跳也要进行完整的日志一致性检查

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"

	//"runtime/debug"
	"sync"
	"sync/atomic"

	"fmt"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.

// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.

func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}

func max(a, b int) int {
    if a > b {
        return a
    }
    return b
}

// 服务器的角色
const (
	FOLLOWER = iota	// 跟随者
	LEADER 			// 领导者
	CANDIDATE		// 候选者
)

var PRINTINF = false	// 打印信息


// ApplyMsg : 当日志条目被commit，该服务器会向m.applyCh发送一个ApplyMsg
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// LogEntry : 日志条目
type LogEntry struct {
	Term int				// 日志条目的任期号
	Command interface{}		// 日志条目的内容
	TimeStamp int64			// Leader接收该条目的时间
}

// Snapshot : 快照
type Snapshot struct {
	LastIncludedIndex int			// 快照中最后包含的日志的索引
	LastIncludedTerm int			// 快照中最后包含的日志的任期号
	Data []byte						// 快照数据
}

// Raft : A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	applyCond  *sync.Cond		  // applychannel的条件变量,表示当前有已经commmit的log可以apply
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// 持久状态
	currentTerm int				// 当前任期号
	votedFor int				// 在当前任期号中投票给的候选人,没有投票的为-1
	log []LogEntry				// 存储的日志条目(第一个索引为1),log[0]始终是一个哨兵条目，存储LastIncludedIndex和LastIncludedTerm

	lastIncludedIndex int		// 快照包含的最后一个日志的索引
	lastIncludedTerm int		// 快照包含的最后一个日志的任期

	// 易失状态
	role int					// 该服务器的角色,初始都为Follower
	commitIndex int				// 已知已提交的最高日志条目索引值
	lastApplied int				// 已知已应用到状态机的最高日志条目6索引值,且有commmitIndex >= lastApplied
	applyChannel chan ApplyMsg	// 日志上传通道

	electionTimer time.Time		// 选举计时
	// Leader的易失状态(在选举后重新初始化)
	nextIndex []int				// 对于每个服务器,要发送给该服务器的下一个日志条目的索引下一个要发送的索引(初始化为Leader的最后一个日志索引+1)
	matchIndex []int			// 对于每个服务器,该服务器已经知道的复制该日志的最高索引值(初始化为0,单增)
}

// return currentTerm and whether this server
// believes it is the leader.

func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.role == LEADER)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).

func (rf *Raft) persist(persistSnapshot bool, snapshot []byte) {
	if persistSnapshot {
		rf.persister.Save(rf.encodeState(), snapshot)
	} else {
		rf.persister.SaveRaftState(rf.encodeState())
	}
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
    // 创建解码器
    r := bytes.NewBuffer(data)
    d := labgob.NewDecoder(r)
    
    // 解码各个状态变量
    var currentTerm int
    var votedFor int
    var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
    
    // 解码数据
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil || 
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
			// error handling
	} else {
		// 解码完成后更新状态
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		// 防御性编程：确保 log 至少有一个哨兵
		if len(rf.log) == 0 {
			rf.log = []LogEntry{{Term: lastIncludedTerm, Command: nil}} // Command 默认为 nil
		} else {
			rf.log = log
		}
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		
		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
	}
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.

// Snapshot : 创建从snapshotBeginIndex开始到index为止的snapshot（包括index）
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果索引小于等于已快照的索引，则无需操作
	if index <= rf.lastIncludedIndex {
		return
	}

	sliceIndex := rf.getSliceIndex(index)
	// 确保索引不超过当前日志范围
	if sliceIndex < 0 || sliceIndex >= len(rf.log) {
		return
	}

	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.log[sliceIndex].Term
	
	newLog := make([]LogEntry, 0, len(rf.log)-sliceIndex)
	newLog = append(newLog, rf.log[sliceIndex:]...)
	rf.log = newLog

	// 清理哨兵
	rf.log[0].Command = nil

	rf.persist(true, snapshot)
}

// InstallSnapshotArgs ： 发送SnapShot的参数
type InstallSnapshotArgs struct { 
	Term int				// Leader的任期号
	LeaderID int			// Leader的ID
	LastIncludedIndex int	// 快照中包含的最后一个日志条目的索引
	LastIncludedTerm int	// 快照中包含的日志条目的任期号
	Offset int				// 该数据块在快照中的偏移量
	Data []byte				// 快照数据
	Done bool				// 是否发送完已经完成快照
}

// InstallSnapshotReply ： 接收快照的回复
type InstallSnapshotReply struct {
	Term int		// 当前节点的任期号
}

// InstallSnapshot ： 发送SnapShot给其他节点
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    
    if PRINTINF {
        fmt.Printf("server %d receive InstallSnapshot from %d, term %d, lastIncludedIndex %d\n", 
            rf.me, args.LeaderID, args.Term, args.LastIncludedIndex)
    }

    reply.Term = rf.currentTerm
    if args.Term < rf.currentTerm {
        return
    }
    
    // 如果领导者任期更大，转为追随者
    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
        rf.votedFor = -1
        rf.role = FOLLOWER
        rf.persist(false, nil)
    }

	// 如果快照太旧，则忽略
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}
    
    // 重置选举定时器
    rf.electionTimer = time.Now()

	indexInLog := rf.getSliceIndex(args.LastIncludedIndex)
	
	if indexInLog > 0 && indexInLog < len(rf.log) && rf.log[indexInLog].Term == args.LastIncludedTerm {
		// 如果切片的索引和任期与log[]中的某个日志都匹配，则从该索引开始截断日志
		rf.log = rf.log[indexInLog:]
	} else {
		// 否则，从log[]的开头开始截断日志
		rf.log = make([]LogEntry, 1)
		rf.log[0].Term = args.LastIncludedTerm
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.log[0].Term = args.LastIncludedTerm
	rf.log[0].Command = nil 
	
	// 默认切片内容已提交
	if args.LastIncludedIndex > rf.commitIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	
	// FIX: 不要在这里更新 lastApplied，也不要发送 applyCh
	// 只需要保存数据并唤醒 applier，由 applier 负责发送，保证顺序
	rf.persist(true, args.Data)
	rf.applyCond.Broadcast()
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	return w.Bytes()
}

// RequestVoteArgs : 投票请求参数 example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term int 			// 候选人的任期号
	CandidateID int		// 候选者的ID
	LastLogIndex int	// 候选者的最后一条日志的索引值
	LastLogTerm int		// 候选者的最后条日志的任期号
}

// RequestVoteReply : 投票请求回复 example RequestVote RPC reply structure.
type RequestVoteReply struct {
	// Your data here (3A).
	Term int			// 自身任期号
	VoteGranted bool	// 是否投票给该候选者
}

// RequestVote : 被请求投票的RPC example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//fmt.Printf("server %d receive RequestVote from %d\n", rf.me, args.CandidateID)
    rf.mu.Lock()
    defer rf.mu.Unlock()
    
	// 如果候选者任期小于当前任期，拒绝投票
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// 如果候选者任期更大，转为追随者
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = FOLLOWER
		rf.persist(false, nil)
	}

	reply.Term = rf.currentTerm

	// 检查投票资格：要么还没投过票，要么已经投给了这个候选者
	canVote := (rf.votedFor == -1 || rf.votedFor == args.CandidateID)
	
	// 检查日志至少一样新
	lastLogIndex, lastLogTerm := rf.getLastLogInfo()
	isUpToDate := false
	if args.LastLogTerm > lastLogTerm {
		isUpToDate = true
	} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
		isUpToDate = true
	}

	if canVote && isUpToDate {
		rf.votedFor = args.CandidateID
		rf.electionTimer = time.Now()
		reply.VoteGranted = true
		rf.persist(false, nil)
	} else {
		reply.VoteGranted = false
	}

	if PRINTINF {
		if reply.VoteGranted {
			fmt.Printf("server %d vote for %d, term:%d\n", rf.me, args.CandidateID, reply.Term)
		} else {
			fmt.Printf("server %d reject %d, term:%d\n", rf.me, args.CandidateID, reply.Term)
		}
	}
}

// AppendEntriesArgs : 复制日志调用参数
type AppendEntriesArgs struct {
    Term int			// Leader的任期号
    LeaderID int		// Leader的ID
	PrevLogIndex int	// 在新Log之前的日志条目索引
	PrevLogTerm int		// 在新Log之前的日志条目的任期号
	Entries []LogEntry	// 新的日志条目,可能不止一项(如果是心跳则为空)
	LeaderCommit int	// Leader的已提交的日志索引
}

// AppendEntriesReply : 复制日志调用返回参数
type AppendEntriesReply struct {
    Term int			// 当前任期
    Success bool		// 是否成功(Follower的前一条日志条目对应prevLogIndex和prevLogTerm)
	XTerm int  			// 与PrevLogIndex出冲突的日志的任期号，没有冲突则为-1
    XIndex int 			// 该冲突任期的第一条日志的索引
    XLen int    		// 空白的Log槽位数量
}

// AppendEntries : 复制日志
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果领导者任期小于当前任期，拒绝
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 收到有效RPC，重置选举超时
	rf.electionTimer = time.Now()

	// 如果领导者任期更大，转为追随者
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = FOLLOWER
		rf.persist(false, nil)
	}
	if rf.role != FOLLOWER {
		rf.role = FOLLOWER
	}

	reply.Term = rf.currentTerm

	lastIndex, _ := rf.getLastLogInfo()
	slicePrevIndex := rf.getSliceIndex(args.PrevLogIndex)

	// Case 1: Follower 日志太短
	if args.PrevLogIndex > lastIndex {
		reply.Success = false
		reply.XTerm = -1
		reply.XLen = lastIndex + 1
		reply.XIndex = -1
		return
	}

	// Case 2: Term 冲突
	var myTermAtPrev int
	if slicePrevIndex >= 0 {
		myTermAtPrev = rf.log[slicePrevIndex].Term
	} else {
		myTermAtPrev = args.PrevLogTerm 
	}

	if myTermAtPrev != args.PrevLogTerm {
		reply.Success = false
		reply.XTerm = myTermAtPrev
		idx := slicePrevIndex
		for idx > 0 && rf.log[idx].Term == reply.XTerm {
			idx--
		}
		reply.XIndex = rf.lastIncludedIndex + idx + 1
		if rf.log[idx].Term != reply.XTerm {
			// standard mismatch
		} else {
			reply.XIndex = rf.lastIncludedIndex + idx
		}
		reply.XLen = lastIndex + 1
		return
	}

	// 日志匹配成功
	reply.Success = true

	if PRINTINF {
		fmt.Printf("server %d: log :", rf.me)
		for i := 0 ; i < len(rf.log); i++ {
			fmt.Printf("%d:%v ", rf.log[i].Term, rf.log[i].Command)
		}
		fmt.Printf("recive log :")
		for i := 0 ; i < len(args.Entries); i++ {
			fmt.Printf("%d:%v ", args.Entries[i].Term, args.Entries[i].Command)
		}
		fmt.Printf("\n")
	}

	// 找到第一个不匹配的条目
	conflictFound := false
	startAppendIdx := 0
	
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		sliceIdx := rf.getSliceIndex(idx)
		
		if sliceIdx < 0 {
			// 如果索引比当前log内索引小，则跳过
			continue
		}
		
		if sliceIdx >= len(rf.log) {
			// 如果索引比当前log索引大，则从i开始添加
			startAppendIdx = i
			conflictFound = true
			break
		}

		if rf.log[sliceIdx].Term != entry.Term {
			// 如果有冲突，先截断再添加
			rf.log = rf.log[:sliceIdx]
			startAppendIdx = i
			conflictFound = true
			break
		}
	}

	if conflictFound {
		rf.log = append(rf.log, args.Entries[startAppendIdx:]...)
		rf.persist(false, nil)
	}

	// 修改commitIndex
	if args.LeaderCommit > rf.commitIndex {
		lastNewIndex, _ := rf.getLastLogInfo()
		rf.commitIndex = min(args.LeaderCommit, lastNewIndex)
		rf.applyCond.Broadcast()
	}
}


// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).

// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.

// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().

// look at the comments in ../labrpc/labrpc.go for more details.

// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.

// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.

// 更新commitIndex
func (rf *Raft) updateCommitIndex() {
	lastLogIndex, _ := rf.getLastLogInfo()
	for n := rf.commitIndex + 1; n <= lastLogIndex; n++ {
		if rf.getLogTerm(n)!= rf.currentTerm {
			continue
		}

		count := 1 // 领导者自己
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= n {
				count++
			}
		}

		if count > len(rf.peers)/2 {
			rf.commitIndex = n
		}
	}

	// 唤醒apply协程
	rf.applyCond.Broadcast()
}

// 广播AppendEntries给所有追随者
func (rf *Raft) broadcastAppendEntries(isHeartbeat bool) {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			rf.mu.Lock()
			if rf.role != LEADER {
				rf.mu.Unlock()
				return
			}

			prevLogIndex := rf.nextIndex[server] - 1
			
			if prevLogIndex < rf.lastIncludedIndex {
				// 如果Follower需要的log不在Leader的log中，则发送快照
				args := &InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderID:          rf.me,
					LastIncludedIndex: rf.lastIncludedIndex,
					LastIncludedTerm:  rf.lastIncludedTerm,
					Data:              rf.persister.ReadSnapshot(),
				}
				rf.mu.Unlock()
				
				reply := &InstallSnapshotReply{}
				if rf.sendInstallSnapshot(server, args, reply) {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.role = FOLLOWER
						rf.votedFor = -1
						rf.persist(false, nil)
					} else {
						if rf.role == LEADER && rf.currentTerm == args.Term {
							// 确保我们发送的快照是最新的，或者至少有效
							if args.LastIncludedIndex > rf.matchIndex[server] {
								// 更新匹配索引和下一个索引
								rf.matchIndex[server] = args.LastIncludedIndex
								rf.nextIndex[server] = args.LastIncludedIndex + 1
							}
						}
					}
					rf.mu.Unlock()
				}
				return
			}

			prevLogTerm := rf.getLogTerm(prevLogIndex)
			
			sliceNextIndex := rf.getSliceIndex(rf.nextIndex[server])
			var entries []LogEntry
			if sliceNextIndex >= 0 && sliceNextIndex < len(rf.log) {
				// 如果nextIndex在log的索引范围内，则截取log
				entries = make([]LogEntry, len(rf.log)-sliceNextIndex)
				copy(entries, rf.log[sliceNextIndex:])
			}

			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderID:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()

			reply := &AppendEntriesReply{}
			if rf.sendAppendEntries(server, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.role = FOLLOWER
					rf.votedFor = -1
					rf.persist(false, nil)
					return
				}

				if rf.role != LEADER || args.Term != rf.currentTerm {
					return
				}

				if reply.Success {
					newMatchIndex := args.PrevLogIndex + len(args.Entries)
					if newMatchIndex > rf.matchIndex[server] {
						// 更新matchIndex和nextIndex
						rf.matchIndex[server] = newMatchIndex
						rf.nextIndex[server] = rf.matchIndex[server] + 1
						rf.updateCommitIndex()
					}
				} else {
					if reply.XTerm == -1 {
						// Follower日志太短
						rf.nextIndex[server] = reply.XLen
					} else {
						found := false
						for idx := len(rf.log) - 1; idx > 0; idx-- {
							if rf.log[idx].Term == reply.XTerm {
								// 在Leader日志中寻找XTerm的最后一条日志
								rf.nextIndex[server] = rf.lastIncludedIndex + idx + 1
								found = true
								break
							}
						}
						if !found {
							// 如果没找到，则将nextIndex设置为XIndex
							rf.nextIndex[server] = reply.XIndex
						}
					}
				}
			}
		}(i)
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != LEADER {
		return -1, -1, false
	}

	index, _ := rf.getLastLogInfo()
	index++
	term := rf.currentTerm

	// 添加日志
	rf.log = append(rf.log, LogEntry{
		Term: term,
		Command: command,
		TimeStamp: time.Now().UnixNano(),
	})
	rf.persist(false, nil)

	// 立即发送心跳
	rf.broadcastAppendEntries(true) 

	return index, term, true
}


// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.

// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.

func (rf *Raft) startElection() {
	rf.mu.Lock()
	
	rf.role = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.electionTimer = time.Now()
	rf.persist(false, nil)
	if PRINTINF {fmt.Printf("server %d start election, its term is %d\n", rf.me, rf.currentTerm)}
	
	lastLogIndex, lastLogTerm := rf.getLastLogInfo()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	var voteCount int32 = 1 // 投给自己
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			reply := &RequestVoteReply{}
			if rf.sendRequestVote(server, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.role != CANDIDATE || args.Term != rf.currentTerm {
					return
				}

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.role = FOLLOWER
					rf.votedFor = -1
					rf.persist(false, nil)
					return
				}

				if reply.VoteGranted {
					atomic.AddInt32(&voteCount, 1)
					if int(atomic.LoadInt32(&voteCount)) > len(rf.peers)/2 {
						if rf.role == CANDIDATE {
							rf.role = LEADER
							if PRINTINF {fmt.Printf("server %d become leader, its term is %d\n", rf.me, rf.currentTerm)}
							// 初始化领导者状态
							for i := range rf.peers {
								rf.nextIndex[i] = lastLogIndex + 1
								rf.matchIndex[i] = 0
							}
							// 立即发送心跳
							rf.broadcastAppendEntries(true)
						}
					}
				}
			}
		}(i)
	}
}

// 索引映射辅助函数
// 将全局 Log Index 转换为 rf.log 切片的下标
// 如果 index 在快照内，返回 -1 (调用者需要处理这种情况)
func (rf *Raft) getSliceIndex(globalIndex int) int {
	sliceIndex := globalIndex - rf.lastIncludedIndex
	if sliceIndex < 0 {
		return -1
	}
	return sliceIndex
}

// 获取日志最后一条的 Index 和 Term
func (rf *Raft) getLastLogInfo() (int, int) {
	sliceLen := len(rf.log)
	lastSliceIndex := sliceLen - 1
	lastGlobalIndex := rf.lastIncludedIndex + lastSliceIndex
	lastTerm := rf.log[lastSliceIndex].Term
	return lastGlobalIndex, lastTerm
}

// 获取指定 Global Index 的 Term
// 假设 index >= lastIncludedIndex
func (rf *Raft) getLogTerm(globalIndex int) int {
	sliceIndex := globalIndex - rf.lastIncludedIndex
	if sliceIndex < 0 {
		return rf.lastIncludedTerm 
	}
	if sliceIndex >= len(rf.log) {
		return -1 //越界
	}
	return rf.log[sliceIndex].Term
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 长时间运行的goroutine
// ticker : 选举检查goroutine
func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.role
		rf.mu.Unlock()

		if state == LEADER {
			rf.mu.Lock()
			// 领导者发送心跳
			rf.broadcastAppendEntries(true)
			rf.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
		} else {
			rf.mu.Lock()
			timeout := time.Duration(300 + rand.Intn(300)) * time.Millisecond
			if time.Since(rf.electionTimer) > timeout {
				// 启动选举
				go rf.startElection()
				rf.electionTimer = time.Now()
			}
			rf.mu.Unlock()
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// applyLogs : 应用已提交的日志条目
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		
		// 只要 lastApplied 落后于 commitIndex 或者 落后于快照，就应该处理
		for rf.lastApplied >= rf.commitIndex && rf.lastApplied >= rf.lastIncludedIndex {
			rf.applyCond.Wait()
		}
		
		// 优先处理快照
		if rf.lastApplied < rf.lastIncludedIndex {
			snapMsg := ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.persister.ReadSnapshot(),
				SnapshotTerm:  rf.lastIncludedTerm,
				SnapshotIndex: rf.lastIncludedIndex,
			}
			
			rf.mu.Unlock()
			rf.applyChannel <- snapMsg
			rf.mu.Lock()
			
			// 更新 lastApplied
			if rf.lastIncludedIndex > rf.lastApplied {
				rf.lastApplied = rf.lastIncludedIndex
			}
			// 确保 commitIndex 至少对齐快照
			if rf.commitIndex < rf.lastApplied {
				rf.commitIndex = rf.lastApplied
			}
			rf.mu.Unlock()
			continue
		}

		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied
		entries := make([]ApplyMsg, 0)
		
		// 循环必须从 lastApplied + 1 开始
		for i := lastApplied + 1; i <= commitIndex; i++ {
			sliceIdx := rf.getSliceIndex(i)
			// sliceIdx == 0 表示这是快照边界条目，已经通过 SnapshotMsg 处理过了，不能作为 Command 发送
			if sliceIdx > 0 && sliceIdx < len(rf.log) {
				msg := ApplyMsg{
					CommandValid: true,
					Command:      rf.log[sliceIdx].Command,
					CommandIndex: i,
				}
				entries = append(entries, msg)
			}
		}
		rf.mu.Unlock()

		for _, msg := range entries {
			rf.applyChannel <- msg
			rf.mu.Lock()
			// 更新 lastApplied，防止重入
			if msg.CommandIndex > rf.lastApplied {
				rf.lastApplied = msg.CommandIndex
			}
			rf.mu.Unlock()
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCond = sync.NewCond(&rf.mu)

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor	= -1
	rf.log = make([]LogEntry, 1)	// log[0]是哨兵值
	rf.log[0] = LogEntry{
		Term: 0,
		Command: nil,
		TimeStamp: time.Now().UnixNano(),
	}
	rf.applyChannel = applyCh

	rf.role = FOLLOWER
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	rf.electionTimer = time.Now()
	go rf.ticker()
	go rf.applier()

	return rf
}
