package shardctrler

//
// Shardctrler clerk.
//

import (
	"time"

	"6.5840/labrpc"
	"crypto/rand"
	"math/big"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clerkID int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clerkID = nrand()
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.ClientID = ck.clerkID
	args.RPCID = nrand()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok {
				if reply.WrongLeader || reply.Err == ErrTimeout {
					continue
				} else if reply.Err == OK || reply.Err == ErrRepeated {
					return reply.Config
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientID = ck.clerkID
	benchLogf("[ctrler-join] begin: clientID=%d", args.ClientID)
	args.RPCID = nrand()
	benchLogf("[ctrler-join] rpcid generated: %d", args.RPCID)

	start := time.Now()
	lastLog := time.Now()
	attempt := 0

	for {
		// try each known server.
		for _, srv := range ck.servers {
			benchLogf("[ctrler-join] call ShardCtrler.Join")
			attempt++
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok {
				if reply.WrongLeader || reply.Err == ErrTimeout {
					continue
				} else if reply.Err == OK || reply.Err == ErrRepeated {
					benchLogf("[ctrler-join] success after %d attempts, elapsed=%.2fs", attempt, time.Since(start).Seconds())
					return
				} else if reply.Err == ErrWrongGID {
					benchLogf("[ctrler-join] ErrWrongGID after %d attempts, elapsed=%.2fs", attempt, time.Since(start).Seconds())
					return
				}
			}
			if time.Since(lastLog) > 2*time.Second {
				benchLogf("[ctrler-join] retrying... attempts=%d elapsed=%.2fs lastOK=%v wrongLeader=%v err=%v",
					attempt, time.Since(start).Seconds(), ok, reply.WrongLeader, reply.Err)
				lastLog = time.Now()
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientID = ck.clerkID
	args.RPCID = nrand()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok {
				if reply.WrongLeader || reply.Err == ErrTimeout {
					continue
				} else if reply.Err == OK || reply.Err == ErrRepeated {
					return
				} else if reply.Err == ErrWrongGID {
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientID = ck.clerkID
	args.RPCID = nrand()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && !reply.WrongLeader {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
