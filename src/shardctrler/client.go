package shardctrler

//
// Shardctrler clerk.
//

import "6.5840/labrpc"
import "time"
import "crypto/rand"
import "math/big"

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
				// 当RPC调用成功时，判断错误信息
				if reply.WrongLeader || reply.Err == ErrTimeout {
					// 如果不是leader或超时，则继续尝试下一个
					continue
				} else if reply.Err == OK || reply.Err == ErrRepeated {
					// 如果错误信息为OK或重复，则返回结果
					return reply.Config
				}
				// 暂时认为不会出现其他错误
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
	args.RPCID = nrand()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok {
				// 当RPC调用成功时，判断错误信息
				if reply.WrongLeader || reply.Err == ErrTimeout {
					// 如果不是leader或超时，则继续尝试下一个
					continue
				} else if reply.Err == OK || reply.Err == ErrRepeated {
					// 如果错误信息为OK或重复，则直接返回
					return
				} else if reply.Err == ErrWrongGID {
					// 如果错误信息错误的GID（join了已经存在的GID），则退出
					return
				}
				// 暂时认为不会出现其他错误
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
				// 当RPC调用成功时，判断错误信息
				if reply.WrongLeader || reply.Err == ErrTimeout {
					// 如果不是leader或超时，则继续尝试下一个
					continue
				} else if reply.Err == OK || reply.Err == ErrRepeated {
					// 如果错误信息为OK或重复，则直接返回
					return
				} else if reply.Err == ErrWrongGID {
					// 如果错误信息错误的GID（leave了不存在的GID），则退出
					return
				}
				// 暂时认为不会出现其他错误
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
		// try each known server. 测试用的函数，应该不会出错，暂时不改
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
