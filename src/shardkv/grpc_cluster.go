package shardkv

import (
	"net"
	"time"

	"google.golang.org/grpc"

	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

// GRPCCluster starts an in-process cluster whose internal RPC uses gRPC.
type GRPCCluster struct {
	ctrlAddr    string
	ctrlServer  *shardctrler.ShardCtrler
	ctrlGRPC    *grpc.Server
	ctrlLis     net.Listener
	groupGID    int
	groupN      int
	groupAddrs  []string
	groupLis    []net.Listener
	groupGRPC   []*grpc.Server
	groupServer []*ShardKV
	persisters  []*raft.Persister
}

// StartGRPCCluster starts a shardkv + shardctrler cluster with gRPC transport.
func StartGRPCCluster(groupN int) (*GRPCCluster, *Clerk) {
	if groupN <= 0 {
		groupN = 1
	}
	gc := &GRPCCluster{
		groupGID:    1001,
		groupN:      groupN,
		groupAddrs:  make([]string, groupN),
		groupLis:    make([]net.Listener, groupN),
		groupGRPC:   make([]*grpc.Server, groupN),
		groupServer: make([]*ShardKV, groupN),
		persisters:  make([]*raft.Persister, groupN),
	}

	// shardctrler (single replica)
	{
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			panic(err)
		}
		gc.ctrlLis = lis
		gc.ctrlAddr = lis.Addr().String()

		ends := []*labrpc.ClientEnd{labrpc.MakeGRPCEnd(gc.ctrlAddr)}
		p := raft.MakePersister()
		gc.ctrlServer = shardctrler.StartServer(ends, 0, p)

		msvc := labrpc.MakeService(gc.ctrlServer)
		rfsvc := labrpc.MakeService(gc.ctrlServer.Raft())
		srv := labrpc.MakeServer()
		srv.AddService(msvc)
		srv.AddService(rfsvc)
		gc.ctrlGRPC = labrpc.ServeGRPCListener(lis, srv)
	}

	// shardkv group: pre-create listeners and addresses
	for i := 0; i < gc.groupN; i++ {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			panic(err)
		}
		gc.groupLis[i] = lis
		gc.groupAddrs[i] = lis.Addr().String()
	}

	// start shardkv servers
	for i := 0; i < gc.groupN; i++ {
		ends := make([]*labrpc.ClientEnd, gc.groupN)
		for j := 0; j < gc.groupN; j++ {
			ends[j] = labrpc.MakeGRPCEnd(gc.groupAddrs[j])
		}
		mends := []*labrpc.ClientEnd{labrpc.MakeGRPCEnd(gc.ctrlAddr)}

		gc.persisters[i] = raft.MakePersister()
		gc.groupServer[i] = StartServer(
			ends,
			i,
			gc.persisters[i],
			-1,
			gc.groupGID,
			mends,
			func(servername string) *labrpc.ClientEnd {
				return labrpc.MakeGRPCEnd(servername)
			},
		)

		kvsvc := labrpc.MakeService(gc.groupServer[i])
		rfsvc := labrpc.MakeService(gc.groupServer[i].rf)
		srv := labrpc.MakeServer()
		srv.AddService(kvsvc)
		srv.AddService(rfsvc)
		gc.groupGRPC[i] = labrpc.ServeGRPCListener(gc.groupLis[i], srv)
	}

	// join group to shardctrler
	{
		ck := shardctrler.MakeClerk([]*labrpc.ClientEnd{labrpc.MakeGRPCEnd(gc.ctrlAddr)})
		ck.Join(map[int][]string{
			gc.groupGID: func() []string {
				names := make([]string, gc.groupN)
				for i := 0; i < gc.groupN; i++ {
					names[i] = gc.groupAddrs[i]
				}
				return names
			}(),
		})
	}

	// create shardkv client
	ck := MakeClerk([]*labrpc.ClientEnd{labrpc.MakeGRPCEnd(gc.ctrlAddr)}, func(servername string) *labrpc.ClientEnd {
		return labrpc.MakeGRPCEnd(servername)
	})

	time.Sleep(200 * time.Millisecond)
	return gc, ck
}

// Close stops all servers and gRPC listeners.
func (gc *GRPCCluster) Close() {
	for i := 0; i < len(gc.groupServer); i++ {
		if gc.groupServer[i] != nil {
			gc.groupServer[i].Kill()
		}
	}
	if gc.ctrlServer != nil {
		gc.ctrlServer.Kill()
	}
	for _, g := range gc.groupGRPC {
		if g != nil {
			g.Stop()
		}
	}
	if gc.ctrlGRPC != nil {
		gc.ctrlGRPC.Stop()
	}
	for _, lis := range gc.groupLis {
		if lis != nil {
			_ = lis.Close()
		}
	}
	if gc.ctrlLis != nil {
		_ = gc.ctrlLis.Close()
	}
}
