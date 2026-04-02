package shardkv

import (
	crand "crypto/rand"
	"encoding/base64"
	"fmt"
	"time"

	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

// DemoCluster is a minimal in-process cluster for local demo usage.
// Note: labrpc is in-memory; this cluster cannot be accessed跨进程.
type DemoCluster struct {
	net *labrpc.Network

	ctrlerNames   []string
	ctrlerServers []*shardctrler.ShardCtrler

	groupGID       int
	groupN         int
	groupServers   []*ShardKV
	groupPersister []*raft.Persister
}

func demoRandString(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

func demoCtrlerName(i int) string {
	return fmt.Sprintf("demo-ctlr-%d", i)
}

func demoServerName(gid int, i int) string {
	return fmt.Sprintf("demo-server-%d-%d", gid, i)
}

// StartDemoCluster starts an in-process shardkv + shardctrler cluster.
// groupN is the number of replicas in the single shardkv group.
func StartDemoCluster(groupN int) (*DemoCluster, *Clerk) {
	if groupN <= 0 {
		groupN = 1
	}

	dc := &DemoCluster{
		net:            labrpc.MakeNetwork(),
		groupGID:       1001,
		groupN:         groupN,
		groupServers:   make([]*ShardKV, groupN),
		groupPersister: make([]*raft.Persister, groupN),
	}
	dc.net.Reliable(true)

	// shardctrler (single replica for demo)
	dc.ctrlerNames = []string{demoCtrlerName(0)}
	dc.ctrlerServers = make([]*shardctrler.ShardCtrler, 1)
	{
		ends := make([]*labrpc.ClientEnd, 1)
		endname := demoRandString(20)
		ends[0] = dc.net.MakeEnd(endname)
		dc.net.Connect(endname, dc.ctrlerNames[0])
		dc.net.Enable(endname, true)

		p := raft.MakePersister()
		dc.ctrlerServers[0] = shardctrler.StartServer(ends, 0, p)

		msvc := labrpc.MakeService(dc.ctrlerServers[0])
		rfsvc := labrpc.MakeService(dc.ctrlerServers[0].Raft())
		srv := labrpc.MakeServer()
		srv.AddService(msvc)
		srv.AddService(rfsvc)
		dc.net.AddServer(dc.ctrlerNames[0], srv)
	}

	// shardkv group
	for i := 0; i < dc.groupN; i++ {
		// ends to other shardkv servers in group
		ends := make([]*labrpc.ClientEnd, dc.groupN)
		for j := 0; j < dc.groupN; j++ {
			endname := demoRandString(20)
			ends[j] = dc.net.MakeEnd(endname)
			dc.net.Connect(endname, demoServerName(dc.groupGID, j))
			dc.net.Enable(endname, true)
		}

		// ends to shardctrler
		mends := make([]*labrpc.ClientEnd, 1)
		mname := demoRandString(20)
		mends[0] = dc.net.MakeEnd(mname)
		dc.net.Connect(mname, dc.ctrlerNames[0])
		dc.net.Enable(mname, true)

		dc.groupPersister[i] = raft.MakePersister()
		dc.groupServers[i] = StartServer(
			ends,
			i,
			dc.groupPersister[i],
			-1,
			dc.groupGID,
			mends,
			func(servername string) *labrpc.ClientEnd {
				name := demoRandString(20)
				end := dc.net.MakeEnd(name)
				dc.net.Connect(name, servername)
				dc.net.Enable(name, true)
				return end
			},
		)

		kvsvc := labrpc.MakeService(dc.groupServers[i])
		rfsvc := labrpc.MakeService(dc.groupServers[i].rf)
		srv := labrpc.MakeServer()
		srv.AddService(kvsvc)
		srv.AddService(rfsvc)
		dc.net.AddServer(demoServerName(dc.groupGID, i), srv)
	}

	// join group to shardctrler
	{
		ends := make([]*labrpc.ClientEnd, 1)
		endname := demoRandString(20)
		ends[0] = dc.net.MakeEnd(endname)
		dc.net.Connect(endname, dc.ctrlerNames[0])
		dc.net.Enable(endname, true)
		ck := shardctrler.MakeClerk(ends)
		ck.Join(map[int][]string{
			dc.groupGID: func() []string {
				names := make([]string, dc.groupN)
				for i := 0; i < dc.groupN; i++ {
					names[i] = demoServerName(dc.groupGID, i)
				}
				return names
			}(),
		})
	}

	// create shardkv client
	ctrlEnds := make([]*labrpc.ClientEnd, 1)
	{
		endname := demoRandString(20)
		ctrlEnds[0] = dc.net.MakeEnd(endname)
		dc.net.Connect(endname, dc.ctrlerNames[0])
		dc.net.Enable(endname, true)
	}

	ck := MakeClerk(ctrlEnds, func(servername string) *labrpc.ClientEnd {
		name := demoRandString(20)
		end := dc.net.MakeEnd(name)
		dc.net.Connect(name, servername)
		dc.net.Enable(name, true)
		return end
	})

	// allow config to propagate
	time.Sleep(200 * time.Millisecond)
	return dc, ck
}

// Close stops all servers and cleans up the network.
func (dc *DemoCluster) Close() {
	for i := 0; i < len(dc.groupServers); i++ {
		if dc.groupServers[i] != nil {
			dc.groupServers[i].Kill()
		}
	}
	for i := 0; i < len(dc.ctrlerServers); i++ {
		if dc.ctrlerServers[i] != nil {
			dc.ctrlerServers[i].Kill()
		}
	}
	if dc.net != nil {
		dc.net.Cleanup()
	}
}
