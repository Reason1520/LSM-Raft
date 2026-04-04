package labrpc

import (
	"context"
	"fmt"
	"net"
	"time"

	"6.5840/labrpcpb"
	"google.golang.org/grpc"
)

const defaultGRPCTimeout = 3 * time.Second

// MakeGRPCEnd creates a ClientEnd that talks over gRPC.
func MakeGRPCEnd(target string, opts ...grpc.DialOption) *ClientEnd {
	if len(opts) == 0 {
		opts = []grpc.DialOption{grpc.WithInsecure()}
	}
	conn, err := grpc.Dial(target, opts...)
	if err != nil {
		panic(fmt.Sprintf("labrpc: grpc dial %s failed: %v", target, err))
	}
	return &ClientEnd{
		grpcClient: labrpcpb.NewLabRPCClient(conn),
		grpcConn:   conn,
		grpcTarget: target,
		timeout:    defaultGRPCTimeout,
	}
}

type grpcServer struct {
	labrpcpb.UnimplementedLabRPCServer
	rs *Server
}

func (g *grpcServer) Call(ctx context.Context, req *labrpcpb.CallRequest) (*labrpcpb.CallReply, error) {
	if req == nil {
		return &labrpcpb.CallReply{Ok: false}, nil
	}
	rep := g.rs.dispatch(reqMsg{
		svcMeth: req.SvcMeth,
		args:    req.Args,
	})
	return &labrpcpb.CallReply{Ok: rep.ok, Reply: rep.reply}, nil
}

// ServeGRPCListener exposes a labrpc.Server over gRPC using an existing listener.
func ServeGRPCListener(lis net.Listener, rs *Server) *grpc.Server {
	g := grpc.NewServer()
	labrpcpb.RegisterLabRPCServer(g, &grpcServer{rs: rs})
	go func() {
		_ = g.Serve(lis)
	}()
	return g
}

// StartGRPCServer exposes a labrpc.Server over gRPC.
func StartGRPCServer(addr string, rs *Server) (*grpc.Server, net.Listener, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, err
	}
	return ServeGRPCListener(lis, rs), lis, nil
}
