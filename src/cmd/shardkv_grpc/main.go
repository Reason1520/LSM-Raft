package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	"6.5840/shardkv"
	"6.5840/shardkvpb"
)

func main() {
	addr := flag.String("addr", ":50051", "gRPC listen address")
	n := flag.Int("n", 3, "replicas in the single shardkv group")
	flag.Parse()

	dc, ck := shardkv.StartGRPCCluster(*n)
	defer dc.Close()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen %s failed: %v", *addr, err)
	}

	grpcServer := grpc.NewServer()
	shardkvpb.RegisterShardKVServer(grpcServer, shardkv.NewGRPCServer(ck))

	go func() {
		log.Printf("shardkv gRPC server listening on %s", *addr)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC serve failed: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh
	log.Printf("shutting down gRPC server")
	grpcServer.GracefulStop()
}
