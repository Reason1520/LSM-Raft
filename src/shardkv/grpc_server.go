package shardkv

import (
	"context"
	"sync"

	"6.5840/shardkvpb"
)

// GRPCServer exposes shardkv APIs over gRPC.
type GRPCServer struct {
	shardkvpb.UnimplementedShardKVServer
	pool sync.Pool
}

func NewGRPCServer(ck *Clerk) *GRPCServer {
	s := &GRPCServer{}
	s.pool.New = func() interface{} {
		return ck.Clone()
	}
	s.pool.Put(ck)
	return s
}

func (s *GRPCServer) getClerk() *Clerk {
	return s.pool.Get().(*Clerk)
}

func (s *GRPCServer) putClerk(ck *Clerk) {
	s.pool.Put(ck)
}

func toIsolation(level shardkvpb.IsolationLevel) IsolationLevel {
	switch level {
	case shardkvpb.IsolationLevel_READ_UNCOMMITTED:
		return ReadUncommitted
	case shardkvpb.IsolationLevel_READ_COMMITTED:
		return ReadCommitted
	case shardkvpb.IsolationLevel_REPEATABLE_READ:
		return RepeatableRead
	case shardkvpb.IsolationLevel_SERIALIZABLE:
		return Serializable
	default:
		return ReadCommitted
	}
}

func errString(e Err) string {
	if e == "" {
		return string(OK)
	}
	return string(e)
}

func (s *GRPCServer) Get(ctx context.Context, req *shardkvpb.GetRequest) (*shardkvpb.GetResponse, error) {
	ck := s.getClerk()
	defer s.putClerk(ck)
	val, err := ck.GetWithErr(req.Key)
	return &shardkvpb.GetResponse{Err: errString(err), Value: val}, nil
}

func (s *GRPCServer) Put(ctx context.Context, req *shardkvpb.PutRequest) (*shardkvpb.PutResponse, error) {
	ck := s.getClerk()
	defer s.putClerk(ck)
	ck.Put(req.Key, req.Value)
	return &shardkvpb.PutResponse{Err: string(OK)}, nil
}

func (s *GRPCServer) Append(ctx context.Context, req *shardkvpb.AppendRequest) (*shardkvpb.AppendResponse, error) {
	ck := s.getClerk()
	defer s.putClerk(ck)
	ck.Append(req.Key, req.Value)
	return &shardkvpb.AppendResponse{Err: string(OK)}, nil
}

func (s *GRPCServer) Range(ctx context.Context, req *shardkvpb.RangeRequest) (*shardkvpb.RangeResponse, error) {
	ck := s.getClerk()
	defer s.putClerk(ck)
	kvs := ck.Range(req.Start, req.End, int(req.Limit))
	out := make([]*shardkvpb.KeyValue, 0, len(kvs))
	for _, kv := range kvs {
		out = append(out, &shardkvpb.KeyValue{Key: kv.Key, Value: kv.Value})
	}
	return &shardkvpb.RangeResponse{Err: string(OK), Kvs: out}, nil
}

func (s *GRPCServer) TxnBegin(ctx context.Context, req *shardkvpb.TxnBeginRequest) (*shardkvpb.TxnBeginResponse, error) {
	if req.KeyHint == "" {
		return &shardkvpb.TxnBeginResponse{Err: string(ErrWrongGroup)}, nil
	}

	ck := s.getClerk()
	defer s.putClerk(ck)
	shard := key2shard(req.KeyHint)
	txnID, snapshot := ck.txnBeginOnShard(shard, toIsolation(req.Isolation))
	return &shardkvpb.TxnBeginResponse{
		Err:      string(OK),
		TxnId:    txnID,
		Snapshot: snapshot,
	}, nil
}

func (s *GRPCServer) TxnGet(ctx context.Context, req *shardkvpb.TxnGetRequest) (*shardkvpb.TxnGetResponse, error) {
	ck := s.getClerk()
	defer s.putClerk(ck)
	expectShard := key2shard(req.Key)
	val, ver, err := ck.txnGetOnShard(expectShard, req.Key, req.Snapshot, req.TxnId)
	return &shardkvpb.TxnGetResponse{
		Err:     errString(err),
		Value:   val,
		Version: ver,
	}, nil
}

func (s *GRPCServer) TxnRange(ctx context.Context, req *shardkvpb.TxnRangeRequest) (*shardkvpb.TxnRangeResponse, error) {
	if req.Start == "" {
		return &shardkvpb.TxnRangeResponse{Err: string(ErrWrongGroup)}, nil
	}
	shard := key2shard(req.Start)
	if req.End != "" && key2shard(req.End) != shard {
		return &shardkvpb.TxnRangeResponse{Err: string(ErrWrongGroup)}, nil
	}
	ck := s.getClerk()
	defer s.putClerk(ck)
	kvs, err := ck.txnRangeOnShard(shard, req.Start, req.End, int(req.Limit), req.Snapshot, req.TxnId)
	out := make([]*shardkvpb.TxnRangeKV, 0, len(kvs))
	for _, kv := range kvs {
		out = append(out, &shardkvpb.TxnRangeKV{
			Key:     kv.Key,
			Value:   kv.Value,
			Version: kv.Version,
		})
	}
	return &shardkvpb.TxnRangeResponse{Err: errString(err), Kvs: out}, nil
}

func (s *GRPCServer) TxnAbort(ctx context.Context, req *shardkvpb.TxnGetRequest) (*shardkvpb.TxnCommitResponse, error) {
	ck := s.getClerk()
	defer s.putClerk(ck)
	err := ck.txnAbortEverywhere(req.TxnId)
	return &shardkvpb.TxnCommitResponse{Err: errString(err)}, nil
}

func (s *GRPCServer) TxnCommit(ctx context.Context, req *shardkvpb.TxnCommitRequest) (*shardkvpb.TxnCommitResponse, error) {
	ck := s.getClerk()
	defer s.putClerk(ck)
	writes := make([]TxnWrite, 0, len(req.Writes))
	for _, w := range req.Writes {
		writes = append(writes, TxnWrite{Key: w.Key, Value: w.Value, Delete: w.Delete})
	}
	reads := make([]TxnRead, 0, len(req.Reads))
	for _, r := range req.Reads {
		reads = append(reads, TxnRead{Key: r.Key, Version: r.Version})
	}

	shard := -1
	for _, w := range writes {
		s := key2shard(w.Key)
		if shard == -1 {
			shard = s
		} else if shard != s {
			return &shardkvpb.TxnCommitResponse{Err: string(ErrWrongGroup)}, nil
		}
	}
	for _, r := range reads {
		s := key2shard(r.Key)
		if shard == -1 {
			shard = s
		} else if shard != s {
			return &shardkvpb.TxnCommitResponse{Err: string(ErrWrongGroup)}, nil
		}
	}
	if shard == -1 {
		err := ck.txnAbortEverywhere(req.TxnId)
		return &shardkvpb.TxnCommitResponse{Err: errString(err)}, nil
	}
	args := TxnCommitArgs{
		TxnID:     req.TxnId,
		ClientID:  ck.ClientID,
		RPCID:     ck.allocRPCID(),
		Isolation: toIsolation(req.Isolation),
		Writes:    writes,
		Reads:     reads,
	}
	err := ck.txnCommitOnShard(shard, &args)
	return &shardkvpb.TxnCommitResponse{Err: errString(err)}, nil
}
