package shardkvgrpc

import (
	"context"
	"sort"
	"time"

	"google.golang.org/grpc"

	"6.5840/shardkv"
	"6.5840/shardkvpb"
)

const defaultTimeout = 5 * time.Second

// Client is an external gRPC client for shardkv.
type Client struct {
	conn    *grpc.ClientConn
	cli     shardkvpb.ShardKVClient
	timeout time.Duration
}

// Dial creates a new gRPC client.
func Dial(addr string, opts ...grpc.DialOption) (*Client, error) {
	if len(opts) == 0 {
		opts = []grpc.DialOption{grpc.WithInsecure()}
	}
	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		return nil, err
	}
	return &Client{
		conn:    conn,
		cli:     shardkvpb.NewShardKVClient(conn),
		timeout: defaultTimeout,
	}, nil
}

// Close closes the client connection.
func (c *Client) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// SetTimeout sets per-request timeout.
func (c *Client) SetTimeout(d time.Duration) {
	c.timeout = d
}

func (c *Client) ctx() (context.Context, context.CancelFunc) {
	if c.timeout <= 0 {
		return context.Background(), func() {}
	}
	return context.WithTimeout(context.Background(), c.timeout)
}

func parseErr(s string) shardkv.Err {
	switch s {
	case string(shardkv.OK):
		return shardkv.OK
	case string(shardkv.ErrNoKey):
		return shardkv.ErrNoKey
	case string(shardkv.ErrWrongGroup):
		return shardkv.ErrWrongGroup
	case string(shardkv.ErrWrongLeader):
		return shardkv.ErrWrongLeader
	case string(shardkv.ErrRepeated):
		return shardkv.ErrRepeated
	case string(shardkv.ErrTimeout):
		return shardkv.ErrTimeout
	case string(shardkv.ErrNotReady):
		return shardkv.ErrNotReady
	case string(shardkv.ErrConfigNotReady):
		return shardkv.ErrConfigNotReady
	case string(shardkv.ErrConflict):
		return shardkv.ErrConflict
	default:
		return shardkv.Err(s)
	}
}

func (c *Client) Get(key string) (string, shardkv.Err) {
	ctx, cancel := c.ctx()
	defer cancel()
	resp, err := c.cli.Get(ctx, &shardkvpb.GetRequest{Key: key})
	if err != nil {
		return "", shardkv.ErrTimeout
	}
	return resp.Value, parseErr(resp.Err)
}

func (c *Client) Put(key, value string) shardkv.Err {
	ctx, cancel := c.ctx()
	defer cancel()
	resp, err := c.cli.Put(ctx, &shardkvpb.PutRequest{Key: key, Value: value})
	if err != nil {
		return shardkv.ErrTimeout
	}
	return parseErr(resp.Err)
}

func (c *Client) Append(key, value string) shardkv.Err {
	ctx, cancel := c.ctx()
	defer cancel()
	resp, err := c.cli.Append(ctx, &shardkvpb.AppendRequest{Key: key, Value: value})
	if err != nil {
		return shardkv.ErrTimeout
	}
	return parseErr(resp.Err)
}

func (c *Client) Range(start, end string, limit int) ([]shardkv.KeyValue, shardkv.Err) {
	ctx, cancel := c.ctx()
	defer cancel()
	resp, err := c.cli.Range(ctx, &shardkvpb.RangeRequest{
		Start: start,
		End:   end,
		Limit: int32(limit),
	})
	if err != nil {
		return nil, shardkv.ErrTimeout
	}
	out := make([]shardkv.KeyValue, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		out = append(out, shardkv.KeyValue{Key: kv.Key, Value: kv.Value})
	}
	return out, parseErr(resp.Err)
}

// Txn is an external single-shard transaction.
type Txn struct {
	c         *Client
	txnID     uint64
	snapshot  uint64
	shard     int
	isolation shardkv.IsolationLevel

	writes  map[string]*string
	reads   map[string]uint64
	invalid bool
}

func toProtoIsolation(level shardkv.IsolationLevel) shardkvpb.IsolationLevel {
	switch level {
	case shardkv.ReadUncommitted:
		return shardkvpb.IsolationLevel_READ_UNCOMMITTED
	case shardkv.ReadCommitted:
		return shardkvpb.IsolationLevel_READ_COMMITTED
	case shardkv.RepeatableRead:
		return shardkvpb.IsolationLevel_REPEATABLE_READ
	case shardkv.Serializable:
		return shardkvpb.IsolationLevel_SERIALIZABLE
	default:
		return shardkvpb.IsolationLevel_READ_COMMITTED
	}
}

// BeginTxn starts a new transaction using keyHint to locate shard.
func (c *Client) BeginTxn(level shardkv.IsolationLevel, keyHint string) (*Txn, shardkv.Err) {
	ctx, cancel := c.ctx()
	defer cancel()
	resp, err := c.cli.TxnBegin(ctx, &shardkvpb.TxnBeginRequest{
		Isolation: toProtoIsolation(level),
		KeyHint:   keyHint,
	})
	if err != nil {
		return nil, shardkv.ErrTimeout
	}
	e := parseErr(resp.Err)
	if e != shardkv.OK {
		return nil, e
	}
	return &Txn{
		c:         c,
		txnID:     resp.TxnId,
		snapshot:  resp.Snapshot,
		shard:     shardkv.Key2ShardForExternal(keyHint),
		isolation: level,
		writes:    make(map[string]*string),
		reads:     make(map[string]uint64),
	}, shardkv.OK
}

// Get reads within the transaction snapshot.
func (tx *Txn) Get(key string) (string, bool) {
	if tx.invalid || shardkv.Key2ShardForExternal(key) != tx.shard {
		tx.invalid = true
		return "", false
	}
	if v, ok := tx.writes[key]; ok {
		if v == nil {
			return "", false
		}
		return *v, true
	}
	ctx, cancel := tx.c.ctx()
	defer cancel()
	resp, err := tx.c.cli.TxnGet(ctx, &shardkvpb.TxnGetRequest{
		Key:      key,
		Snapshot: tx.snapshot,
		TxnId:    tx.txnID,
	})
	if err != nil {
		return "", false
	}
	e := parseErr(resp.Err)
	if e == shardkv.OK {
		tx.reads[key] = resp.Version
		return resp.Value, true
	}
	if e == shardkv.ErrNoKey {
		tx.reads[key] = 0
	}
	return "", false
}

// Put buffers a write.
func (tx *Txn) Put(key, value string) bool {
	if tx.invalid || shardkv.Key2ShardForExternal(key) != tx.shard {
		tx.invalid = true
		return false
	}
	v := value
	tx.writes[key] = &v
	return true
}

// Remove buffers a delete.
func (tx *Txn) Remove(key string) bool {
	if tx.invalid || shardkv.Key2ShardForExternal(key) != tx.shard {
		tx.invalid = true
		return false
	}
	tx.writes[key] = nil
	return true
}

// Range reads a range within the transaction snapshot.
func (tx *Txn) Range(start, end string, limit int) ([]shardkv.KeyValue, bool) {
	if tx.invalid {
		return nil, false
	}
	if start == "" || shardkv.Key2ShardForExternal(start) != tx.shard {
		tx.invalid = true
		return nil, false
	}
	if end != "" && shardkv.Key2ShardForExternal(end) != tx.shard {
		tx.invalid = true
		return nil, false
	}

	ctx, cancel := tx.c.ctx()
	defer cancel()
	resp, err := tx.c.cli.TxnRange(ctx, &shardkvpb.TxnRangeRequest{
		Start:    start,
		End:      end,
		Limit:    int32(limit),
		Snapshot: tx.snapshot,
		TxnId:    tx.txnID,
	})
	if err != nil {
		return nil, false
	}
	e := parseErr(resp.Err)
	if e != shardkv.OK && e != shardkv.ErrNoKey {
		return nil, false
	}

	for _, kv := range resp.Kvs {
		if _, hasWrite := tx.writes[kv.Key]; !hasWrite {
			tx.reads[kv.Key] = kv.Version
		}
	}

	base := make(map[string]string, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		base[kv.Key] = kv.Value
	}

	inRange := func(k string) bool {
		if k < start {
			return false
		}
		if end != "" && k >= end {
			return false
		}
		return true
	}
	for k, v := range tx.writes {
		if !inRange(k) {
			continue
		}
		if v == nil {
			delete(base, k)
		} else {
			base[k] = *v
		}
	}

	out := make([]shardkv.KeyValue, 0, len(base))
	for k, v := range base {
		out = append(out, shardkv.KeyValue{Key: k, Value: v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, true
}

// Commit submits the transaction.
func (tx *Txn) Commit() bool {
	if tx.invalid {
		return false
	}
	writes := make([]*shardkvpb.TxnWrite, 0, len(tx.writes))
	for k, v := range tx.writes {
		if v == nil {
			writes = append(writes, &shardkvpb.TxnWrite{Key: k, Delete: true})
		} else {
			writes = append(writes, &shardkvpb.TxnWrite{Key: k, Value: *v})
		}
	}
	reads := make([]*shardkvpb.TxnRead, 0, len(tx.reads))
	for k, v := range tx.reads {
		reads = append(reads, &shardkvpb.TxnRead{Key: k, Version: v})
	}

	ctx, cancel := tx.c.ctx()
	defer cancel()
	resp, err := tx.c.cli.TxnCommit(ctx, &shardkvpb.TxnCommitRequest{
		TxnId:     tx.txnID,
		Isolation: toProtoIsolation(tx.isolation),
		Writes:    writes,
		Reads:     reads,
	})
	if err != nil {
		return false
	}
	e := parseErr(resp.Err)
	return e == shardkv.OK
}
