package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"6.5840/shardkv"
)

func main() {
	n := flag.Int("n", 3, "number of shardkv replicas in the single group")
	flag.Parse()

	dc, ck := shardkv.StartDemoCluster(*n)
	defer dc.Close()

	fmt.Println("ShardKV demo started (in-process).")
	fmt.Println("Commands: get <k> | put <k> <v> | append <k> <v> | range <start> <end|- > [limit] | txndemo | help | exit")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		switch strings.ToLower(fields[0]) {
		case "exit", "quit":
			return
		case "help":
			fmt.Println("get <k>")
			fmt.Println("put <k> <v>")
			fmt.Println("append <k> <v>")
			fmt.Println("range <start> <end|- > [limit]")
			fmt.Println("txndemo")
		case "get":
			if len(fields) < 2 {
				fmt.Println("usage: get <k>")
				continue
			}
			v := ck.Get(fields[1])
			fmt.Println(v)
		case "put":
			if len(fields) < 3 {
				fmt.Println("usage: put <k> <v>")
				continue
			}
			ck.Put(fields[1], strings.Join(fields[2:], " "))
			fmt.Println("OK")
		case "append":
			if len(fields) < 3 {
				fmt.Println("usage: append <k> <v>")
				continue
			}
			ck.Append(fields[1], strings.Join(fields[2:], " "))
			fmt.Println("OK")
		case "range":
			if len(fields) < 3 {
				fmt.Println("usage: range <start> <end|- > [limit]")
				continue
			}
			start := fields[1]
			end := fields[2]
			if end == "-" {
				end = ""
			}
			limit := 0
			if len(fields) >= 4 {
				if v, err := strconv.Atoi(fields[3]); err == nil {
					limit = v
				}
			}
			kvs := ck.Range(start, end, limit)
			for _, kv := range kvs {
				fmt.Printf("%s = %s\n", kv.Key, kv.Value)
			}
		case "txndemo":
			tx := ck.BeginTxn(shardkv.RepeatableRead)
			_ = tx.Put("a1", "v1")
			_ = tx.Put("a2", "v2")
			kvs, ok := tx.Range("a", "a~", 0)
			fmt.Printf("txn range ok=%v kvs=%v\n", ok, kvs)
			ok = tx.Commit()
			fmt.Printf("txn commit ok=%v\n", ok)
		default:
			fmt.Println("unknown command, type 'help'")
		}
	}
}
