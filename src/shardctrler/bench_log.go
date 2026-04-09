package shardctrler

import (
	"fmt"
	"os"
)

func benchLogf(format string, args ...interface{}) {
	if os.Getenv("BENCH_DEBUG") != "1" {
		return
	}
	fmt.Fprintf(os.Stderr, format, args...)
	if len(format) == 0 || format[len(format)-1] != '\n' {
		fmt.Fprint(os.Stderr, "\n")
	}
}
