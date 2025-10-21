//go:build ignore

package main

import (
	"fmt"
	"os"

	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/storage"
)

func main() {

	total := 50000

	values := make([][]byte, total)
	for n := range total {
		values[n] = fmt.Appendf(make([]byte, 0, 13), "somedata_%s", os.Args[1])
	}

	strg := storage.NewStorage()
	_ = strg.CrashSafeWriteFileLines(os.Args[2], values, 0666, 0666)
}
