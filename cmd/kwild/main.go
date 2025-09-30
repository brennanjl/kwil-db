package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/trufnetwork/kwil-db/app"
	"github.com/trufnetwork/kwil-db/extensions/precompiles"
	"github.com/trufnetwork/kwil-db/node/cache"
)

func init() {
	err := precompiles.RegisterInitializer("lru", cache.CacheInitializer)
	if err != nil {
		panic("err")
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		fmt.Println("shutdown signal received")
		cancel()
	}()

	if err := app.RunRootCmd(ctx); err != nil {
		os.Exit(-1)
	}

	os.Exit(0)
}
