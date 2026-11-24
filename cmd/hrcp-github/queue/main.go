// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"dimidiumlabs.io/hrc/internal/config"
)

func main() {
	socket := flag.String("connect", "", "Parent RPC socket")
	version := flag.Bool("version", false, "Show version")
	flag.Parse()
	if *version {
		fmt.Println(config.Version)
		return
	}
	if *socket == "" {
		flag.Usage()
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", *socket)
	if err == nil {
		err = ServeQueue(ctx, conn)
	}
	if err != nil {
		slog.Error("Provider stopped", "err", err)
		os.Exit(1)
	}
}
