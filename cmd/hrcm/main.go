// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	hrcconfig "dimidiumlabs.io/hrc/internal/config"
	"dimidiumlabs.io/hrc/internal/runtime"
)

var (
	help    = flag.Bool("help", false, "Show this help")
	version = flag.Bool("version", false, "Show hrcm version")
)

func main() {
	flag.Parse()
	if *help {
		flag.Usage()
		return
	}
	if *version {
		fmt.Println(hrcconfig.Version)
		return
	}

	if flag.NArg() != 2 {
		flag.Usage()
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := serve(ctx, flag.Args()); err != nil {
		slog.Error("Monitor stopped", "err", err)
		os.Exit(1)
	}
}

func serve(ctx context.Context, args []string) (err error) {
	conn, err := runtime.InheritedConnection(args)
	if err != nil {
		return fmt.Errorf("Coudn't connect to admin sock: %w", err)
	}

	r := newRPC(conn)
	defer r.Close()

	if err := r.Handshake(ctx); err != nil {
		if errors.Is(err, net.ErrClosed) {
			return nil
		}
		return err
	}

	s, err := NewMonitor(r)
	if err != nil {
		return err
	}
	defer func() {
		r.Close()
		err = errors.Join(err, s.close())
	}()
	return s.Serve(ctx)
}
