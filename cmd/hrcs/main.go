// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	hrcconfig "dimidiumlabs.io/hrc/internal/config"
	"dimidiumlabs.io/hrc/internal/provider"
	github "dimidiumlabs.io/hrc/internal/provider-github"
	ipc "dimidiumlabs.io/hrc/internal/scheduler-ipc"
	"dimidiumlabs.io/hrc/internal/slot"
	slotpgroup "dimidiumlabs.io/hrc/internal/slot-pgroup"
)

var (
	help    = flag.Bool("help", false, "Show this help")
	version = flag.Bool("version", false, "Show hrcs version")

	socket = flag.String("connect", "", "Local scheduler socket")
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

	if *socket == "" {
		flag.Usage()
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", *socket)
	if err == nil {
		err = serve(ctx, conn, func(cfg ipc.Config) (*scheduler, error) { return configure(cfg, nil) })
	}
	if err != nil {
		slog.Error("Scheduler stopped", "err", err)
		os.Exit(1)
	}
}

func configure(config ipc.Config, create func(string) (slot.Slot, error)) (*scheduler, error) {
	if config.Slot != "pgroup" {
		return nil, fmt.Errorf("unknown slot implementation %q", config.Slot)
	}
	var slotConfig slotpgroup.Config
	if err := json.Unmarshal(config.SlotConfig, &slotConfig); err != nil || slotConfig.Dir == "" {
		return nil, errors.New("pgroup root is required")
	}
	var githubConfig *github.WorkerConfig
	for name, data := range config.Providers {
		switch name {
		case "github":
			var cfg github.WorkerConfig
			if err := json.Unmarshal(data, &cfg); err != nil {
				return nil, err
			}
			githubConfig = &cfg
		default:
			return nil, fmt.Errorf("unknown provider %q", name)
		}
	}
	if create == nil {
		create = func(dir string) (slot.Slot, error) { return slotpgroup.New(dir, githubConfig) }
	}
	s, err := newScheduler(config.Capacity, !config.Ephemeral, func(ctx context.Context, name string) (slot.Slot, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return create(filepath.Join(slotConfig.Dir, name+"-"+rand.Text()))
	})
	if err != nil {
		return nil, err
	}
	s.decode = func(name string, payload []byte) (provider.Job, error) {
		var job provider.Job
		switch name {
		case "github":
			job = github.NewJob()
		default:
			return nil, fmt.Errorf("%w: %s", slot.ErrProviderUnavailable, name)
		}
		if err := job.Unmarshal(payload); err != nil {
			return nil, err
		}
		return job, nil
	}
	return s, nil
}
