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

	"dimidiumlabs.io/hrc/internal/space"

	hrcconfig "dimidiumlabs.io/hrc/internal/config"
	"dimidiumlabs.io/hrc/internal/provider"
	"dimidiumlabs.io/hrc/internal/runtime"
	"dimidiumlabs.io/hrc/internal/slot"
	slotpgroup "dimidiumlabs.io/hrc/internal/slot-pgroup"
)

var (
	help    = flag.Bool("help", false, "Show this help")
	version = flag.Bool("version", false, "Show hrcm version")

	socket = flag.String("connect", "", "Local monitor socket")
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
		err = serve(ctx, conn, func(cfg *space.Config) (*scheduler, error) { return configure(cfg, nil) })
	}
	if err != nil {
		slog.Error("Monitor stopped", "err", err)
		os.Exit(1)
	}
}

func configure(config *space.Config, create func(string) (slot.Slot, error)) (*scheduler, error) {
	if config.Slot != "pgroup" {
		return nil, fmt.Errorf("unknown slot implementation %q", config.Slot)
	}
	var slotConfig slotpgroup.Config
	if err := json.Unmarshal(config.SlotConfig, &slotConfig); err != nil || slotConfig.Dir == "" {
		return nil, errors.New("pgroup root is required")
	}
	providers := make(map[string]provider.ProcessConfig, len(config.Providers))
	for name, data := range config.Providers {
		var cfg provider.ProcessConfig
		if err := json.Unmarshal(data, &cfg); err != nil {
			return nil, err
		}
		if err := cfg.Validate(); err != nil {
			return nil, fmt.Errorf("provider %q: %w", name, err)
		}
		providers[name] = cfg
	}
	pool := &runtime.Pool{}
	if create == nil {
		create = func(dir string) (slot.Slot, error) { return slotpgroup.New(dir, providers, pool) }
	}
	s, err := newScheduler(int(config.Capacity), !config.Ephemeral, func(ctx context.Context, name string) (slot.Slot, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return create(filepath.Join(slotConfig.Dir, name+"-"+rand.Text()))
	})
	if err != nil {
		return nil, err
	}
	s.pool = pool
	s.decode = func(name string, payload []byte) (provider.Job, error) {
		if _, ok := providers[name]; !ok {
			return nil, fmt.Errorf("%w: %s", slot.ErrProviderUnavailable, name)
		}
		return &provider.OpaqueJob{Provider: name, Payload: payload}, nil
	}
	return s, nil
}
