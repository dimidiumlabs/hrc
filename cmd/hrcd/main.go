// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"context"
	"encoding/json/v2"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	hrcconfig "dimidiumlabs.io/hrc/internal/config"
	"dimidiumlabs.io/hrc/internal/provider"
	github "dimidiumlabs.io/hrc/internal/provider-github"
	slotpgroup "dimidiumlabs.io/hrc/internal/slot-pgroup"
	"dimidiumlabs.io/hrc/internal/space"
	spacehost "dimidiumlabs.io/hrc/internal/space-host"
)

type ForgeConfig struct {
	hrcconfig.Sum[hrcconfig.Provider, struct {
		GitHub *github.ForgeConfig `variant:"github"`
	}]
}

func (c ForgeConfig) NewQueue(id, version string) (provider.Queue, error) {
	switch spec := c.Value().(type) {
	case *github.ForgeConfig:
		return spec.NewQueue(id, version)
	default:
		return nil, provider.ErrInvalidConfig
	}
}

type SlotConfig struct {
	hrcconfig.Sum[hrcconfig.Slot, struct {
		Pgroup *slotpgroup.Config `variant:"pgroup"`
	}]
}

func (c SlotConfig) executionConfig(root string) ([]byte, error) {
	switch spec := c.Value().(type) {
	case *slotpgroup.Config:
		config := *spec
		if config.Dir == "" {
			config.Dir = filepath.Join(root, "slots")
		}
		var err error
		config.Dir, err = filepath.Abs(config.Dir)
		if err != nil {
			return nil, err
		}
		return json.Marshal(config)
	default:
		return nil, errors.New("unsupported slot configuration")
	}
}

type SpaceConfig struct {
	hrcconfig.Sum[hrcconfig.Space, struct {
		Host *spacehost.Config `variant:"host"`
	}]
}

func (c SpaceConfig) NewSpace(dirs Directories, pool SlotConfig, capacity int, providers map[string][]byte) (space.Space, error) {
	switch spec := c.Value().(type) {
	case *spacehost.Config:
		root := spec.Dir
		if root == "" {
			root = filepath.Join(dirs.Spaces, string(spec.ID))
		}
		root, err := filepath.Abs(root)
		if err != nil {
			return nil, err
		}
		slotConfig, err := pool.executionConfig(root)
		if err != nil {
			return nil, err
		}
		config := &space.Config{
			Capacity:   uint64(capacity),
			Ephemeral:  pool.Base().Ephemeral,
			Slot:       pool.Base().Type,
			SlotConfig: slotConfig,
			Providers:  providers,
		}
		return spacehost.New(string(spec.ID), root, []string{"hrcm"}, config)
	default:
		return nil, space.ErrInvalidConfig
	}
}

// executionConfigs supplies opaque provider settings to the execution process.
func executionConfigs(dirs Directories) (map[string][]byte, error) {
	runtimeDir, err := filepath.Abs(dirs.Runtime)
	if err != nil {
		return nil, err
	}
	logRoot, err := filepath.Abs(dirs.Logs)
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(github.WorkerConfig{Dir: runtimeDir, LogRoot: logRoot})
	if err != nil {
		return nil, err
	}
	return map[string][]byte{"github": data}, nil
}

var (
	help    = flag.Bool("help", false, "Show this help")
	version = flag.Bool("version", false, "Show hrcd version")
	config  = flag.String("config", "", "Path to the JSON or YAML config file")
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

	if *config == "" {
		flag.Usage()
		os.Exit(1)
	}

	cfg, err := LoadConfig(*config)
	if err != nil {
		slog.Error("Could't read config", "err", err)
		os.Exit(1)
	}

	if err := runDaemon(cfg); err != nil {
		slog.Error("Daemon stopped", "err", err)
		os.Exit(1)
	}
}

func runDaemon(cfg *Config) (err error) {
	demands := make(Demands, len(cfg.Spaces))
	if err := demands.Init(cfg); err != nil {
		return fmt.Errorf("calculate space capacities: %w", err)
	}
	defer demands.Close()

	providers, err := executionConfigs(cfg.Directories)
	if err != nil {
		return err
	}
	visor, err := NewSpaceVisor(cfg, demands, providers)
	if err != nil {
		return fmt.Errorf("create spacevisor: %w", err)
	}
	defer func() { err = errors.Join(err, visor.Close()) }()

	controller, err := NewController(cfg, demands)
	if err != nil {
		return fmt.Errorf("create controller: %w", err)
	}
	defer func() { err = errors.Join(err, controller.Close()) }()

	// Service managers stop with SIGTERM; Windows delivers console close and
	// shutdown events as SIGTERM as well.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := controller.Serve(ctx)

dispatch:
	for {
		select {
		case job, ok := <-jobs:
			if !ok {
				break dispatch
			}
			visor.Submit(ctx, job)
		case <-visor.Failed():
			break dispatch
		case <-ctx.Done():
			break dispatch
		}
	}

	// Stop acquisition, request graceful shutdown for every current task,
	// and wait for execution and Handle cleanup before closing the spaces.
	cancel()
	for _, job := range controller.Jobs() {
		job.Shutdown()
	}

	return nil
}
