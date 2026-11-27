// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	goruntime "runtime"
	"syscall"

	hrcconfig "dimidiumlabs.io/hrc/internal/config"
	"dimidiumlabs.io/hrc/internal/provider"
	"dimidiumlabs.io/hrc/internal/runtime"
	slotpgroup "dimidiumlabs.io/hrc/internal/slot-pgroup"
	"dimidiumlabs.io/hrc/internal/space"
	spacehost "dimidiumlabs.io/hrc/internal/space-host"
)

// ForgeConfig reads placement only. The complete original document belongs to
// the provider, including its schema, credentials, defaults and validation.
type ForgeConfig struct {
	placement hrcconfig.Provider
	raw       []byte
}

func (c *ForgeConfig) UnmarshalJSON(data []byte) error {
	if trimmed := bytes.TrimSpace(data); len(trimmed) == 0 || trimmed[0] != '{' {
		return fmt.Errorf("forge configuration must be an object")
	}
	var placement hrcconfig.Provider
	if err := json.Unmarshal(data, &placement, json.RejectUnknownMembers(false)); err != nil {
		return err
	}
	c.placement = placement
	c.raw = bytes.Clone(data)
	return nil
}

func (c ForgeConfig) MarshalJSON() ([]byte, error) { return bytes.Clone(c.raw), nil }
func (c *ForgeConfig) Base() *hrcconfig.Provider   { return &c.placement }

func (c *ForgeConfig) Validate() error {
	if c.placement.Type == "" {
		c.placement.Type = "github"
	}
	if c.placement.Type != "github" {
		return fmt.Errorf("%w: unknown configuration type %q", provider.ErrInvalidConfig, c.placement.Type)
	}
	return nil
}

func (c ForgeConfig) NewQueue(id, version string, pool *runtime.Pool, onError func(error)) (provider.Queue, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	var user *runtime.User
	if goruntime.GOOS != "windows" {
		var err error
		user, err = runtime.LookupUser("hrc")
		if err != nil {
			return nil, err
		}
	}
	return &provider.ProcessQueue{
		User: user,
		Pool: pool, OnError: onError,
		Command: []string{"hrcp-github-queue"}, Config: bytes.Clone(c.raw),
		Instance: id, Version: version, Name: string(c.placement.ID),
	}, nil
}

type SlotConfig struct {
	hrcconfig.Sum[hrcconfig.Slot, struct {
		Pgroup *slotpgroup.Config `variant:"pgroup"`
	}]
}

func (c SlotConfig) executionConfig() ([]byte, error) {
	switch spec := c.Value().(type) {
	case *slotpgroup.Config:
		return json.Marshal(spec)
	default:
		return nil, errors.New("unsupported slot configuration")
	}
}

type SpaceConfig struct {
	hrcconfig.Sum[hrcconfig.Space, struct {
		Host *spacehost.Config `variant:"host"`
	}]
}

func (c SpaceConfig) NewSpace(pool SlotConfig, capacity int, providers map[string][]byte, processes *runtime.Pool) (space.Space, error) {
	switch spec := c.Value().(type) {
	case *spacehost.Config:
		user, err := runtime.LookupUser(spec.User)
		if err != nil {
			return nil, fmt.Errorf("space %q: %w", spec.ID, err)
		}
		command := runtime.Command{Path: "hrcm", Dir: user.HomeDir, User: user, Env: user.Environ(), Stdout: os.Stdout, Stderr: os.Stderr}
		slotConfig, err := pool.executionConfig()
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
		return spacehost.New(string(spec.ID), command, config, processes)
	default:
		return nil, space.ErrInvalidConfig
	}
}

// Built-in provider processes are an implementation detail, not user configuration.
func executionConfigs(dirs Directories) (map[string][]byte, error) {
	runtime, err := filepath.Abs(dirs.Runtime)
	if err != nil {
		return nil, err
	}
	settings, err := json.Marshal(struct{ Dir string }{runtime})
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(provider.ProcessConfig{
		Command: []string{"hrcp-github-worker"}, Config: settings,
	})
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
	if goruntime.GOOS != "windows" && os.Geteuid() != 0 {
		return errors.New("hrcd must run as root on Unix")
	}
	demands := make(Demands, len(cfg.Spaces))
	if err := demands.Init(cfg); err != nil {
		return fmt.Errorf("calculate space capacities: %w", err)
	}
	defer demands.Close()

	providers, err := executionConfigs(cfg.Directories)
	if err != nil {
		return err
	}
	pool := &runtime.Pool{}
	defer func() { err = errors.Join(err, pool.Close()) }()
	controller, err := NewController(cfg, demands, providers, pool)
	if err != nil {
		return fmt.Errorf("create controller: %w", err)
	}

	// Service managers stop with SIGTERM; Windows delivers console close and
	// shutdown events as SIGTERM as well.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	return controller.Run(ctx)
}
