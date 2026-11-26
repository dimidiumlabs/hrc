// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

// Package spacehost owns a directory and a separate host monitor process.
package spacehost

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"

	hrcconfig "dimidiumlabs.io/hrc/internal/config"
	"dimidiumlabs.io/hrc/internal/provider"
	"dimidiumlabs.io/hrc/internal/runtime"
	"dimidiumlabs.io/hrc/internal/space"
	"google.golang.org/protobuf/proto"
)

type SpaceHost struct {
	space.Daemon

	mu sync.Mutex

	child  *runtime.Child
	pool   *runtime.Pool
	name   string
	args   []string
	config *space.Config

	rootDir     string
	initialized bool
}

var (
	_ space.Space          = (*SpaceHost)(nil)
	_ space.DaemonDelegate = (*SpaceHost)(nil)
)

// New prepares configuration only. Run launches hrcm on the first Job.
func New(name, root string, command []string, config *space.Config, pool *runtime.Pool) (*SpaceHost, error) {
	if name == "" || root == "" || len(command) == 0 || config == nil {
		return nil, fmt.Errorf("%w: space name, root, monitor command and configuration are required", space.ErrInvalidConfig)
	}

	root, err := filepath.Abs(root)
	if err != nil {
		return nil, errors.Join(space.ErrInvalidConfig, err)
	}

	command = slices.Clone(command)
	path, err := runtime.LookPath(command[0])
	if err != nil {
		return nil, fmt.Errorf("%w: find monitor: %w", space.ErrInvalidConfig, err)
	}
	command[0] = path

	space := SpaceHost{
		name:    name,
		rootDir: root,
		config:  proto.CloneOf(config),
		args:    command,
		pool:    pool,
	}
	space.config.SpaceType = space.Type()
	space.Daemon.Delegate = &space
	return &space, nil
}

func (*SpaceHost) Type() string   { return "host" }
func (s *SpaceHost) Name() string { return s.name }

func (s *SpaceHost) Cwd() string { return s.rootDir }

func (s *SpaceHost) Run(ctx context.Context, job provider.Job) (provider.Run, error) {
	if err := s.connect(ctx); err != nil {
		return nil, err
	}

	payload, err := job.Marshal()
	if err != nil {
		return nil, err
	}

	return s.Daemon.Start(ctx, job.Type(), payload)
}

func (s *SpaceHost) connect(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.Status() == space.Ready {
		return nil
	}

	_, err := s.cleanup()
	if err == nil {
		s.SetStatus(space.Connecting, nil)
		err = s.start(ctx)
	}
	if err == nil {
		s.SetStatus(space.Ready, nil)
		err = s.Err()
	}
	if err == nil {
		return nil
	}

	_, cleanupErr := s.cleanup()
	ctxErr := ctx.Err()
	if cleanupErr == nil {
		if ctxErr != nil {
			s.SetStatus(space.Idle, nil)
			return ctxErr
		}
	}
	failure := fmt.Errorf("%w: %w", space.ErrUnavailable, errors.Join(err, ctxErr, cleanupErr))
	s.SetStatus(space.Broken, failure)
	return failure
}

// start launches a monitor after connect has retired the previous generation.
// The caller holds s.mu and handles lifecycle status and error classification.
func (s *SpaceHost) start(ctx context.Context) error {
	// Only the launch cwd is created here; hrcm prepares the space contents.
	if err := os.MkdirAll(s.rootDir, 0700); err != nil {
		return err
	}
	startup, cancel := context.WithTimeout(ctx, hrcconfig.ProcessStartTimeout)
	defer cancel()
	var err error
	s.child, err = s.pool.Start(startup, runtime.ChildSpec{
		Command: runtime.Command{Path: s.args[0], Args: s.args[1:], Dir: s.rootDir, Stdout: os.Stdout, Stderr: os.Stderr},
	})
	if err != nil {
		return fmt.Errorf("start monitor: %w", err)
	}
	if err := s.Daemon.Connect(startup, s.child.Connection(), s.config); err != nil {
		return err
	}
	s.initialized = true
	return nil
}

// cleanup retires the protocol before releasing its child through the pool.
// The caller holds s.mu and publishes lifecycle status.
func (s *SpaceHost) cleanup() (exitErr, err error) {
	err = s.Daemon.Close()
	if s.child == nil {
		return nil, err
	}
	grace := hrcconfig.MonitorExitTimeout
	if !s.initialized {
		grace = 0
	}
	exitErr, cleanup := s.child.Close(grace)
	if cleanup == nil {
		s.child = nil
		s.initialized = false
	}
	return exitErr, errors.Join(err, cleanup)
}

// Disconnected delegates connection-loss cleanup to the environment owner.
func (s *SpaceHost) Disconnected(done <-chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.Daemon.Done() != done || s.Status() != space.Broken {
		return
	}

	failure := s.Err()
	exitErr, err := s.cleanup()
	if exitErr != nil || err != nil {
		s.SetStatus(space.Broken, errors.Join(failure, exitErr, err))
	}
}

func (s *SpaceHost) Destroy() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.SetStatus(space.Destroying, nil)

	exitErr, cleanupErr := s.cleanup()
	err := errors.Join(exitErr, cleanupErr)
	if err != nil {
		failure := errors.Join(space.ErrUnknown, err)
		s.SetStatus(space.Broken, failure)
		return failure
	}
	s.SetStatus(space.Idle, nil)
	return err
}
