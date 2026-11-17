// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

// Package spacehost owns a directory and a separate host scheduler process.
package spacehost

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"dimidiumlabs.io/hrc/internal/provider"
	platformruntime "dimidiumlabs.io/hrc/internal/runtime"
	"dimidiumlabs.io/hrc/internal/space"
	"google.golang.org/protobuf/proto"
)

type SpaceHost struct {
	space.Daemon

	mu sync.Mutex

	cmd    *exec.Cmd
	name   string
	args   []string
	config *space.Config

	rootDir, sockDir string
}

var (
	_ space.Space          = (*SpaceHost)(nil)
	_ space.DaemonDelegate = (*SpaceHost)(nil)
)

// New prepares configuration only. Run launches hrcs on the first Job.
func New(name, root string, command []string, config *space.Config) (*SpaceHost, error) {
	if name == "" || root == "" || len(command) == 0 {
		return nil, fmt.Errorf("%w: space name, root and scheduler command are required", space.ErrInvalidConfig)
	}

	root, err := filepath.Abs(root)
	if err != nil {
		return nil, errors.Join(space.ErrInvalidConfig, err)
	}

	command = slices.Clone(command)
	path, err := platformruntime.LookPath(command[0])
	if err != nil {
		return nil, fmt.Errorf("%w: find scheduler: %w", space.ErrInvalidConfig, err)
	}
	command[0] = path

	space := SpaceHost{
		name:    name,
		rootDir: root,
		config:  proto.CloneOf(config),
		args:    command,
	}
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

// start launches a scheduler after connect has retired the previous generation.
// The caller holds s.mu and handles lifecycle status and error classification.
func (s *SpaceHost) start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.MkdirAll(s.rootDir, 0o700); err != nil {
		return err
	}

	// AF_UNIX path lengths are short, independently of the workspace path.
	dir, err := os.MkdirTemp("", "hrc-space-")
	if err != nil {
		return err
	}

	s.sockDir = dir
	socket := filepath.Join(dir, "scheduler.sock")

	listener, err := net.Listen("unix", socket)
	if err != nil {
		return err
	}
	defer listener.Close()

	startup, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	stop := context.AfterFunc(startup, func() { listener.Close() })
	defer stop()

	args := append(slices.Clone(s.args[1:]), "-connect", socket)
	s.cmd = exec.Command(s.args[0], args...)
	s.cmd.Dir = s.rootDir
	s.cmd.Stdout, s.cmd.Stderr = os.Stdout, os.Stderr
	configureProcess(s.cmd)

	if err := s.cmd.Start(); err != nil {
		return err
	}

	conn, err := listener.Accept()
	if err == nil {
		err = s.Daemon.Connect(startup, conn, s.config)
		if err == nil {
			return nil
		}
	}

	if startup.Err() != nil {
		err = startup.Err()
	}

	return fmt.Errorf("start scheduler: %w", err)
}

// cleanup releases this generation's resources so another scheduler can start.
// The caller holds s.mu and publishes lifecycle status. The process outcome is
// separate from cleanup errors: rollback and replacement may discard an exit
// error, but must not discard a failure to release resources.
func (s *SpaceHost) cleanup() (exitErr, err error) {
	err = s.Daemon.Close()
	if cmd := s.cmd; cmd != nil && cmd.Process != nil && cmd.ProcessState == nil {
		// Wait belongs to this cleanup operation. Its goroutine only delivers
		// the result; cleanup always receives it before releasing the process.
		waited := make(chan error, 1)
		go func(result chan<- error) { result <- cmd.Wait() }(waited)

		if s.Status() != space.Connecting {
			// Closing RPC lets a connected scheduler drain its Runs first.
			timer := time.NewTimer(30 * time.Second)
			select {
			case exitErr = <-waited:
				waited = nil
			case <-timer.C:
			}
			timer.Stop()
		}
		if waited != nil {
			killErr := cmd.Process.Kill()
			exitErr = <-waited
			// Windows may reject TerminateProcess for an already-exited
			// process before Wait reaps it. A confirmed process outcome means
			// there is no process left to kill, regardless of that error.
			if cmd.ProcessState == nil && killErr != nil && !errors.Is(killErr, os.ErrProcessDone) {
				err = errors.Join(err, killErr)
			}
		}
	}
	if s.sockDir != "" {
		err = errors.Join(err, os.RemoveAll(s.sockDir))
	}
	if err == nil {
		s.cmd, s.sockDir = nil, ""
	}
	return exitErr, err
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
