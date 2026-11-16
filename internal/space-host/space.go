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
	"sync/atomic"
	"time"

	"dimidiumlabs.io/hrc/internal/config"
	"dimidiumlabs.io/hrc/internal/provider"
	"dimidiumlabs.io/hrc/internal/space"
	"google.golang.org/protobuf/proto"
)

// Config describes a host space.
type Config struct {
	config.Space

	Dir string `json:"dir"`
}

func (c *Config) Validate() error {
	if c.Type == "" {
		c.Type = "host"
	}
	if c.Type != "host" {
		return fmt.Errorf("unknown space type %q", c.Type)
	}
	if err := c.ID.Validate(); err != nil {
		return fmt.Errorf("space: %w", err)
	}
	if err := c.Resources.Validate(); err != nil {
		return err
	}
	return nil
}

type Space struct {
	mu        sync.Mutex
	status    atomic.Uint32
	name      string
	root      string
	command   []string
	config    *space.Config
	client    *space.Client
	cmd       *exec.Cmd
	done      chan struct{}
	waitErr   error
	failure   error
	closed    bool
	socketDir string
}

// New prepares configuration only. Run launches hrcs on the first Job.
func New(name, root string, command []string, config *space.Config) (*Space, error) {
	if name == "" || root == "" || len(command) == 0 {
		return nil, fmt.Errorf("%w: space name, root and scheduler command are required", space.ErrInvalidConfig)
	}
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, errors.Join(space.ErrInvalidConfig, err)
	}
	command = slices.Clone(command)
	path, err := exec.LookPath(command[0])
	if err != nil {
		return nil, fmt.Errorf("%w: find scheduler: %w", space.ErrInvalidConfig, err)
	}
	command[0], err = filepath.Abs(path)
	if err != nil {
		return nil, errors.Join(space.ErrInvalidConfig, err)
	}
	return &Space{name: name, root: root, command: command, config: proto.CloneOf(config)}, nil
}

func (s *Space) Name() string { return s.name }
func (s *Space) Cwd() string  { return s.root }
func (*Space) Type() string   { return "host" }
func (s *Space) Status() space.Status {
	status := space.Status(s.status.Load())
	if status == space.Ready && s.mu.TryLock() {
		if space.Status(s.status.Load()) == space.Ready && s.client != nil {
			select {
			case <-s.client.Done():
				s.status.Store(uint32(space.Failed))
			default:
			}
		}
		s.mu.Unlock()
		status = space.Status(s.status.Load())
	}
	return status
}

func (s *Space) State(ctx context.Context) (space.State, error) {
	state := space.State{}
	status := s.Status()
	if status != space.Ready && status != space.Failed {
		return state, nil
	}
	s.mu.Lock()
	client, failure := s.client, s.failure
	s.mu.Unlock()
	if client == nil {
		return state, failure
	}
	snapshot, err := client.State(ctx)
	if err != nil {
		return state, err
	}
	return snapshot, nil
}

func (s *Space) Run(ctx context.Context, job provider.Job) (provider.Run, error) {
	payload, err := job.Marshal()
	if err != nil {
		return nil, err
	}
	client, err := s.connect(ctx)
	if err != nil {
		return nil, err
	}
	return client.Start(ctx, job.Type(), payload)
}

func (s *Space) connect(ctx context.Context) (*space.Client, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, space.ErrClosed
	}
	if s.failure != nil {
		return nil, s.failure
	}
	if s.client != nil {
		select {
		case <-s.client.Done():
			return nil, s.client.Err()
		default:
			return s.client, nil
		}
	}
	s.status.Store(uint32(space.Starting))
	client, err := s.start(ctx)
	if err != nil {
		if ctx.Err() != nil {
			s.status.Store(uint32(space.Idle))
			return nil, ctx.Err()
		}
		s.status.Store(uint32(space.Failed))
		s.failure = fmt.Errorf("%w: %w", space.ErrUnavailable, err)
		return nil, s.failure
	}
	s.client = client
	s.status.Store(uint32(space.Ready))
	return client, nil
}

func (s *Space) start(ctx context.Context) (*space.Client, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(s.root, 0o700); err != nil {
		return nil, err
	}
	// AF_UNIX path lengths are short, independently of the workspace path.
	dir, err := os.MkdirTemp("", "hrc-space-")
	if err != nil {
		return nil, err
	}
	s.socketDir = dir
	socket := filepath.Join(dir, "scheduler.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		os.RemoveAll(dir)
		return nil, err
	}
	defer listener.Close()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	stop := context.AfterFunc(ctx, func() { listener.Close() })
	defer stop()
	args := append(slices.Clone(s.command[1:]), "-connect", socket)
	s.cmd = exec.Command(s.command[0], args...)
	configureProcess(s.cmd)
	s.cmd.Dir = s.root
	s.cmd.Stdout, s.cmd.Stderr = os.Stdout, os.Stderr
	if err := s.cmd.Start(); err != nil {
		os.RemoveAll(dir)
		return nil, err
	}
	s.done = make(chan struct{})
	go func() {
		s.waitErr = s.cmd.Wait()
		close(s.done)
		listener.Close()
	}()
	conn, err := listener.Accept()
	if err == nil {
		var client *space.Client
		// On disconnect hrcs drains its Runs before exiting. Wait for that
		// shutdown before releasing handles; an abnormal exit remains a lost Run.
		owned := &processConn{Conn: conn, done: s.done, kill: s.cmd.Process.Kill}
		client, err = space.NewClient(ctx, owned, s.config)
		if err == nil {
			return client, nil
		}
	}
	// Never leave an unconnected scheduler behind, including canceled startup.
	s.cmd.Process.Kill()
	<-s.done
	os.RemoveAll(dir)
	if ctx.Err() != nil {
		err = ctx.Err()
	}
	return nil, fmt.Errorf("start scheduler: %w", errors.Join(err, s.waitErr))
}

type processConn struct {
	net.Conn
	done <-chan struct{}
	kill func() error
	once sync.Once
	err  error
}

func (c *processConn) Close() error {
	c.once.Do(func() {
		c.err = c.Conn.Close()
		select {
		case <-c.done:
		case <-time.After(30 * time.Second):
			c.err = errors.Join(c.err, c.kill())
			<-c.done
		}
	})
	return c.err
}

func (s *Space) Destroy() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	s.status.Store(uint32(space.Destroying))
	if s.client != nil {
		s.client.Close()
	}
	var err error
	if s.done != nil {
		select {
		case <-s.done:
		case <-time.After(30 * time.Second):
			err = errors.New("scheduler exceeded shutdown timeout")
			err = errors.Join(err, s.cmd.Process.Kill())
			<-s.done
		}
		err = errors.Join(err, s.waitErr)
	}
	if s.socketDir != "" {
		err = errors.Join(err, os.RemoveAll(s.socketDir))
	}
	if err != nil {
		s.status.Store(uint32(space.Failed))
		return errors.Join(space.ErrUnknown, err)
	}
	s.status.Store(uint32(space.Destroyed))
	return err
}
