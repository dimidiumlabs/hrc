// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package runtime

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"

	hrcconfig "dimidiumlabs.io/hrc/internal/config"
)

// Pool owns direct children, not OS groups, provider state or restart policy.
// Its zero value is ready for use. Close prevents starts and joins all children.
type Pool struct {
	mu       sync.Mutex
	children map[*Child]struct{}
	closed   bool
}

func (p *Pool) Children() []*Child {
	p.mu.Lock()
	defer p.mu.Unlock()
	children := make([]*Child, 0, len(p.children))
	for c := range p.children {
		children = append(children, c)
	}
	return children
}

func (p *Pool) Start(ctx context.Context, spec ChildSpec) (*Child, error) {
	return launchChild(ctx, spec, p)
}

func launchChild(ctx context.Context, spec ChildSpec, p *Pool) (child *Child, err error) {
	timeout := spec.StartupTimeout
	if timeout <= 0 {
		timeout = hrcconfig.ProcessStartTimeout
	}
	startup, cancel := context.WithTimeout(ctx, timeout)
	c := &Child{pool: p, startupCancel: cancel, started: make(chan struct{}), done: make(chan struct{})}
	if p != nil {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			cancel()
			return nil, os.ErrClosed
		}
		if p.children == nil {
			p.children = make(map[*Child]struct{})
		}
		p.children[c] = struct{}{}
		p.mu.Unlock()
	}
	defer func() {
		cancel()
		if !c.launched {
			c.code, c.waitErr = -1, err
			c.exitErr = err
			close(c.done)
		}
		close(c.started)
		if err != nil {
			_, cleanup := c.Close(0)
			err = errors.Join(err, cleanup)
		}
	}()
	if err := startup.Err(); err != nil {
		return nil, err
	}
	command := spec.Command
	if spec.SocketArgs == nil {
		if err := c.startProcess(startup, command); err != nil {
			return nil, err
		}
		return c, nil
	}
	dir, err := os.MkdirTemp("", "hrc-child-")
	if err != nil {
		return nil, err
	}
	c.socket = filepath.Join(dir, "rpc.sock")
	listener, err := net.Listen("unix", c.socket)
	if err != nil {
		return nil, err
	}
	defer listener.Close()
	abort := context.AfterFunc(startup, func() { listener.Close() })
	defer abort()
	command.Args = append(append([]string(nil), command.Args...), spec.SocketArgs(c.socket)...)
	if err := c.startProcess(startup, command); err != nil {
		return nil, err
	}
	// Interrupt Accept on early exit without touching buffered connection data.
	stopExit := make(chan struct{})
	defer close(stopExit)
	go func() {
		select {
		case <-c.done:
			listener.Close()
		case <-stopExit:
		}
	}()
	c.conn, err = listener.Accept()
	if err != nil {
		if startup.Err() != nil {
			return nil, startup.Err()
		}
		select {
		case <-c.done:
			if c.exitErr != nil {
				return nil, c.exitErr
			}
			return nil, errors.New("child exited before connecting")
		default:
			return nil, err
		}
	}
	if err := startup.Err(); err != nil {
		return nil, err
	}
	return c, nil
}

func (p *Pool) Close() error {
	p.mu.Lock()
	p.closed = true
	children := make([]*Child, 0, len(p.children))
	for c := range p.children {
		children = append(children, c)
		c.startupCancel()
	}
	p.mu.Unlock()
	results := make(chan error, len(children))
	for _, c := range children {
		go func() { exit, cleanup := c.Close(hrcconfig.ProcessExitTimeout); results <- errors.Join(exit, cleanup) }()
	}
	var err error
	for range children {
		err = errors.Join(err, <-results)
	}
	return err
}
