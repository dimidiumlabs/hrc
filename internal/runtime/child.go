// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package runtime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

// ChildSpec describes a direct child with an optional parent-hosted control socket.
// A nil SocketArgs disables the socket. Otherwise it supplies protocol-specific
// arguments; runtime does not assume gRPC or -connect.
type ChildSpec struct {
	Command        Command
	SocketArgs     func(string) []string
	StartupTimeout time.Duration
}

// Child owns one process generation. Done means that Wait has completed, not
// that an RPC outcome has been consumed. Process exit does not discard buffered
// transport data. The adapter closes the child after retiring its protocol.
type Child struct {
	pool          *Pool
	startupCancel context.CancelFunc
	started       chan struct{}
	done          chan struct{}
	cmd           *exec.Cmd
	launched      bool
	group         *Group
	input         io.WriteCloser
	pipes         []io.ReadWriteCloser
	waitErr       error
	conn          net.Conn
	socket        string
	code          int
	exitErr       error
	closeMu       sync.Mutex
	processClosed bool
	cleanupErr    error
}

// ExitError preserves an unsuccessful OS exit independently of transport errors.
type ExitError struct{ Code int }

func (e *ExitError) Error() string { return fmt.Sprintf("child exited with status %d", e.Code) }
func (e *ExitError) ExitCode() int { return e.Code }

func (c *Child) Connection() net.Conn  { <-c.started; return c.conn }
func (c *Child) SocketPath() string    { <-c.started; return c.socket }
func (c *Child) Done() <-chan struct{} { return c.done }
func (c *Child) Err() error {
	select {
	case <-c.done:
		return c.exitErr
	default:
		return nil
	}
}

// Close retires only this child, never its Group. Startup cancellation is
// separate from the lifetime of a connected process. Filesystem cleanup may be
// retried; the process is reaped once, including during startup rollback.
func (c *Child) Close(grace time.Duration) (exitErr, cleanupErr error) {
	c.startupCancel()
	<-c.started
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	if c.conn != nil {
		c.conn.Close()
	}
	if !c.processClosed {
		if c.launched {
			timer := time.NewTimer(grace)
			select {
			case <-c.done:
			case <-timer.C:
				err := c.Kill()
				// Kill may race an already-finished Wait. Its outcome is authoritative.
				select {
				case <-c.done:
				default:
					c.cleanupErr = errors.Join(c.cleanupErr, err)
				}
				<-c.done
			}
			timer.Stop()
		}
		if c.input != nil {
			c.input.Close()
		}
		for _, f := range c.pipes {
			f.Close()
		}
		c.processClosed = true
	}
	cleanupErr = c.cleanupErr
	if c.socket != "" {
		cleanupErr = errors.Join(cleanupErr, os.RemoveAll(filepath.Dir(c.socket)))
	}
	if cleanupErr == nil && c.pool != nil {
		c.pool.mu.Lock()
		delete(c.pool.children, c)
		c.pool.mu.Unlock()
	}
	return c.Err(), cleanupErr
}
