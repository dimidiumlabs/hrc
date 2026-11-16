// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package slotpgroup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"time"

	platformruntime "dimidiumlabs.io/hrc/internal/runtime"
)

// PipeDirection is relative to the child process.
type PipeDirection uint16

const (
	PipeRead PipeDirection = iota + 1
	PipeWrite
)

// Command describes a child process in a pgroup.
// Pipes requests additional anonymous pipes. Their child descriptors/handles
// are appended to Args in order. The caller owns the returned parent ends;
// Process.Close also closes them. No host descriptors cross this interface.
type Command struct {
	Path, Dir, EnvFile string
	Args, Env          []string
	Stdout, Stderr     io.Writer
	Input              bool
	Pipes              []PipeDirection
}
type Process interface {
	Stdin() io.WriteCloser
	Pipes() []io.ReadWriteCloser
	Wait() (int, error)
	Kill() error
	Close() error
}

// process is one child in a pgroup. Its descendants belong to the group,
// independently of this child's lifetime.
type process struct {
	cmd    *exec.Cmd
	group  *pgroup
	waited bool
	input  io.WriteCloser
	pipes  []io.ReadWriteCloser
}

// Start starts a child in the group. If the child cannot join its tree, it is
// killed and reaped before Start returns. A closed or nil group cannot start
// children.
func (g *pgroup) Start(ctx context.Context, config Command) (*process, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if g == nil {
		return nil, errors.New("process group is required")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return nil, os.ErrClosed
	}
	if g.id == "" {
		return nil, errors.New("uninitialized process group")
	}
	p := &process{group: g}
	p.cmd = exec.Command(config.Path, config.Args...)
	env, err := environment(config.EnvFile, config.Env)
	if err != nil {
		return nil, err
	}
	p.cmd.Dir, p.cmd.Env = config.Dir, g.environment(env)
	p.cmd.Stdout, p.cmd.Stderr = config.Stdout, config.Stderr
	p.cmd.WaitDelay = 5 * time.Second
	var childPipes []*os.File
	defer func() {
		for _, f := range childPipes {
			f.Close()
		}
	}()
	started := false
	defer func() {
		if !started {
			for _, f := range p.pipes {
				f.Close()
			}
		}
	}()
	for _, direction := range config.Pipes {
		if direction != PipeRead && direction != PipeWrite {
			return nil, os.ErrInvalid
		}
		read, write, err := platformruntime.Pipe()
		if err != nil {
			return nil, err
		}
		child, parent := read, write
		if direction == PipeWrite {
			child, parent = write, read
		}
		childPipes = append(childPipes, child)
		p.pipes = append(p.pipes, parent)
	}
	defer p.closeInherited()
	if err := p.configure(childPipes); err != nil {
		return nil, fmt.Errorf("configure process: %w", err)
	}
	if config.Input {
		p.input, err = p.cmd.StdinPipe()
		if err != nil {
			return nil, err
		}
	}
	if err := p.cmd.Start(); err != nil {
		if p.input != nil {
			p.input.Close()
		}
		return nil, err
	}
	// Join the tree before the caller hands the child any work, so that no
	// descendant can start outside it.
	if err := p.attach(); err != nil {
		killErr := p.Kill()
		_, waitErr := p.Wait()
		return nil, errors.Join(fmt.Errorf("attach process: %w", err), killErr, waitErr)
	}
	if err := ctx.Err(); err != nil {
		p.Kill()
		p.Wait()
		p.Close()
		return nil, err
	}
	started = true
	return p, nil
}

func (p *process) Pipes() []io.ReadWriteCloser { return p.pipes }

func (p *process) Stdin() io.WriteCloser { return p.input }

// Wait waits for the child, once. An exit status is not an error: it is the
// caller's to interpret from the returned exit code.
func (p *process) Wait() (int, error) {
	err := p.cmd.Wait()
	p.waited = true
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		err = nil
	}
	if p.cmd.ProcessState == nil {
		return -1, err
	}
	return p.cmd.ProcessState.ExitCode(), err
}

// Kill terminates only this child. Its descendants remain in the pgroup.
func (p *process) Kill() error {
	err := p.cmd.Process.Kill()
	if errors.Is(err, os.ErrProcessDone) {
		return nil
	}
	return err
}

// Close terminates a child that has not been waited for. It does not close the
// pgroup or kill descendants. The caller must still Wait to reap the child.
func (p *process) Close() error {
	for _, f := range p.pipes {
		f.Close()
	}
	if !p.waited {
		return p.Kill()
	}
	return nil
}
