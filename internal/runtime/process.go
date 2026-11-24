// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package runtime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"

	hrcconfig "dimidiumlabs.io/hrc/internal/config"
)

// PipeDirection is relative to the child process.
type PipeDirection uint16

const (
	PipeRead PipeDirection = iota + 1
	PipeWrite
)

// Command describes a direct child, optionally attached to a Group.
// Pipes requests anonymous pipes; child descriptors/handles are appended to Args.
// Child.Close closes their parent ends. Env follows exec.Cmd semantics.
type Command struct {
	Group          *Group
	Path, Dir      string
	Args, Env      []string
	Stdout, Stderr io.Writer
	Input          bool
	Pipes          []PipeDirection
}

// StartChild starts a child without a control socket. The caller owns its cleanup.
func StartChild(ctx context.Context, command Command) (*Child, error) {
	return launchChild(ctx, ChildSpec{Command: command}, nil)
}

// Start starts a child in the group. Closing the child never closes the group.
func (g *Group) Start(ctx context.Context, command Command) (*Child, error) {
	if g == nil {
		return nil, errors.New("process group is required")
	}
	command.Group = g
	return StartChild(ctx, command)
}

// startProcess is Child's platform launch implementation, not a second owner.
// On failure after exec.Start, the caller retires the same Child through Close.
func (p *Child) startProcess(ctx context.Context, config Command) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	g := config.Group
	if g != nil {
		g.mu.Lock()
		defer g.mu.Unlock()
		if g.closed {
			return os.ErrClosed
		}
		if g.id == "" {
			return errors.New("uninitialized process group")
		}
	}
	path, err := LookPath(config.Path)
	if err != nil {
		return err
	}
	p.group = g
	p.cmd = exec.Command(path, config.Args...)
	p.cmd.Dir, p.cmd.Env = config.Dir, config.Env
	if g != nil {
		p.cmd.Env = g.environment(config.Env)
	}
	p.cmd.Stdout, p.cmd.Stderr = config.Stdout, config.Stderr
	p.cmd.WaitDelay = hrcconfig.ProcessWaitDelay
	var childPipes []*os.File
	defer func() {
		for _, f := range childPipes {
			f.Close()
		}
	}()
	for _, direction := range config.Pipes {
		if direction != PipeRead && direction != PipeWrite {
			return os.ErrInvalid
		}
		read, write, err := Pipe()
		if err != nil {
			return err
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
		return fmt.Errorf("configure process: %w", err)
	}
	if config.Input {
		var err error
		p.input, err = p.cmd.StdinPipe()
		if err != nil {
			return err
		}
	}
	if err := p.cmd.Start(); err != nil {
		return err
	}
	p.launched = true
	go p.reap()
	// Attach before handing the adapter any work, so descendants inherit the group.
	if err := p.attach(); err != nil {
		return fmt.Errorf("attach process: %w", err)
	}
	return ctx.Err()
}

func (p *Child) Pipes() []io.ReadWriteCloser { return p.pipes }
func (p *Child) Stdin() io.WriteCloser       { return p.input }

// reap is the only owner of exec.Cmd.Wait, including during startup rollback.
func (p *Child) reap() {
	p.waitErr = p.cmd.Wait()
	var exit *exec.ExitError
	if errors.As(p.waitErr, &exit) {
		p.waitErr = nil
	}
	p.code = -1
	if p.cmd.ProcessState != nil {
		p.code = p.cmd.ProcessState.ExitCode()
	}
	p.exitErr = p.waitErr
	if p.exitErr == nil && p.code != 0 {
		p.exitErr = &ExitError{Code: p.code}
	}
	close(p.done)
}

// Wait joins the existing reaper; repeated or concurrent calls are safe.
// Nonzero status is returned as a code; Err also exposes it as ExitError.
func (p *Child) Wait() (int, error) {
	<-p.done
	return p.code, p.waitErr
}

// Kill terminates only this child, without closing its Group or control socket.
func (p *Child) Kill() error {
	<-p.started
	if p.cmd == nil || p.cmd.Process == nil {
		return nil
	}
	err := p.cmd.Process.Kill()
	if errors.Is(err, os.ErrProcessDone) {
		return nil
	}
	return err
}
