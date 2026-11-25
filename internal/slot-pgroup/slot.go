// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package slotpgroup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"dimidiumlabs.io/hrc/internal/provider"
	"dimidiumlabs.io/hrc/internal/runtime"
	"dimidiumlabs.io/hrc/internal/slot"
)

// Environment supplies a prepared workspace and its storage lifecycle.
// The monitor owns the layout; process slots only invoke these lifecycle hooks.
type Environment interface {
	Name() string
	Cwd() string
	Environment() []string
	Prepare() error
	Destroy() error
}

// Slot owns its workspace, background processes and successive Runs.
// Worker selection and process launch are explicit implementation details.
type Slot struct {
	environment   Environment
	group         *runtime.Group
	launch        func(context.Context, *Slot, provider.Job) (provider.Run, error)
	mu            sync.Mutex
	active        *run
	status        slot.Status
	preparing     chan struct{}
	prepareCancel context.CancelFunc
	destroyOnce   sync.Once
	destroyErr    error
}

// New creates a process slot with trusted provider commands.
func New(environment Environment, providers map[string]provider.ProcessConfig, pool *runtime.Pool) (*Slot, error) {
	return newSlot(environment, func(ctx context.Context, target *Slot, job provider.Job) (provider.Run, error) {
		config, ok := providers[job.Type()]
		if !ok {
			return nil, fmt.Errorf("%w: %s", slot.ErrProviderUnavailable, job.Type())
		}
		payload, err := job.Marshal()
		if err != nil {
			return nil, err
		}
		return provider.StartWorker(ctx, config, payload, runtime.Command{
			Group: target.group, Dir: target.Cwd(), Env: target.environment.Environment(),
			Stdout: os.Stdout, Stderr: os.Stderr,
		}, pool)
	})
}

func newSlot(environment Environment, launch func(context.Context, *Slot, provider.Job) (provider.Run, error)) (*Slot, error) {
	group, err := runtime.NewGroup()
	if err != nil {
		return nil, errors.Join(slot.ErrPrepare, err)
	}
	return &Slot{environment: environment, group: group, launch: launch, status: slot.Idle}, nil
}
func (s *Slot) Name() string        { return s.environment.Name() }
func (s *Slot) Cwd() string         { return s.environment.Cwd() }
func (*Slot) Type() string          { return "pgroup" }
func (s *Slot) Status() slot.Status { s.mu.Lock(); defer s.mu.Unlock(); return s.status }

func (s *Slot) Run(ctx context.Context, job provider.Job) (provider.Run, error) {
	s.mu.Lock()
	if s.status != slot.Idle {
		state := s.status
		s.mu.Unlock()
		if state >= slot.Destroying {
			return nil, slot.ErrDestroyed
		}
		return nil, slot.ErrBusy
	}
	if s.launch == nil {
		s.mu.Unlock()
		return nil, slot.ErrProviderUnavailable
	}
	s.status = slot.Starting
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.preparing = make(chan struct{})
	s.prepareCancel = cancel
	s.mu.Unlock()
	var local provider.Run
	err := ctx.Err()
	if err == nil {
		err = s.environment.Prepare()
		if err != nil {
			err = errors.Join(slot.ErrPrepare, err)
		}
	}
	if err == nil {
		if err = ctx.Err(); err == nil {
			local, err = s.launch(ctx, s, job)
		}
	}
	s.mu.Lock()
	if err == nil {
		err = ctx.Err()
	}
	if s.status >= slot.Destroying {
		err = errors.Join(err, slot.ErrDestroyed)
	}
	if err != nil || local == nil {
		s.mu.Unlock()
		if local != nil {
			err = errors.Join(err, local.Stop(context.Background(), provider.StopForce))
		}
		s.mu.Lock()
		if s.status < slot.Destroying {
			s.status = slot.Idle
		}
		close(s.preparing)
		s.preparing = nil
		s.prepareCancel = nil
		s.mu.Unlock()
		if err == nil {
			err = slot.ErrInvalidRun
		}
		return nil, err
	}
	r := &run{owner: s, local: local, done: make(chan error, 1), finished: make(chan struct{})}
	s.active, s.status = r, slot.Running
	close(s.preparing)
	s.preparing = nil
	s.prepareCancel = nil
	s.mu.Unlock()
	go func() {
		r.err = <-local.Done()
		r.result, r.data = local.Result()
		s.mu.Lock()
		if s.active == r {
			s.active = nil
			if s.status < slot.Destroying {
				s.status = slot.Idle
			}
		}
		close(r.finished)
		r.done <- r.err
		close(r.done)
		s.mu.Unlock()
	}()
	return r, nil
}

func (s *Slot) Stop(ctx context.Context, target provider.Run) error {
	r, ok := target.(*run)
	if !ok || r.owner != s {
		return slot.ErrUnknownRun
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	if s.active == r && s.status < slot.Destroying {
		s.status = slot.Stopping
	}
	s.mu.Unlock()
	// Closing this handle always targets its original provider Run.
	stopErr := r.local.Stop(ctx, provider.StopForce)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.finished:
		// Publication of Done and the pool state completes under the slot lock.
		s.mu.Lock()
		s.mu.Unlock()
		return stopErr
	}
}

func (s *Slot) Destroy(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	if s.status != slot.Destroyed {
		s.status = slot.Destroying
	}
	active := s.active
	preparing := s.preparing
	if s.prepareCancel != nil {
		s.prepareCancel()
	}
	s.mu.Unlock()
	if preparing != nil {
		select {
		case <-preparing:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if active != nil {
		if err := s.Stop(ctx, active); err != nil {
			return err
		}
	}
	s.destroyOnce.Do(func() {
		s.destroyErr = errors.Join(s.group.Close(), s.environment.Destroy())
		if s.destroyErr != nil {
			s.destroyErr = errors.Join(slot.ErrUnknown, s.destroyErr)
		} else {
			s.mu.Lock()
			s.status = slot.Destroyed
			s.mu.Unlock()
		}
	})
	return s.destroyErr
}

type run struct {
	owner    *Slot
	local    provider.Run
	done     chan error
	finished chan struct{}
	result   provider.Result
	data     []byte
	err      error
}

func (r *run) Done() <-chan error { return r.done }
func (r *run) Result() (provider.Result, []byte) {
	select {
	case <-r.finished:
		return r.result, r.data
	default:
		return provider.Unknown, nil
	}
}
func (r *run) command(ctx context.Context, send func(context.Context) error) error {
	err := send(ctx)
	if err != nil {
		result, _ := r.local.Result()
		if result.Valid() {
			select {
			case <-r.finished:
				r.owner.mu.Lock()
				r.owner.mu.Unlock()
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return err
}
func (r *run) Stop(ctx context.Context, reason provider.StopReason) error {
	if reason == provider.StopForce {
		return r.owner.Stop(ctx, r)
	}
	return r.command(ctx, func(ctx context.Context) error { return r.local.Stop(ctx, reason) })
}
