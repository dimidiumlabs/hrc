// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"

	"dimidiumlabs.io/hrc/internal/config"
	"dimidiumlabs.io/hrc/internal/provider"
	"dimidiumlabs.io/hrc/internal/slot"
	"dimidiumlabs.io/hrc/internal/space"
)

// allocation grants exclusive use of a slot until release.
type allocation struct {
	slot.Slot
	busy bool
}

type scheduler struct {
	mu                sync.Mutex
	limit, used, next int
	waiting           int
	reuse             bool
	decode            func(string, []byte) (provider.Job, error)
	create            func(context.Context, string) (slot.Slot, error)
	all               map[*allocation]struct{}
	free              []*allocation
	changed           chan struct{}
	closed            bool
	closeErr          error
}

// All slots use the same factory and reuse policy. Creation counts against
// capacity, and occurs only after a Run is submitted.
func newScheduler(limit int, reuse bool, create func(context.Context, string) (slot.Slot, error)) (*scheduler, error) {
	if limit <= 0 || create == nil {
		return nil, errors.New("positive slot limit and factory are required")
	}
	return &scheduler{limit: limit, reuse: reuse, create: create, all: make(map[*allocation]struct{}), changed: make(chan struct{})}, nil
}

func (s *scheduler) acquire(ctx context.Context) (*allocation, error) {
	s.mu.Lock()
	s.waiting++
	for s.used == s.limit && !s.closed && ctx.Err() == nil {
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		}
		s.mu.Lock()
	}
	s.waiting--
	if s.closed {
		s.mu.Unlock()
		return nil, os.ErrClosed
	}
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	s.used++
	if n := len(s.free); n > 0 {
		a := s.free[n-1]
		s.free = s.free[:n-1]
		a.busy = true
		s.mu.Unlock()
		return a, nil
	}
	s.next++
	name := fmt.Sprintf("slot-%d", s.next)
	s.mu.Unlock()
	instance, err := s.create(ctx, name)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err == nil {
		if s.closed {
			err = os.ErrClosed
		} else {
			err = ctx.Err()
		}
	}
	if err != nil {
		if instance != nil {
			err = errors.Join(err, instance.Destroy(context.Background()))
		}
		s.used--
		s.notify()
		return nil, err
	}
	a := &allocation{Slot: instance, busy: true}
	s.all[a] = struct{}{}
	return a, nil
}

// release is called after the Run has stopped and been reaped. An unusable
// slot is discarded even if reuse is enabled.
func (s *scheduler) release(a *allocation, usable bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	if _, ok := s.all[a]; !ok || !a.busy {
		return errors.New("slot is not acquired from this scheduler")
	}
	var err error
	if s.reuse && usable {
		s.free = append(s.free, a)
	} else {
		err = a.Destroy(context.Background())
		delete(s.all, a)
	}
	a.busy = false
	s.used--
	s.notify()
	return err
}

func (s *scheduler) notify() { close(s.changed); s.changed = make(chan struct{}) }

func (s *scheduler) state() space.State {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := space.State{Capacity: uint64(s.limit), Slots: uint64(len(s.all)), BusySlots: uint64(s.used), Queued: uint64(s.waiting)}
	for a := range s.all {
		status := a.Status()
		if status == slot.Running || status == slot.Stopping {
			state.Running++
		}
	}
	return state
}

// close expires idle and active slots and wakes waiting acquisitions.
// All Runs must be stopped before closing their slots.
func (s *scheduler) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		for a := range s.all {
			s.closeErr = errors.Join(s.closeErr, a.Destroy(context.Background()))
		}
		s.all, s.free = nil, nil
		s.notify()
	}
	return s.closeErr
}

func serve(ctx context.Context, conn net.Conn, configure func(*space.Config) (*scheduler, error)) error {
	var s *scheduler
	server := &Scheduler{
		Configure: func(config *space.Config) (err error) { s, err = configure(config); return err },
		State:     func() space.State { return s.state() },
		Execute: func(ctx context.Context, name string, payload []byte, stops <-chan provider.StopReason) (provider.Result, []byte, error) {
			job, err := s.decode(name, payload)
			if err != nil {
				return provider.Failed, nil, err
			}
			lifetime, cancel := context.WithCancel(ctx)
			defer cancel()
			runCtx, stopRun := context.WithCancel(lifetime)
			defer stopRun()
			e := &execution{ctx: runCtx, lifetime: lifetime, cancel: stopRun, changed: make(chan struct{}, 1)}
			stopped := make(chan struct{})
			go func() {
				defer close(stopped)
				for {
					select {
					case reason := <-stops:
						e.request(reason)
					case <-lifetime.Done():
						return
					}
				}
			}()
			defer func() { cancel(); <-stopped }()
			return s.execute(e, job)
		},
	}
	err := server.Serve(ctx, conn)
	// Serve waits for RPC handlers, so every Run has stopped before slots expire.
	if s != nil {
		err = errors.Join(err, s.close())
	}
	return err
}

type execution struct {
	ctx       context.Context
	lifetime  context.Context
	cancel    context.CancelFunc
	mu        sync.Mutex
	requested provider.StopReason
	changed   chan struct{}
	running   bool
}

func (e *execution) request(kind provider.StopReason) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if kind > e.requested {
		e.requested = kind
	}
	// Interrupt waiting and preparation. An active Run receives the corresponding
	// provider command, allowing its own cancellation/cleanup semantics.
	if !e.running {
		e.cancel()
	}
	select {
	case e.changed <- struct{}{}:
	default:
	}
}

func (s *scheduler) execute(e *execution, job provider.Job) (result provider.Result, data []byte, err error) {
	result = provider.Failed
	allocation, err := s.acquire(e.ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return provider.Canceled, nil, nil
		}
		return result, data, err
	}
	defer func() { err = errors.Join(err, s.release(allocation, err == nil)) }()
	local, err := allocation.Run(e.ctx, job)
	if local != nil {
		defer func() { err = errors.Join(err, allocation.Stop(context.Background(), local)) }()
	}
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return provider.Canceled, nil, nil
		}
		return result, data, err
	}
	if local == nil {
		return result, data, errors.New("provider returned no Run")
	}
	e.mu.Lock()
	e.running = true
	e.mu.Unlock()
	for {
		select {
		case err = <-local.Done():
			result, data = local.Result()
			if !result.Valid() {
				result = provider.Failed
			}
			return result, data, err
		case <-e.lifetime.Done():
			err = allocation.Stop(context.Background(), local)
			waitErr := <-local.Done()
			result, data = local.Result()
			if !result.Valid() {
				result = provider.Canceled
			}
			return result, data, errors.Join(err, waitErr)
		case <-e.changed:
			e.mu.Lock()
			kind := e.requested
			e.mu.Unlock()
			send, stop := context.WithTimeout(context.Background(), config.ControlTimeout)
			switch kind {
			case provider.StopCancel:
				err = local.Stop(send, provider.StopCancel)
			case provider.StopShutdown:
				err = local.Stop(send, provider.StopShutdown)
			case provider.StopForce:
				err = allocation.Stop(context.Background(), local)
			}
			stop()
			if err != nil {
				// Cancellation can race with normal completion. Preserve the provider's
				// completion data so Job does not report the same completion again.
				select {
				case err = <-local.Done():
					result, data = local.Result()
					if !result.Valid() {
						result = provider.Failed
					}
					return result, data, err
				default:
				}
				cause := err
				closeErr := allocation.Stop(context.Background(), local)
				err = <-local.Done()
				result, data = local.Result()
				if !result.Valid() {
					result = provider.Failed
				}
				return result, data, errors.Join(cause, closeErr, err)
			}
		}
	}
}
