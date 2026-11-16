// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"dimidiumlabs.io/hrc/internal/provider"
	ipc "dimidiumlabs.io/hrc/internal/scheduler-ipc"
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

func serve(ctx context.Context, conn net.Conn, configure func(ipc.Config) (*scheduler, error)) error {
	wire := ipc.NewConn(conn)
	defer wire.Close()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	initial, err := wire.Receive()
	if err != nil {
		return err
	}
	if initial.Type != ipc.Init || initial.Version != ipc.Version {
		return errors.New("incompatible scheduler protocol")
	}
	var config ipc.Config
	if err := json.Unmarshal(initial.Payload, &config); err != nil {
		return errors.New("invalid scheduler configuration")
	}
	s, err := configure(config)
	if err != nil {
		_ = wire.Send(ctx, ipc.Message{Type: ipc.Ready, Version: ipc.Version, Error: err.Error()})
		return err
	}
	defer s.close()
	if err := wire.Send(ctx, ipc.Message{Type: ipc.Ready, Version: ipc.Version, Capacity: config.Capacity}); err != nil {
		return err
	}
	var mu sync.Mutex
	active := make(map[uint64]*execution)
	var tasks sync.WaitGroup
	defer func() { cancel(); tasks.Wait() }()
	var lastID uint64
	for {
		m, err := wire.Receive()
		if err != nil {
			if errors.Is(err, io.EOF) || ctx.Err() != nil {
				return nil
			}
			return err
		}
		switch m.Type {
		case ipc.Inspect:
			if err := wire.Send(ctx, ipc.Message{Type: ipc.Snapshot, ID: m.ID, State: s.state()}); err != nil {
				return err
			}
		case ipc.Submit:
			if m.ID <= lastID {
				return errors.New("invalid or repeated Run identifier")
			}
			lastID = m.ID
			job, err := s.decode(m.Provider, m.Payload)
			if err != nil {
				result := ipc.Message{Type: ipc.Finished, ID: m.ID, Result: provider.Failed}
				result.SetError(err)
				if err := wire.Send(ctx, result); err != nil {
					return err
				}
				continue
			}
			runCtx, stopRun := context.WithCancel(ctx)
			e := &execution{ctx: runCtx, lifetime: ctx, cancel: stopRun, changed: make(chan struct{}, 1)}
			mu.Lock()
			active[m.ID] = e
			mu.Unlock()
			tasks.Go(func() {
				defer stopRun()
				outcome, data, err := s.execute(e, job)
				mu.Lock()
				delete(active, m.ID)
				mu.Unlock()
				result := ipc.Message{Type: ipc.Finished, ID: m.ID, Result: outcome, Data: data}
				result.SetError(err)
				if err := wire.Send(ctx, result); err != nil {
					cancel()
				}
			})
		case ipc.Cancel, ipc.ShutdownRun, ipc.Kill:
			mu.Lock()
			e := active[m.ID]
			mu.Unlock()
			if e != nil {
				e.request(m.Type)
			}
		case ipc.Stop:
			return nil
		default:
			return fmt.Errorf("unexpected scheduler command %d", m.Type)
		}
	}
}

type execution struct {
	ctx       context.Context
	lifetime  context.Context
	cancel    context.CancelFunc
	mu        sync.Mutex
	requested ipc.Type
	changed   chan struct{}
	running   bool
}

func (e *execution) request(kind ipc.Type) {
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
			send, stop := context.WithTimeout(context.Background(), 30*time.Second)
			switch kind {
			case ipc.Cancel:
				err = local.Stop(send, provider.StopCancel)
			case ipc.ShutdownRun:
				err = local.Stop(send, provider.StopShutdown)
			case ipc.Kill:
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
