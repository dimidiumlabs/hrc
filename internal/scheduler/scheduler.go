// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

// Package scheduler manages capacity and slot lifetimes within one space.
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"strconv"
	"sync"

	"dimidiumlabs.io/hrc/internal/provider"
	"dimidiumlabs.io/hrc/internal/slot"
	"dimidiumlabs.io/hrc/internal/space"
)

// slotCount returns how many runners fit on the host, each holding quota after
// the host keeps reserve. Without a quota the count is unlimited.
func slotCount(host, reserve, quota space.Resources) (int, error) {
	total := math.MaxInt32
	if quota.CPU > 0 {
		// The epsilon keeps quotas such as 0.1 from losing a slot to rounding.
		n := math.Floor((host.CPU-reserve.CPU)/quota.CPU + 1e-9)
		total = min(total, int(max(n, 0)))
	}
	if quota.Memory > 0 {
		n := uint64(0)
		if host.Memory > reserve.Memory {
			n = (host.Memory - reserve.Memory) / quota.Memory
		}
		total = min(total, int(min(n, math.MaxInt32)))
	}
	if total == 0 {
		return 0, fmt.Errorf("quota (cpu %g, memory %d bytes) exceeds host resources (cpu %g, memory %d bytes) minus reserve (cpu %g, memory %d bytes)",
			quota.CPU, quota.Memory, host.CPU, host.Memory, reserve.CPU, reserve.Memory)
	}
	return total, nil
}

// Allocation contains scheduling metadata; its Slot owns the actual resources.
type Allocation struct {
	slot.Slot
	owner    string
	reusable bool
	busy     bool
}

// Scheduler accounts for reservations and manages reusable and ephemeral slots.
type Scheduler struct {
	mu       sync.Mutex
	space    space.Space
	total    int
	used     int
	waiting  int
	all      map[*Allocation]struct{}
	free     map[string][]*Allocation
	next     map[string]int
	released chan struct{}
	closed   bool
	closeErr error
}

// New creates a scheduler with a per-slot quota and resources reserved for the
// environment. A zero quota leaves capacity unlimited without probing the space.
func New(environment space.Space, reserve, quota space.Resources) (*Scheduler, error) {
	total := math.MaxInt32
	if quota != (space.Resources{}) {
		host, err := environment.Capacity()
		if err != nil {
			return nil, err
		}
		total, err = slotCount(host, reserve, quota)
		if err != nil {
			return nil, fmt.Errorf("invalid quota: %w", err)
		}
		slog.Info("Space capacity", "cpu", host.CPU, "memory", host.Memory, "runners", total)
	}
	return newScheduler(environment, total), nil
}

func newScheduler(environment space.Space, total int) *Scheduler {
	return &Scheduler{
		space: environment, total: total,
		all: make(map[*Allocation]struct{}), free: make(map[string][]*Allocation),
		next: make(map[string]int), released: make(chan struct{}),
	}
}

// Reserve accounts for acquisition demand without preparing an execution slot.
func (p *Scheduler) Reserve() provider.Reservation {
	p.mu.Lock()
	defer p.mu.Unlock()
	r := &reservation{pool: p}
	if p.closed {
		r.closed = true
		return r
	}
	r.pending = true
	p.waiting++
	p.notify()
	return r
}

type reservation struct {
	pool     *Scheduler
	pending  bool
	acquired bool
	closed   bool
}

func (r *reservation) Wait(ctx context.Context) error {
	p := r.pool
	p.mu.Lock()
	defer p.mu.Unlock()
	for {
		if p.closed || r.closed {
			return os.ErrClosed
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if r.acquired {
			return errors.New("reservation is already acquired")
		}
		if p.used < p.total {
			p.waiting--
			p.used++
			r.pending, r.acquired = false, true
			return nil
		}
		changed := p.released
		p.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		}
		p.mu.Lock()
	}
}

func (r *reservation) Close() error {
	p := r.pool
	p.mu.Lock()
	defer p.mu.Unlock()
	if !r.closed {
		r.closed = true
		if r.pending {
			p.waiting--
		}
		if r.acquired {
			p.used--
		}
		p.notify()
	}
	return nil
}

// Acquire materializes a slot for an already acquired Job. The queue holds a
// capacity reservation until this slot is released and the Job is closed.
func (p *Scheduler) Acquire(ctx context.Context, owner string, reusable bool) (*Allocation, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, os.ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var s *Allocation
	if idle := p.free[owner]; reusable && len(idle) > 0 {
		s = idle[len(idle)-1]
		p.free[owner] = idle[:len(idle)-1]
	} else {
		name := ""
		if reusable {
			name = owner + "-" + strconv.Itoa(p.next[owner]+1)
		}
		instance, err := p.space.CreateSlot(name)
		if err != nil {
			return nil, err
		}
		s = &Allocation{Slot: instance, owner: owner, reusable: reusable}
		p.all[s] = struct{}{}
		if reusable {
			p.next[owner]++
		}
	}
	s.busy = true
	return s, nil
}

// Release returns a reusable slot to its owner or closes an ephemeral slot.
// The caller releases its capacity reservation separately, after Job cleanup.
func (p *Scheduler) Release(s *Allocation) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	if _, ok := p.all[s]; !ok || !s.busy {
		return errors.New("slot is not acquired from this scheduler")
	}
	var err error
	if s.reusable {
		p.free[s.owner] = append(p.free[s.owner], s)
	} else {
		err = s.Close()
		delete(p.all, s)
	}
	s.busy = false
	return err
}

func (p *Scheduler) notify() {
	close(p.released)
	p.released = make(chan struct{})
}

// State reports capacity not held by running or pending acquisitions.
func (p *Scheduler) State() (int, <-chan struct{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return 0, p.released
	}
	return max(p.total-p.used-p.waiting, 0), p.released
}

// Close stops all slots and wakes pending reservations. It is safe to repeat.
func (p *Scheduler) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.closed {
		p.closed = true
		for s := range p.all {
			p.closeErr = errors.Join(p.closeErr, s.Close())
		}
		p.all, p.free = nil, nil
		p.notify()
	}
	return p.closeErr
}
