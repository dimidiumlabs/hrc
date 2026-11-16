// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"sync"

	hrcconfig "dimidiumlabs.io/hrc/internal/config"
	"dimidiumlabs.io/hrc/internal/provider"
	"dimidiumlabs.io/hrc/internal/runtime"
	"dimidiumlabs.io/hrc/internal/space"
)

// Demands binds space IDs to acquisition budgets without exposing spaces.
type Demands map[string]*demand

// Init calculates fixed budgets at startup, before acquisition or space creation.
// On error no budgets are installed. Demands must be an initialized, empty map.
func (d Demands) Init(cfg *Config) error {
	if d == nil || len(d) != 0 {
		return errors.New("demands must be an initialized, empty map")
	}
	limits, err := spaceCapacities(cfg)
	if err != nil {
		return err
	}
	for i, spec := range cfg.Spaces {
		d[string(spec.Base().ID)] = newDemand(limits[i])
	}
	return nil
}

// Capacity is the fixed limit, independent of current reservations.
func (d Demands) Capacity(id string) int {
	if budget := d[id]; budget != nil {
		return budget.total
	}
	return 0
}

func (d Demands) Close() {
	for _, budget := range d {
		budget.Close()
	}
}

// demand bounds acquired and pending Jobs across queues sharing one space.
// It does not create slots or start the space. Its limit is fixed at startup.
type demand struct {
	mu                   sync.Mutex
	total, used, waiting int
	released             chan struct{}
	closed               bool
}

func newDemand(limit int) *demand {
	return &demand{total: max(limit, 0), released: make(chan struct{})}
}

// Reserve accounts for acquisition demand without preparing an execution slot.
func (p *demand) Reserve() provider.Reservation {
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
	pool     *demand
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

func (p *demand) notify() {
	close(p.released)
	p.released = make(chan struct{})
}

// State reports capacity not held by running or pending acquisitions.
func (p *demand) State() (int, <-chan struct{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return 0, p.released
	}
	return max(p.total-p.used-p.waiting, 0), p.released
}

func (p *demand) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.closed {
		p.closed = true
		p.notify()
	}
	return nil
}

// Resource calculations happen once in hrcd, before lazy space startup.
func spaceLimits(specs []hrcconfig.Space, pools []hrcconfig.Slot, host hrcconfig.Resources) ([]int, error) {
	templates := make(map[hrcconfig.ID]hrcconfig.Slot, len(pools))
	for _, pool := range pools {
		templates[pool.Space] = pool
	}
	available := space.Resources{CPU: host.CPU, Memory: uint64(host.Memory)}

	var used space.Resources

	limits := make([]int, len(specs))
	for i, spec := range specs {
		budget := space.Resources{CPU: spec.Resources.CPU, Memory: uint64(spec.Resources.Memory)}
		if budget.CPU == 0 {
			budget.CPU = available.CPU
		}
		if budget.Memory == 0 {
			budget.Memory = available.Memory
		}
		if budget.CPU > available.CPU-used.CPU+1e-9 || budget.Memory > available.Memory-used.Memory {
			return nil, fmt.Errorf("space %q exceeds available host resources", spec.ID)
		}
		used.CPU += budget.CPU
		used.Memory += budget.Memory
		pool, ok := templates[spec.ID]
		if !ok {
			continue
		}
		limit, err := space.SlotCount(budget, space.Resources{CPU: pool.Quota.CPU, Memory: uint64(pool.Quota.Memory)})
		if err != nil {
			return nil, fmt.Errorf("space %q: %w", spec.ID, err)
		}
		limits[i] = limit
	}

	return limits, nil
}

func spaceCapacities(cfg *Config) ([]int, error) {
	specs := make([]hrcconfig.Space, len(cfg.Spaces))
	for i, config := range cfg.Spaces {
		specs[i] = *config.Base()
	}
	pools := make([]hrcconfig.Slot, len(cfg.Slots))
	for i, config := range cfg.Slots {
		pools[i] = *config.Base()
	}
	if len(specs) == 0 {
		return nil, nil
	}
	if len(specs) == 1 && specs[0].Resources == (hrcconfig.Resources{}) {
		if len(pools) == 0 {
			return []int{0}, nil
		}
		if pools[0].Quota == (hrcconfig.Resources{}) {
			return []int{math.MaxInt32}, nil
		}
	}
	cpu, err := runtime.CPUCapacity()
	if err != nil {
		return nil, err
	}
	memory, err := runtime.MemoryCapacity()
	if err != nil {
		return nil, err
	}
	return spaceLimits(specs, pools, hrcconfig.Resources{CPU: cpu, Memory: hrcconfig.ByteSize(memory)})
}
