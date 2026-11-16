// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	hrcconfig "dimidiumlabs.io/hrc/internal/config"
	"dimidiumlabs.io/hrc/internal/provider"
)

// Controller owns acquisition from forges; it knows neither spaces nor execution.
type Controller struct {
	forges []managedForge
	done   chan struct{}
	err    error
	mu     sync.Mutex
	active map[*acquiredJob]struct{}
	tasks  sync.WaitGroup
}

type managedForge struct {
	id, space string
	queue     provider.Queue
	demand    provider.Demand
}

// acquiredJob stays tracked until execution and Handle cleanup have finished.
type acquiredJob struct {
	forge, space string
	handle       provider.Handle
	ctx          context.Context
	shutdown     context.CancelCauseFunc
	owner        *Controller
	once         sync.Once
}

var errJobShutdown = errors.New("daemon shutdown")

func (j *acquiredJob) Shutdown() { j.shutdown(errJobShutdown) }

func (j *acquiredJob) complete() {
	j.once.Do(func() {
		j.shutdown(nil)
		j.owner.mu.Lock()
		delete(j.owner.active, j)
		j.owner.mu.Unlock()
		j.owner.tasks.Done()
	})
}

// Jobs returns a snapshot of all acquired tasks that have not finished cleanup.
func (c *Controller) Jobs() []*acquiredJob {
	c.mu.Lock()
	defer c.mu.Unlock()
	jobs := make([]*acquiredJob, 0, len(c.active))
	for job := range c.active {
		jobs = append(jobs, job)
	}
	return jobs
}

func NewController(cfg *Config, demands Demands) (*Controller, error) {
	c := &Controller{}
	placements := make(map[hrcconfig.ID]hrcconfig.ID, len(cfg.Slots))
	for _, pool := range cfg.Slots {
		placements[pool.Base().ID] = pool.Base().Space
	}
	for _, variant := range cfg.Forges {
		forge := variant.Base()
		spaceID, ok := placements[forge.Slot]
		if !ok {
			return nil, fmt.Errorf("forge %q: unknown slot pool %q", forge.ID, forge.Slot)
		}
		demand := demands[string(spaceID)]
		if demand == nil {
			return nil, fmt.Errorf("forge %q: no demand for space %q", forge.ID, spaceID)
		}
		queue, err := variant.NewQueue(string(cfg.InstanceID), hrcconfig.Version)
		if err != nil {
			return nil, fmt.Errorf("forge %q: %w", forge.ID, err)
		}
		c.forges = append(c.forges, managedForge{id: string(forge.ID), space: string(spaceID), queue: queue, demand: demand})
	}
	return c, nil
}

// Serve is called once. Ownership transfers when a job is received from jobs.
// Cancellation stops acquisition and cleans up any handles not transferred.
func (c *Controller) Serve(ctx context.Context) <-chan *acquiredJob {
	jobs := make(chan *acquiredJob)
	c.done = make(chan struct{})
	c.active = make(map[*acquiredJob]struct{})
	failures := make([]error, len(c.forges))
	var wg sync.WaitGroup
	for i, forge := range c.forges {
		wg.Go(func() {
			for handle := range forge.queue.Serve(ctx, forge.demand) {
				execution, shutdown := context.WithCancelCause(context.WithoutCancel(ctx))
				job := &acquiredJob{forge: forge.id, space: forge.space, handle: handle, ctx: execution, shutdown: shutdown, owner: c}
				c.mu.Lock()
				c.active[job] = struct{}{}
				c.tasks.Add(1)
				c.mu.Unlock()
				select {
				case jobs <- job:
				case <-ctx.Done():
					if err := finishUnstarted(ctx, handle, provider.Canceled); err != nil {
						failures[i] = errors.Join(failures[i], fmt.Errorf("forge %q: %w", forge.id, err))
					}
					job.complete()
				}
			}
		})
	}
	go func() {
		wg.Wait()
		c.err = errors.Join(failures...)
		close(jobs)
		close(c.done)
	}()
	return jobs
}

func (c *Controller) Close() error { <-c.done; c.tasks.Wait(); return c.err }

func finishUnstarted(ctx context.Context, handle provider.Handle, result provider.Result) error {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
	defer cancel()
	err := handle.Finish(cleanup, result, nil)
	closeCtx, closeCancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
	defer closeCancel()
	return errors.Join(err, handle.Close(closeCtx))
}
