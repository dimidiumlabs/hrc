// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
	acquisition, stopAcquisition := context.WithCancel(ctx)
	failures := make([]error, len(c.forges))
	var queues sync.Mutex
	open := len(c.forges)
	var wg sync.WaitGroup
	for i, forge := range c.forges {
		wg.Go(func() {
			delay := hrcconfig.QueueRetryMin
			for acquisition.Err() == nil {
				for handle := range forge.queue.Serve(acquisition, forge.demand) {
					execution, shutdown := context.WithCancelCause(context.WithoutCancel(ctx))
					job := &acquiredJob{forge: forge.id, space: forge.space, handle: handle, ctx: execution, shutdown: shutdown, owner: c}
					c.mu.Lock()
					c.active[job] = struct{}{}
					c.tasks.Add(1)
					c.mu.Unlock()
					select {
					case jobs <- job:
						delay = hrcconfig.QueueRetryMin
					case <-acquisition.Done():
						if err := finishUnstarted(ctx, handle, provider.Canceled); err != nil {
							failures[i] = errors.Join(failures[i], fmt.Errorf("forge %q: %w", forge.id, err))
						}
						job.complete()
					}
				}
				queues.Lock()
				open--
				if open == 0 {
					stopAcquisition()
				}
				queues.Unlock()
				if acquisition.Err() != nil {
					return
				}
				slog.Warn("Queue closed, retrying", "forge", forge.id, "delay", delay)
				timer := time.NewTimer(delay)
				select {
				case <-acquisition.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
				queues.Lock()
				if acquisition.Err() != nil {
					queues.Unlock()
					return
				}
				open++
				queues.Unlock()
				delay = min(2*delay, hrcconfig.QueueRetryMax)
			}
		})
	}
	go func() {
		defer close(c.done)
		defer close(jobs)
		defer stopAcquisition()
		wg.Wait()
		c.err = errors.Join(failures...)
		if ctx.Err() != nil || len(c.forges) == 0 {
			return
		}
		// All queue streams have ended. No more tasks can be added, but
		// delivered jobs must finish execution and Handle cleanup before
		// the daemon exits. External shutdown may still interrupt draining.
		drained := make(chan struct{})
		go func() { c.tasks.Wait(); close(drained) }()
		select {
		case <-ctx.Done():
			return
		case <-drained:
		}
		if ctx.Err() == nil {
			c.err = errors.Join(c.err, errors.New("all queues closed"))
		}
	}()
	return jobs
}

func (c *Controller) Close() error { <-c.done; c.tasks.Wait(); return c.err }

func finishUnstarted(ctx context.Context, handle provider.Handle, result provider.Result) error {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), hrcconfig.JobCleanupTimeout)
	defer cancel()
	err := handle.Finish(cleanup, result, nil)
	closeCtx, closeCancel := context.WithTimeout(context.WithoutCancel(ctx), hrcconfig.JobCleanupTimeout)
	defer closeCancel()
	return errors.Join(err, handle.Close(closeCtx))
}
