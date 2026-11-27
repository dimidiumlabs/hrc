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
	"dimidiumlabs.io/hrc/internal/resource"
	"dimidiumlabs.io/hrc/internal/runtime"
)

// Controller implements the daemon's fanout and owns active jobs, routing
// acquisition, execution, events and completion between queues and monitors.
// A single resource heap owns jobs and the infrastructure they depend on.
type Controller struct {
	resources       resource.Heap
	ownership       sync.Once
	ownershipErr    error
	escalation      sync.Mutex
	collectionErr   error
	drain           chan struct{}
	drainOnce       sync.Once
	terminate       chan struct{}
	terminateOnce   sync.Once
	draining        bool
	stopAcquisition context.CancelFunc
	spaces          map[string]*managedSpace
	runTasks        sync.WaitGroup
	closeOnce       sync.Once
	closeErr        error
	closed          bool
	failure         error
	failed          chan struct{}
	failOnce        sync.Once
	forges          []managedForge
	done            chan struct{}
	err             error
	mu              sync.Mutex
	active          map[*acquiredJob]struct{}
	tasks           sync.WaitGroup
}

type managedForge struct {
	id, space string
	queue     provider.Queue
	demand    provider.Demand
	processes *runtime.Pool
	resource  *managedQueue
	ref       *resource.Ref
}

// acquiredJob stays tracked until execution and Handle cleanup have finished.
type acquiredJob struct {
	forge, space string
	handle       provider.Handle
	ctx          context.Context
	shutdown     context.CancelCauseFunc
	owner        *Controller
	once         sync.Once
	ref          *resource.Ref
	executed     chan struct{}
	finalization jobFinalization
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

func NewController(cfg *Config, demands Demands, providers map[string][]byte) (_ *Controller, err error) {
	c := &Controller{spaces: make(map[string]*managedSpace), failed: make(chan struct{})}
	defer func() {
		if err != nil {
			err = errors.Join(err, c.Close())
		}
	}()
	roots := make(map[string]string)
	for _, spec := range cfg.Spaces {
		slots := cfg.poolForSpace(spec.Base().ID)
		if slots == nil {
			continue
		}
		processes := &runtime.Pool{}
		environment, err := spec.NewSpace(cfg.Directories, *slots, demands.Capacity(string(spec.Base().ID)), providers, processes)
		if err != nil {
			return nil, err
		}
		id, root := string(spec.Base().ID), environment.Cwd()
		host := &managedSpace{id: id, environment: environment, demand: demands[id], processes: processes}
		host.ref, err = c.resources.Add(host)
		if err != nil {
			return nil, errors.Join(err, environment.Destroy())
		}
		if other, exists := roots[root]; exists {
			return nil, fmt.Errorf("spaces %q and %q share a directory", other, id)
		}
		roots[root] = id
		c.spaces[id] = host
	}
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
		processes := &runtime.Pool{}
		queue, err := variant.NewQueue(string(cfg.InstanceID), hrcconfig.Version, processes, c.recordFailure)
		if err != nil {
			return nil, fmt.Errorf("forge %q: %w", forge.ID, err)
		}
		managed := managedForge{id: string(forge.ID), space: string(spaceID), queue: queue, demand: demand, processes: processes}
		if err := c.ownForge(&managed); err != nil {
			return nil, err
		}
		c.forges = append(c.forges, managed)
	}
	return c, nil
}

// Serve is called once. Each provider handle is adopted into the resource heap before handoff.
// Cancellation stops acquisition, not delivered execution. Undelivered handles
// are finalized by the same resource heap rather than a separate cleanup path.
func (c *Controller) Serve(ctx context.Context) <-chan *acquiredJob {
	jobs := make(chan *acquiredJob)
	if err := c.initializeOwnership(); err != nil {
		c.recordFailure(err)
		close(jobs)
		return jobs
	}
	c.done = make(chan struct{})
	c.active = make(map[*acquiredJob]struct{})
	ctx, stopServing := context.WithCancel(ctx)
	acquisition, stopAcquisition := context.WithCancel(ctx)
	c.stopAcquisition = stopServing
	failures := make([]error, len(c.forges))
	var queues sync.Mutex
	open := len(c.forges)
	var wg sync.WaitGroup
	guards := make([]*resource.Ref, len(c.forges))
	for i, forge := range c.forges {
		dependencies := []*resource.Ref{forge.ref}
		if host := c.spaces[forge.space]; host != nil {
			dependencies = append(dependencies, host.ref)
		}
		var err error
		guards[i], err = c.resources.Add(&acquisitionResource{}, dependencies...)
		if err != nil {
			stopAcquisition()
			stopServing()
			c.recordFailure(err)
			close(c.done)
			close(jobs)
			return jobs
		}
	}
	for _, forge := range c.forges {
		forge.resource.started.Store(true)
	}
	for i, forge := range c.forges {
		wg.Go(func() {
			defer func() {
				close(forge.resource.done)
				c.resources.Request(guards[i], resource.Down)
				if err := c.resources.Collect(context.Background()); err != nil {
					c.collectionFailed(err)
				}
			}()
			acquisition, stop := context.WithCancel(acquisition)
			defer stop()
			unwatch := context.AfterFunc(forge.resource.ctx, stop)
			defer unwatch()
			delay := hrcconfig.QueueRetryMin
			for acquisition.Err() == nil {
				for handle := range forge.queue.Serve(acquisition, forge.demand) {
					execution, shutdown := context.WithCancelCause(context.WithoutCancel(ctx))
					job := &acquiredJob{forge: forge.id, space: forge.space, handle: handle, ctx: execution, shutdown: shutdown, owner: c}
					if err := c.ownJob(job, forge); err != nil {
						shutdown(err)
						c.recordFailure(err)
						failures[i] = errors.Join(failures[i], err, finishUnstarted(ctx, handle, provider.Abandoned))
						continue
					}
					c.mu.Lock()
					c.active[job] = struct{}{}
					c.tasks.Add(1)
					level := forge.resource.level
					if host := c.spaces[job.space]; host != nil {
						level = max(level, host.level)
					}
					if level >= resource.Term {
						job.Shutdown()
						c.resources.Request(job.ref, level)
					}
					c.mu.Unlock()
					select {
					case jobs <- job:
						delay = hrcconfig.QueueRetryMin
					case <-acquisition.Done():
						c.mu.Lock()
						drain := c.draining
						c.mu.Unlock()
						if drain {
							select {
							case jobs <- job:
								continue
							case <-c.terminate:
							}
						}
						job.finalization.result = provider.Canceled
						// Admission must not wait for finalization on the same resource heap
						// which may currently be quiescing this acquisition.
						c.runTasks.Go(job.executionFinished)
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
		defer stopServing()
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

// Run owns dispatch and daemon shutdown. Normal collection releases accepted
// handles before retiring their infrastructure; forced cleanup reports leftovers.
func (c *Controller) Run(ctx context.Context) error {
	acquisition, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := c.Serve(acquisition)
	drain, failed, stopping := c.drain, c.Failed(), ctx.Done()
	level := resource.Down
	for jobs != nil {
		select {
		case job, ok := <-jobs:
			if !ok {
				jobs = nil
				continue
			}
			if level >= resource.Term || ctx.Err() != nil {
				job.Shutdown()
			}
			c.Submit(job)
		case <-drain:
			drain = nil
			c.mu.Lock()
			c.draining = true
			c.mu.Unlock()
			if err := c.quiesceAdmission(ctx); err != nil && ctx.Err() == nil {
				c.collectionFailed(err)
			}
			cancel()
		case <-failed:
			failed, drain = nil, nil
			level = resource.Term
			c.terminateJobs()
			cancel()
		case <-stopping:
			stopping, drain = nil, nil
			level = resource.Term
			c.terminateJobs()
			cancel()
		}
	}
	cancel()
	return c.close(ctx, level)
}

func (c *Controller) terminateJobs() {
	c.terminateOnce.Do(func() { close(c.terminate) })
	c.mu.Lock()
	c.draining = false
	c.mu.Unlock()
	for _, job := range c.Jobs() {
		job.Shutdown()
	}
}

// Close drains accepted work. Run additionally observes termination while draining.
func (c *Controller) Close() error { return c.close(context.Background(), resource.Down) }

func (c *Controller) close(ctx context.Context, level resource.Level) error {
	c.closeOnce.Do(func() {
		c.closeErr = c.initializeOwnership()
		if c.stopAcquisition != nil {
			c.stopAcquisition()
		}
		c.closeErr = errors.Join(c.closeErr, c.quiesceAdmission(context.Background()))
		if level >= resource.Term || ctx.Err() != nil {
			level = resource.Term
			c.terminateJobs()
		}
		if c.done != nil {
			<-c.done
		}
		c.closeErr = errors.Join(c.closeErr, c.resources.Quiesce(context.Background()))
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		ran := make(chan struct{})
		go func() { c.runTasks.Wait(); close(ran) }()
		select {
		case <-ran:
		case <-ctx.Done():
			level = resource.Term
			c.terminateJobs()
			<-ran
		case <-c.Failed():
			level = resource.Term
			c.terminateJobs()
			<-ran
		}
		for ; level <= resource.Kill; level++ {
			if level == resource.Kill {
				// Failed consumers still retain dependencies in the graph.
				// Force their reachable processes without pretending that this
				// discharged the failed consumer's release obligations.
				for _, forge := range c.forges {
					c.closeErr = errors.Join(c.closeErr, collectProcesses(context.Background(), forge.processes, level))
				}
				for _, host := range c.spaces {
					c.closeErr = errors.Join(c.closeErr, collectProcesses(context.Background(), host.processes, level))
				}
			}
			err := c.resources.Shutdown(context.Background(), level)
			if err == nil {
				break
			}
			c.closeErr = errors.Join(c.closeErr, err)
			if level < resource.Kill {
				c.terminateJobs()
				c.expandCollection(err, level+1)
			}
		}
		if remaining := c.resources.Len(); remaining != 0 {
			c.closeErr = errors.Join(c.closeErr, fmt.Errorf("%d resources remain uncollected", remaining))
		}
	})
	c.mu.Lock()
	defer c.mu.Unlock()
	return errors.Join(c.err, c.failure, c.collectionErr, c.closeErr)
}

func finishUnstarted(ctx context.Context, handle provider.Handle, result provider.Result) error {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), hrcconfig.JobCleanupTimeout)
	defer cancel()
	err := handle.Finish(cleanup, result, nil)
	closeCtx, closeCancel := context.WithTimeout(context.WithoutCancel(ctx), hrcconfig.JobCleanupTimeout)
	defer closeCancel()
	return errors.Join(err, handle.Close(closeCtx))
}

// initializeOwnership also supports controllers assembled directly by tests.
// Production constructors register each resource immediately after creation.
func (c *Controller) initializeOwnership() error {
	c.ownership.Do(func() {
		if c.failed == nil {
			c.failed = make(chan struct{})
		}
		c.drain = make(chan struct{})
		c.terminate = make(chan struct{})
		for name, host := range c.spaces {
			host.id = name
			if host.ref == nil {
				host.ref, c.ownershipErr = c.resources.Add(host)
				if c.ownershipErr != nil {
					c.ownershipErr = fmt.Errorf("own space %q: %w", name, c.ownershipErr)
					return
				}
			}
		}
		for i := range c.forges {
			if err := c.ownForge(&c.forges[i]); err != nil {
				c.ownershipErr = err
				return
			}
		}
	})
	return c.ownershipErr
}

func (c *Controller) ownForge(forge *managedForge) error {
	if forge.resource != nil {
		return nil
	}
	ctx, stop := context.WithCancel(context.Background())
	queue := &managedQueue{id: forge.id, ctx: ctx, stop: stop, processes: forge.processes, done: make(chan struct{})}
	ref, err := c.resources.Add(queue)
	if err != nil {
		stop()
		return err
	}
	forge.resource, forge.ref = queue, ref
	return nil
}

func (c *Controller) ownJob(job *acquiredJob, forge managedForge) error {
	dependencies := []*resource.Ref{forge.ref}
	if host := c.spaces[job.space]; host != nil {
		dependencies = append(dependencies, host.ref)
	}
	job.executed = make(chan struct{})
	job.finalization.handle = job.handle
	var err error
	job.ref, err = c.resources.Add(&jobResource{job}, dependencies...)
	return err
}

// Drain requests normal completion, not cancellation. It may precede Run.
func (c *Controller) Drain() {
	if err := c.initializeOwnership(); err != nil {
		c.recordFailure(err)
		return
	}
	c.drainOnce.Do(func() { close(c.drain) })
}

// executionFinished publishes the outcome before entering the collector, so a
// simultaneous full shutdown can wait for execution without waiting on itself.
func (j *acquiredJob) executionFinished() {
	close(j.executed)
	if err := j.owner.resources.Request(j.ref, resource.Down); err != nil {
		j.owner.recordFailure(err)
		return
	}
	if err := j.owner.resources.Collect(context.Background()); err != nil {
		j.owner.collectionFailed(err)
	}
}

// collectionFailed belongs to controller policy, not the resource heap. A failed finalizer
// identifies a queue or space; all jobs depending on that resource lose Drain.
// An unclassified failure, or a scope still uncollectable at KILL, is fatal.
func (c *Controller) collectionFailed(err error) {
	c.escalation.Lock()
	defer c.escalation.Unlock()
	c.mu.Lock()
	c.collectionErr = errors.Join(c.collectionErr, err)
	c.mu.Unlock()
	for level := resource.Term; level <= resource.Kill && err != nil; level++ {
		c.expandCollection(err, level)
		err = c.resources.Collect(context.Background())
	}
	if err != nil {
		c.recordFailure(err)
	}
}

// quiesceAdmission does not wait for a resource collection pass: a pass may itself be waiting for
// a job whose accepted handoff still needs the Run dispatch loop to progress.
func (c *Controller) quiesceAdmission(ctx context.Context) error {
	for _, forge := range c.forges {
		forge.resource.stop()
	}
	return ctx.Err()
}

func (c *Controller) expandCollection(err error, level resource.Level) {
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, item := range joined.Unwrap() {
			c.expandCollection(item, level)
		}
		return
	}
	var failure *resource.Error
	if !errors.As(err, &failure) {
		c.recordFailure(err)
		for _, job := range c.Jobs() {
			job.Shutdown()
		}
		return
	}
	var forgeID, spaceID string
	switch resource := failure.Resource.(type) {
	case *jobResource:
		var operation *finalizationError
		if !errors.As(failure.Err, &operation) {
			c.recordFailure(err)
			return
		}
		if operation.queue {
			forgeID = resource.job.forge
		} else {
			spaceID = resource.job.space
		}
	case *managedQueue:
		forgeID = resource.id
	case *managedSpace:
		for id, host := range c.spaces {
			if host == resource {
				spaceID = id
				break
			}
		}
	default:
		c.recordFailure(err)
		return
	}
	c.mu.Lock()
	for _, forge := range c.forges {
		if forge.id == forgeID {
			forge.resource.level = max(forge.resource.level, level)
		}
	}
	if host := c.spaces[spaceID]; host != nil {
		host.level = max(host.level, level)
	}
	for job := range c.active {
		if (forgeID != "" && job.forge == forgeID) || (spaceID != "" && job.space == spaceID) {
			job.Shutdown()
			c.resources.Request(job.ref, level)
		}
	}
	c.mu.Unlock()
	for _, forge := range c.forges {
		if forge.id == forgeID || (spaceID != "" && forge.space == spaceID) {
			forge.resource.stop()
		}
		if forge.id == forgeID {
			c.resources.Request(forge.ref, level)
			if level == resource.Kill {
				if err := collectProcesses(context.Background(), forge.processes, level); err != nil {
					c.recordFailure(err)
				}
			}
		}
	}
	if host := c.spaces[spaceID]; host != nil {
		c.resources.Request(host.ref, level)
		if level == resource.Kill {
			if err := collectProcesses(context.Background(), host.processes, level); err != nil {
				c.recordFailure(err)
			}
		}
	}
}
