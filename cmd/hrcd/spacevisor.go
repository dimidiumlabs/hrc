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

	"dimidiumlabs.io/hrc/internal/provider"
	"dimidiumlabs.io/hrc/internal/space"
)

type managedSpace struct {
	environment space.Space
	demand      provider.Demand
}

func (s *managedSpace) Close() error { return s.environment.Destroy() }

// SpaceVisor owns the space pool and its resource reservations.
type SpaceVisor struct {
	spaces    map[string]*managedSpace
	closeOnce sync.Once
	closeErr  error
	tasks     sync.WaitGroup
	mu        sync.Mutex
	closed    bool
	failure   error
	failed    chan struct{}
	failOnce  sync.Once
}

func (v *SpaceVisor) Failed() <-chan struct{} { return v.failed }

// Submit takes ownership of the handle, including on dispatch failure.
func (v *SpaceVisor) Submit(ctx context.Context, job *acquiredJob) {
	ctx = job.ctx
	v.mu.Lock()
	if v.closed {
		v.mu.Unlock()
		err := finishUnstarted(ctx, job.handle, provider.Abandoned)
		v.recordFailure(errors.Join(space.ErrClosed, err))
		job.complete()
		return
	}
	v.tasks.Add(1)
	v.mu.Unlock()
	go func() {
		defer job.complete()
		defer v.tasks.Done()
		host := v.spaces[job.space]
		var err error
		result := provider.Failed
		if host == nil {
			err = errors.Join(fmt.Errorf("unknown space %q", job.space), finishUnstarted(ctx, job.handle, result))
		} else {
			result, err = runJob(ctx, job.handle, host.environment, controllerOptions{Name: job.forge}, time.Minute)
		}
		slog.Info("Job completed", "forge", job.forge, "job", job.handle.Job().ID(), "result", result.String(), "err", err)
		// Execution and supervision errors belong to this job. runJob has
		// stopped execution, attempted completion and released the handle.
		// Only a dispatch invariant violation is fatal to the visor.
		if host == nil {
			v.recordFailure(fmt.Errorf("forge %q, space %q: %w", job.forge, job.space, err))
		}
	}()
}

func (v *SpaceVisor) recordFailure(err error) {
	v.mu.Lock()
	v.failure = errors.Join(v.failure, err)
	v.mu.Unlock()
	v.failOnce.Do(func() { close(v.failed) })
}

func (v *SpaceVisor) Close() error {
	v.closeOnce.Do(func() {
		v.mu.Lock()
		v.closed = true
		v.mu.Unlock()
		v.tasks.Wait()
		for name, host := range v.spaces {
			if err := host.Close(); err != nil {
				v.closeErr = errors.Join(v.closeErr, fmt.Errorf("close space %q: %w", name, err))
			}
		}
	})
	v.mu.Lock()
	defer v.mu.Unlock()
	return errors.Join(v.failure, v.closeErr)
}

func NewSpaceVisor(cfg *Config, demands Demands, providers map[string][]byte) (_ *SpaceVisor, err error) {
	v := &SpaceVisor{spaces: make(map[string]*managedSpace), failed: make(chan struct{})}
	defer func() {
		if err != nil {
			err = errors.Join(err, v.Close())
		}
	}()
	roots := make(map[string]string)
	for _, spec := range cfg.Spaces {
		pool := cfg.poolForSpace(spec.Base().ID)
		if pool == nil {
			continue
		}
		environment, err := spec.NewSpace(cfg.Directories, *pool, demands.Capacity(string(spec.Base().ID)), providers)
		if err != nil {
			return nil, err
		}
		id := string(spec.Base().ID)
		root := environment.Cwd()
		if other, exists := roots[root]; exists {
			return nil, errors.Join(fmt.Errorf("spaces %q and %q share a directory", other, id), environment.Destroy())
		}
		roots[root] = id
		v.spaces[id] = &managedSpace{environment: environment, demand: demands[id]}
	}
	return v, nil
}

// controllerOptions identifies the forge for execution diagnostics.
type controllerOptions struct{ Name string }

// runJob keeps supervision alive through execution cleanup and completion.
func runJob(ctx context.Context, job provider.Handle, environment space.Space, options controllerOptions, grace time.Duration) (result provider.Result, err error) {
	result = provider.Failed
	var data []byte
	var local provider.Run
	defer func() {
		if local != nil {
			stop, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			err = errors.Join(err, local.Stop(stop, provider.StopForce))
			cancel()
			err = errors.Join(err, <-local.Done())
			_, data = local.Result()
		}
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
		err = errors.Join(err, job.Finish(cleanup, result, data))
		cancel()
		cleanup, cancel = context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		err = errors.Join(err, job.Close(cleanup))
		cancel()
	}()
	if grace < 0 {
		return result, errors.New("negative job cancellation timeout")
	}
	if grace == 0 {
		grace = time.Minute
	}
	if stopped, status, cause := pendingStop(ctx, job); stopped {
		return status, cause
	}
	description := job.Job()
	startCtx, stopStart := context.WithCancel(ctx)
	defer stopStart()
	type startedRun struct {
		run provider.Run
		err error
	}
	started := make(chan startedRun, 1)
	events := job.Events()
	go func() {
		r, err := environment.Run(startCtx, description)
		started <- startedRun{r, err}
	}()
	select {
	case startedRun := <-started:
		local, err = startedRun.run, startedRun.err
		if err != nil {
			if errors.Is(err, context.Canceled) && errors.Is(context.Cause(ctx), errJobShutdown) {
				return provider.Canceled, nil
			}
			return result, err
		}
	case event, ok := <-events:
		stopStart()
		local = (<-started).run
		if local == nil {
			return stoppedJob(event, ok)
		}
		pending := make(chan provider.Event, 1)
		if ok {
			pending <- event
		} else {
			close(pending)
		}
		events = pending
	case <-ctx.Done():
		stopStart()
		local = (<-started).run
		if local == nil {
			if errors.Is(context.Cause(ctx), errJobShutdown) {
				return provider.Canceled, nil
			}
			return provider.Canceled, ctx.Err()
		}
	}
	result, data, err = executeJob(ctx, events, local, grace)
	return result, err
}

func pendingStop(ctx context.Context, job provider.Handle) (bool, provider.Result, error) {
	if err := ctx.Err(); err != nil {
		if errors.Is(context.Cause(ctx), errJobShutdown) {
			return true, provider.Canceled, nil
		}
		return true, provider.Canceled, err
	}
	select {
	case event, ok := <-job.Events():
		status, cause := stoppedJob(event, ok)
		return true, status, cause
	default:
		return false, provider.Unknown, nil
	}
}

func eventError(event provider.Event) error {
	if event.Err != nil {
		return event.Err
	}
	return fmt.Errorf("job supervision failed (event %d)", event.Type)
}

func executeJob(ctx context.Context, events <-chan provider.Event, local provider.Run, grace time.Duration) (provider.Result, []byte, error) {
	var cause error
	result, shutdownRequested := provider.Canceled, false
	for cause == nil {
		select {
		case err := <-local.Done():
			return jobOutcome(local, err)
		case <-ctx.Done():
			cause, shutdownRequested = ctx.Err(), true
			if errors.Is(context.Cause(ctx), errJobShutdown) {
				cause = errJobCanceled
			}
		case event, ok := <-events:
			if !ok {
				cause = errors.New("job supervision stopped while execution was running")
				break
			}
			switch event.Type {
			case provider.Cancellation:
				if event.Grace > 0 {
					grace = event.Grace
				}
				cause = errJobCanceled
			case provider.Shutdown:
				cause, shutdownRequested = errJobCanceled, true
			case provider.LeaseLost:
				cause, result = eventError(event), provider.Abandoned
			case provider.NetworkError:
				cause = eventError(event)
			default:
				cause = errors.New("unsupported job supervision event")
			}
		}
	}
	shutdown, cancelShutdown := context.WithTimeout(context.WithoutCancel(ctx), grace)
	defer cancelShutdown()
	send, cancel := context.WithTimeout(shutdown, 30*time.Second)
	var sendErr error
	if shutdownRequested {
		sendErr = local.Stop(send, provider.StopShutdown)
	} else {
		sendErr = local.Stop(send, provider.StopCancel)
	}
	cancel()
	// A failed send may mean that the process has already completed.
	select {
	case exitErr := <-local.Done():
		actual, data, exitErr := jobOutcome(local, exitErr)
		if errors.Is(cause, errJobCanceled) {
			cause = nil
		}
		return actual, data, errors.Join(cause, exitErr)
	case <-shutdown.Done():
	}
	force, cancelForce := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancelForce()
	stopErr := local.Stop(force, provider.StopForce)
	if errors.Is(cause, errJobCanceled) {
		cause = errors.New("run exceeded cancellation timeout")
	}
	exitErr := <-local.Done()
	_, data := local.Result()
	return result, data, errors.Join(cause, sendErr, stopErr, exitErr)
}

var errJobCanceled = errors.New("job canceled by server")

func jobOutcome(local provider.Run, err error) (provider.Result, []byte, error) {
	result, data := local.Result()
	if !result.Valid() {
		result = provider.Failed
	}
	return result, data, err
}

func stoppedJob(event provider.Event, ok bool) (provider.Result, error) {
	if !ok {
		return provider.Failed, errors.New("job supervision stopped before execution")
	}
	switch event.Type {
	case provider.Cancellation, provider.Shutdown:
		return provider.Canceled, nil
	case provider.LeaseLost:
		return provider.Abandoned, eventError(event)
	default:
		return provider.Failed, eventError(event)
	}
}
