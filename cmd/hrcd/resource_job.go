// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"context"
	"errors"
	"log/slog"

	hrcconfig "dimidiumlabs.io/hrc/internal/config"
	"dimidiumlabs.io/hrc/internal/provider"
	"dimidiumlabs.io/hrc/internal/resource"
)

type jobResource struct{ job *acquiredJob }

func (*jobResource) Quiesce(context.Context) error { return nil }
func (r *jobResource) Shutdown(ctx context.Context, level resource.Level) error {
	job := r.job
	if level >= resource.Term {
		job.Shutdown()
	}
	select {
	case <-job.executed:
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := job.finalization.Shutdown(ctx, level); err != nil {
		return err
	}
	job.complete()
	return nil
}

// finalizationError identifies the dependency whose operation failed. Execution
// failure is a job outcome, not by itself a collection failure.
type finalizationError struct {
	queue bool
	err   error
}

func (e *finalizationError) Error() string { return e.err.Error() }
func (e *finalizationError) Unwrap() error { return e.err }

type jobFinalization struct {
	handle                    provider.Handle
	local                     provider.Run
	result                    provider.Result
	data                      []byte
	loss                      error
	stopped, finished, closed bool
}

func (f *jobFinalization) Shutdown(ctx context.Context, level resource.Level) error {
	if f.local != nil && !f.stopped {
		stop, cancel := context.WithTimeout(ctx, hrcconfig.ControlTimeout)
		err := f.local.Stop(stop, provider.StopForce)
		if err != nil {
			cancel()
			return &finalizationError{err: err}
		}
		select {
		case <-f.local.Done():
			f.stopped = true
			_, f.data = f.local.Result()
		case <-stop.Done():
			err = stop.Err()
		}
		cancel()
		if err != nil {
			return &finalizationError{err: err}
		}
	}
	var lost error
	if !f.finished {
		finish, cancel := context.WithTimeout(ctx, hrcconfig.JobFinishTimeout)
		err := f.handle.Finish(finish, f.result, f.data)
		cancel()
		if err != nil {
			if level == resource.Down || ctx.Err() != nil {
				return &finalizationError{queue: true, err: err}
			}
			// A broken queue may never accept a result. Escalated cleanup
			// releases local ownership, but reports the lost result honestly.
			lost = err
		} else {
			f.finished = true
		}
	}
	if !f.closed {
		closeCtx, cancel := context.WithTimeout(ctx, hrcconfig.JobCloseTimeout)
		err := f.handle.Close(closeCtx)
		cancel()
		if err != nil {
			return &finalizationError{queue: true, err: errors.Join(lost, err)}
		}
		f.closed = true
	}
	if lost != nil {
		f.loss = lost
		slog.Error("Job result lost during escalated finalization", "job", f.handle.Job().ID(), "err", lost)
	}
	return nil
}
