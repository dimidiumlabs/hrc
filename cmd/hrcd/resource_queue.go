// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"context"
	"sync/atomic"

	"dimidiumlabs.io/hrc/internal/resource"
	"dimidiumlabs.io/hrc/internal/runtime"
)

// managedQueue keeps the provider's process membership alive until all jobs
// have discharged their handles. Quiescing only stops acquisition.
type managedQueue struct {
	id        string
	level     resource.Level
	started   atomic.Bool
	done      chan struct{}
	ctx       context.Context
	stop      context.CancelFunc
	processes *runtime.Pool
}

func (q *managedQueue) Quiesce(ctx context.Context) error {
	q.stop()
	if !q.started.Load() {
		return nil
	}
	select {
	case <-q.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (q *managedQueue) Shutdown(ctx context.Context, level resource.Level) error {
	if err := q.Quiesce(ctx); err != nil {
		return err
	}
	return collectProcesses(ctx, q.processes, level)
}

// An acquisition guard retains its queue and placement until all in-flight
// handles have transferred into the graph. It is requested only after handoff
// stops, so a requested infrastructure object cannot disappear during adoption.
type acquisitionResource struct{}

func (*acquisitionResource) Quiesce(context.Context) error                  { return nil }
func (*acquisitionResource) Shutdown(context.Context, resource.Level) error { return nil }
