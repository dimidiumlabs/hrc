// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"dimidiumlabs.io/hrc/internal/provider"
	"dimidiumlabs.io/hrc/internal/space"
)

type fakeSpace struct {
	job     *fakeJob
	created int
	prepare func(context.Context) error
	calls   *[]string
}

func (*fakeSpace) Destroy() error { return nil }

func (*fakeSpace) Name() string { return "test-space" }

func (*fakeSpace) Status() space.Status { return space.Ready }

func (*fakeSpace) State(context.Context) (space.State, error) {
	return space.State{}, nil
}

func (*fakeSpace) Type() string { return "test" }

func (*fakeSpace) Cwd() string { return "" }

func (f *fakeSpace) Run(ctx context.Context, _ provider.Job) (provider.Run, error) {
	f.created++
	if f.prepare != nil {
		if err := f.prepare(ctx); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.job.started = true
	if f.job.onRun != nil {
		f.job.onRun()
	}
	return f.job.local, nil
}

type fakeQueue struct{ job *fakeJob }

func (q *fakeQueue) Serve(ctx context.Context, capacity provider.Demand) <-chan provider.Handle {
	jobs := make(chan provider.Handle)
	go func() {
		defer close(jobs)
		reservation := capacity.Reserve()
		if err := reservation.Wait(ctx); err != nil {
			reservation.Close()
			return
		}
		q.job.reservation = reservation
		select {
		case jobs <- q.job:
		case <-ctx.Done():
			q.job.Close(context.Background())
		}
	}()
	return jobs
}

type fakeJob struct {
	events      chan provider.Event
	local       *fakeRun
	onRun       func()
	calls       *[]string
	result      provider.Result
	finishErr   error
	closeErr    error
	reservation provider.Reservation
	closeOnce   sync.Once
	started     bool
}

func (*fakeJob) ID() string { return "test-job" }

func (j *fakeJob) Events() <-chan provider.Event { return j.events }

func (j *fakeJob) Job() provider.Job { return &fakeDescription{} }

type fakeDescription struct{}

func (*fakeDescription) Type() string { return "test" }

func (*fakeDescription) ID() string { return "test-job" }

func (*fakeDescription) Marshal() ([]byte, error) { panic("controller must not serialize Job") }

func (*fakeDescription) Unmarshal([]byte) error { return nil }

func (j *fakeJob) Finish(_ context.Context, result provider.Result, _ []byte) error {
	j.result = result
	*j.calls = append(*j.calls, "finish")
	return j.finishErr
}

func (j *fakeJob) Close(context.Context) error {
	j.closeOnce.Do(func() {
		*j.calls = append(*j.calls, "job-close")
		if j.reservation != nil {
			j.reservation.Close()
		}
	})
	return j.closeErr
}

type fakeRun struct {
	done     chan error
	ready    chan struct{}
	result   provider.Result
	calls    *[]string
	stop     func()
	finished sync.Once
	closed   sync.Once
}

func (r *fakeRun) finish(result provider.Result) {
	r.finished.Do(func() { r.result = result; close(r.ready); r.done <- nil; close(r.done) })
}

func (r *fakeRun) Done() <-chan error { return r.done }

func (r *fakeRun) Result() (provider.Result, []byte) {
	select {
	case <-r.ready:
		return r.result, nil
	default:
		return provider.Unknown, nil
	}
}

func (r *fakeRun) Stop(_ context.Context, reason provider.StopReason) error {
	switch reason {
	case provider.StopCancel, provider.StopShutdown:
		command := "cancel"
		if reason == provider.StopShutdown {
			command = "shutdown"
		}
		*r.calls = append(*r.calls, command)
		if r.stop != nil {
			r.stop()
		}
	case provider.StopForce:
		r.closed.Do(func() {
			*r.calls = append(*r.calls, "run-close")
			r.finish(provider.Unknown)
		})
	default:
		return provider.ErrInvalidStopReason
	}
	return nil
}

func fakeController(t *testing.T) (*demand, *fakeSpace, *fakeJob, *[]string) {
	t.Helper()
	calls := &[]string{}
	environment := &fakeSpace{calls: calls}
	host := newDemand(1)
	t.Cleanup(func() { host.Close() })
	run := &fakeRun{done: make(chan error, 1), ready: make(chan struct{}), calls: calls}
	run.stop = func() { run.finish(provider.Canceled) }
	job := &fakeJob{events: make(chan provider.Event, 1), local: run, calls: calls}
	environment.job = job
	return host, environment, job, calls
}

type queueSession struct {
	ctx  context.Context
	jobs chan provider.Handle
}

type reopeningQueue struct{ sessions chan queueSession }

func (q *reopeningQueue) Serve(ctx context.Context, _ provider.Demand) <-chan provider.Handle {
	input := make(chan provider.Handle)
	output := make(chan provider.Handle)
	q.sessions <- queueSession{ctx: ctx, jobs: input}
	go func() {
		defer close(output)
		for {
			select {
			case handle, ok := <-input:
				if !ok {
					return
				}
				select {
				case output <- handle:
				case <-ctx.Done():
					finishUnstarted(ctx, handle, provider.Canceled)
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return output
}

type handoffQueue struct {
	handle   provider.Handle
	received chan struct{}
}

func (q *handoffQueue) Serve(ctx context.Context, d provider.Demand) <-chan provider.Handle {
	jobs := make(chan provider.Handle)
	go func() {
		defer close(jobs)
		jobs <- q.handle
		close(q.received)
		<-ctx.Done()
	}()
	return jobs
}

// A queue must finish before its shared space is destroyed.
type waitingQueue struct {
	ready   chan struct{}
	stopped chan struct{}
}

func (q *waitingQueue) Serve(ctx context.Context, _ provider.Demand) <-chan provider.Handle {
	jobs := make(chan provider.Handle)
	close(q.ready)
	go func() {
		<-ctx.Done()
		close(q.stopped)
		close(jobs)
	}()
	return jobs
}

// openQueue delivers one job and keeps its stream open until acquisition stops.
// Unlike fakeQueue's finite stream, this does not trigger queue reopening.
type openQueue struct{ job *fakeJob }

func (q *openQueue) Serve(ctx context.Context, demand provider.Demand) <-chan provider.Handle {
	jobs := make(chan provider.Handle)
	go func() {
		defer close(jobs)
		for handle := range (&fakeQueue{q.job}).Serve(ctx, demand) {
			select {
			case jobs <- handle:
			case <-ctx.Done():
				finishUnstarted(ctx, handle, provider.Canceled)
			}
		}
		<-ctx.Done()
	}()
	return jobs
}

type closingSpace struct {
	space.Space
	destroy func() error
}

func (s *closingSpace) Destroy() error { return s.destroy() }

type invalidConfigQueue struct {
	ready <-chan struct{}
	fail  func(error)
}

func (q *invalidConfigQueue) Serve(ctx context.Context, _ provider.Demand) <-chan provider.Handle {
	jobs := make(chan provider.Handle)
	go func() {
		defer close(jobs)
		select {
		case <-q.ready:
			q.fail(fmt.Errorf("forge broken: %w", provider.ErrInvalidConfig))
		case <-ctx.Done():
		}
	}()
	return jobs
}

type finalizationHandle struct {
	*fakeJob
	key                           string
	gate                          <-chan struct{}
	finishFailures, closeFailures int
	finishCalls, closeCalls       int
}

func (h *finalizationHandle) Job() provider.Job {
	return &provider.OpaqueJob{Key: h.key, Provider: "test"}
}

func (h *finalizationHandle) Finish(ctx context.Context, result provider.Result, data []byte) error {
	h.finishCalls++
	if h.finishFailures > 0 {
		h.finishFailures--
		return provider.ErrUnavailable
	}
	return h.fakeJob.Finish(ctx, result, data)
}

func (h *finalizationHandle) Close(ctx context.Context) error {
	h.closeCalls++
	if h.closeFailures > 0 {
		h.closeFailures--
		return provider.ErrUnavailable
	}
	return h.fakeJob.Close(ctx)
}

type gatedQueue struct{ handles []*finalizationHandle }

func (q *gatedQueue) Serve(ctx context.Context, demand provider.Demand) <-chan provider.Handle {
	jobs := make(chan provider.Handle)
	go func() {
		defer close(jobs)
		for _, handle := range q.handles {
			if handle.gate != nil {
				select {
				case <-handle.gate:
				case <-ctx.Done():
					return
				}
			}
			reservation := demand.Reserve()
			if err := reservation.Wait(ctx); err != nil {
				reservation.Close()
				return
			}
			handle.reservation = reservation
			select {
			case jobs <- handle:
			case <-ctx.Done():
				handle.Close(context.Background())
				return
			}
		}
		<-ctx.Done()
	}()
	return jobs
}

type runSpace struct {
	space.Space
	run func(context.Context, provider.Job) (provider.Run, error)
}

func (s *runSpace) Run(ctx context.Context, job provider.Job) (provider.Run, error) {
	return s.run(ctx, job)
}

type cleanupRun struct {
	provider.Run
	forceFailures int
	wedged        bool
}

func (r *cleanupRun) Stop(ctx context.Context, reason provider.StopReason) error {
	if reason == provider.StopForce {
		if r.forceFailures > 0 {
			r.forceFailures--
			return space.ErrUnavailable
		}
		if r.wedged {
			return nil
		}
	}
	return r.Run.Stop(ctx, reason)
}
