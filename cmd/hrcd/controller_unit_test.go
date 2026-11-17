// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"dimidiumlabs.io/hrc/internal/provider"
	"dimidiumlabs.io/hrc/internal/space"
)

type fakeSpace struct {
	job     *fakeJob
	created int
	prepare func(context.Context) error
	calls   *[]string
}

func (*fakeSpace) Destroy() error       { return nil }
func (*fakeSpace) Name() string         { return "test-space" }
func (*fakeSpace) Status() space.Status { return space.Ready }
func (*fakeSpace) State(context.Context) (space.State, error) {
	return space.State{}, nil
}
func (*fakeSpace) Type() string { return "test" }
func (*fakeSpace) Cwd() string  { return "" }
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

func (*fakeJob) ID() string                      { return "test-job" }
func (j *fakeJob) Events() <-chan provider.Event { return j.events }
func (j *fakeJob) Job() provider.Job             { return &fakeDescription{} }

type fakeDescription struct{}

func (*fakeDescription) Type() string { return "test" }

func (*fakeDescription) ID() string               { return "test-job" }
func (*fakeDescription) Marshal() ([]byte, error) { panic("controller must not serialize Job") }
func (*fakeDescription) Unmarshal([]byte) error   { return nil }
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

func TestControllerWithFakeProviderAndSpace(t *testing.T) {
	for _, result := range []provider.Result{provider.Succeeded, provider.Failed} {
		t.Run(result.String(), func(t *testing.T) {
			host, environment, job, calls := fakeController(t)
			job.local.finish(result)
			if err := serveJobs(t.Context(), &fakeQueue{job}, environment, host, controllerOptions{}); err != nil {
				t.Fatal(err)
			}
			if job.result != result || environment.created != 1 {
				t.Fatalf("result=%s created=%d", job.result, environment.created)
			}
			if !slices.Equal(*calls, []string{"run-close", "finish", "job-close"}) {
				t.Fatalf("cleanup order: %v", *calls)
			}
			if available, _ := host.State(); available != 1 {
				t.Fatalf("capacity was not released: %d", available)
			}
		})
	}
}

func TestCancellationBeforeExecution(t *testing.T) {
	for _, duringPreparation := range []bool{false, true} {
		t.Run(map[bool]string{false: "before-preparation", true: "during-preparation"}[duringPreparation], func(t *testing.T) {
			host, environment, job, _ := fakeController(t)
			cancel := func() error {
				job.events <- provider.Event{Type: provider.Cancellation}
				return nil
			}
			expected := 0
			if duringPreparation {
				expected = 1
				environment.prepare = func(ctx context.Context) error { cancel(); <-ctx.Done(); return ctx.Err() }
			} else {
				cancel()
			}
			if err := serveJobs(t.Context(), &fakeQueue{job}, environment, host, controllerOptions{}); err != nil {
				t.Fatal(err)
			}
			if job.started || job.result != provider.Canceled || environment.created != expected {
				t.Fatalf("started=%v result=%s created=%d", job.started, job.result, environment.created)
			}
		})
	}
}

func TestControllerStopsRuns(t *testing.T) {
	for _, mode := range []string{"cancel", "shutdown", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			host, environment, job, calls := fakeController(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			job.onRun = func() {
				if mode == "shutdown" {
					cancel()
				} else {
					job.events <- provider.Event{Type: provider.Cancellation, Grace: 5 * time.Millisecond}
				}
			}
			if mode == "timeout" {
				job.local.stop = nil
			}
			err := serveJobs(ctx, &fakeQueue{job}, environment, host, controllerOptions{})
			if job.result != provider.Canceled || (err != nil) != (mode != "cancel") {
				t.Fatalf("result=%s error=%v", job.result, err)
			}
			if mode == "shutdown" && !errors.Is(err, context.Canceled) {
				t.Fatalf("shutdown cause lost: %v", err)
			}
			command := "cancel"
			if mode == "shutdown" {
				command = "shutdown"
			}
			if !slices.Equal(*calls, []string{command, "run-close", "finish", "job-close"}) {
				t.Fatalf("shutdown order: %v", *calls)
			}
		})
	}
}

func TestPreparationFailureCompletesAcquiredJob(t *testing.T) {
	host, environment, job, calls := fakeController(t)
	failure := errors.New("space is unavailable")
	environment.prepare = func(context.Context) error { return failure }
	err := serveJobs(t.Context(), &fakeQueue{job}, environment, host, controllerOptions{})
	if !errors.Is(err, failure) || job.result != provider.Failed || job.started {
		t.Fatalf("started=%v result=%s error=%v", job.started, job.result, err)
	}
	if !slices.Equal(*calls, []string{"finish", "job-close"}) {
		t.Fatalf("cleanup after preparation failure: %v", *calls)
	}
}

// serveJobs acquires jobs within demand and supervises execution through a space.
// The caller owns the space and closes it after its queues have stopped.
func serveJobs(ctx context.Context, queue provider.Queue, environment space.Space, demand provider.Demand, options controllerOptions) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var failed sync.Once
	var failure error
	var tasks sync.WaitGroup
	for job := range queue.Serve(ctx, demand) {
		tasks.Go(func() {
			_, err := runJob(ctx, job, environment, options, time.Minute)
			if err != nil {
				failed.Do(func() { failure = err })
			}
			if errors.Is(err, space.ErrUnavailable) {
				cancel()
			}
		})
	}
	tasks.Wait()
	return failure
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

func TestControllerDrainsWhenAllQueuesClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	queue := &reopeningQueue{sessions: make(chan queueSession, 4)}
	capacity, _, handle, _ := fakeController(t)
	c := &Controller{forges: []managedForge{{id: "forge", space: "target", queue: queue, demand: capacity}}}
	jobs := c.Serve(ctx)
	session := <-queue.sessions
	session.jobs <- handle
	task := <-jobs
	close(session.jobs)
	<-session.ctx.Done()
	if task.ctx.Err() != nil {
		t.Fatal("queue exhaustion canceled an acquired job")
	}
	select {
	case <-queue.sessions:
		t.Fatal("controller retried after all queues closed")
	case <-jobs:
		t.Fatal("controller stopped before job cleanup")
	default:
	}
	handle.Finish(ctx, provider.Succeeded, nil)
	handle.Close(ctx)
	task.complete()
	if _, ok := <-jobs; ok {
		t.Fatal("unexpected job after all queues closed")
	}
	if err := c.Close(); err == nil || !strings.Contains(err.Error(), "all queues closed") {
		t.Fatalf("lost all-queues-closed error: %v", err)
	}
	if ctx.Err() != nil {
		t.Fatal("test completed only because its context expired")
	}
}

func TestControllerRetriesQueuesIndependently(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		queue := &reopeningQueue{sessions: make(chan queueSession, 4)}
		healthy := &reopeningQueue{sessions: make(chan queueSession, 4)}
		capacity, _, handle, _ := fakeController(t)
		c := &Controller{forges: []managedForge{
			{id: "retrying", space: "target", queue: queue, demand: capacity},
			{id: "healthy", space: "target", queue: healthy, demand: capacity},
		}}
		jobs := c.Serve(ctx)
		session, other := <-queue.sessions, <-healthy.sessions
		close(session.jobs)
		synctest.Wait()
		// A failed queue must not prevent a healthy queue from delivering
		// jobs, nor cancel those jobs while reconnection is in progress.
		other.jobs <- handle
		task := <-jobs
		for _, delay := range []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 32 * time.Second, time.Minute, time.Minute} {
			time.Sleep(delay - time.Nanosecond)
			synctest.Wait()
			select {
			case <-queue.sessions:
				t.Fatalf("queue reopened before %v backoff", delay)
			default:
			}
			time.Sleep(time.Nanosecond)
			synctest.Wait()
			select {
			case session = <-queue.sessions:
			default:
				t.Fatalf("queue did not reopen after %v", delay)
			}
			if task.ctx.Err() != nil || other.ctx.Err() != nil {
				t.Fatal("failed queue interrupted healthy execution or acquisition")
			}
			close(session.jobs)
			synctest.Wait()
		}
		// A delivered job resets this queue's backoff to two seconds.
		time.Sleep(time.Minute)
		synctest.Wait()
		session = <-queue.sessions
		_, _, recovered, _ := fakeController(t)
		session.jobs <- recovered
		recoveredTask := <-jobs
		close(session.jobs)
		synctest.Wait()
		time.Sleep(2 * time.Second)
		synctest.Wait()
		select {
		case session = <-queue.sessions:
		default:
			t.Fatal("successful delivery did not reset backoff")
		}
		close(session.jobs)
		synctest.Wait()
		close(other.jobs) // Now every queue is closed, including retries.
		synctest.Wait()
		if task.ctx.Err() != nil || recoveredTask.ctx.Err() != nil {
			t.Fatal("all queues closing canceled active jobs")
		}
		select {
		case <-jobs:
			t.Fatal("controller exited before jobs were drained")
		default:
		}
		for _, pending := range []*acquiredJob{task, recoveredTask} {
			pending.handle.Finish(context.Background(), provider.Succeeded, nil)
			pending.handle.Close(context.Background())
			pending.complete()
		}
		if _, ok := <-jobs; ok {
			t.Fatal("unexpected extra job")
		}
		if err := c.Close(); err == nil || !strings.Contains(err.Error(), "all queues closed") {
			t.Fatalf("controller did not exit after all queues closed: %v", err)
		}
		time.Sleep(time.Minute)
		synctest.Wait()
		select {
		case <-queue.sessions:
			t.Fatal("queue reopened after all queues closed")
		default:
		}
	})
}

func TestControllerCancellationDuringQueueBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		queue := &reopeningQueue{sessions: make(chan queueSession, 4)}
		healthy := &reopeningQueue{sessions: make(chan queueSession, 4)}
		c := &Controller{forges: []managedForge{
			{id: "retrying", queue: queue},
			{id: "healthy", queue: healthy},
		}}
		jobs := c.Serve(ctx)
		session := <-queue.sessions
		<-healthy.sessions
		close(session.jobs)
		synctest.Wait()
		cancel()
		if _, ok := <-jobs; ok {
			t.Fatal("unexpected job during cancellation")
		}
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Minute)
		synctest.Wait()
		select {
		case <-queue.sessions:
			t.Fatal("queue reopened after cancellation")
		default:
		}
	})
}

func TestControllerCancellationWhileDrainingQueue(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	queue := &reopeningQueue{sessions: make(chan queueSession, 2)}
	capacity, _, handle, _ := fakeController(t)
	c := &Controller{forges: []managedForge{{id: "forge", space: "target", queue: queue, demand: capacity}}}
	jobs := c.Serve(ctx)
	session := <-queue.sessions
	session.jobs <- handle
	task := <-jobs
	close(session.jobs)
	<-session.ctx.Done()
	cancel()
	if _, ok := <-jobs; ok {
		t.Fatal("shutdown delivered another job")
	}
	if task.ctx.Err() != nil {
		t.Fatal("acquisition cancellation stopped execution before explicit shutdown")
	}
	task.Shutdown()
	handle.Finish(context.Background(), provider.Canceled, nil)
	handle.Close(context.Background())
	task.complete()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-queue.sessions:
		t.Fatal("queue reopened during daemon shutdown")
	default:
	}
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

func TestControllerHandleOwnership(t *testing.T) {
	for _, transfer := range []bool{false, true} {
		name := "cancel-before-transfer"
		if transfer {
			name = "transfer"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			calls := &[]string{}
			handle := &fakeJob{calls: calls}
			queue := &handoffQueue{handle: handle, received: make(chan struct{})}
			c := &Controller{forges: []managedForge{{id: "forge", space: "target", queue: queue, demand: newDemand(1)}}}
			tasks := c.Serve(ctx)
			<-queue.received // controller now owns the handle
			if transfer {
				task := <-tasks
				if task.handle != handle || task.forge != "forge" || task.space != "target" {
					t.Fatalf("incorrect transfer: %+v", task)
				}
				task.complete()
			}
			cancel()
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
			if transfer {
				if len(*calls) != 0 {
					t.Fatalf("controller finalized transferred handle: %v", *calls)
				}
			} else {
				if !slices.Equal(*calls, []string{"finish", "job-close"}) || handle.result != provider.Canceled {
					t.Fatalf("undelivered handle leaked: %v, %v", *calls, handle.result)
				}
			}
		})
	}
}
