// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"dimidiumlabs.io/hrc/internal/provider"
	"dimidiumlabs.io/hrc/internal/space"
)

func TestForgeSpaceConfiguration(t *testing.T) {
	const base = poolConfigJSON
	var cfg Config
	if err := json.Unmarshal([]byte(base), &cfg); err != nil {
		t.Fatal(err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if cfg.poolForSpace("build") != &cfg.Slots[0] {
		t.Fatal("pool placement lost")
	}
	cfg.Slots[0].Base().Space = "missing"
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "unknown space") {
		t.Fatalf("unknown space: %v", err)
	}
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

func TestControllerRoutesAndDrainsJob(t *testing.T) {
	capacity, environment, handle, _ := fakeController(t)
	started := make(chan struct{})
	handle.onRun = func() { close(started) }
	destroyed := false
	c := &Controller{
		forges: []managedForge{{id: "forge", space: "target", queue: &openQueue{handle}, demand: capacity}},
		spaces: map[string]*managedSpace{"target": {environment: &closingSpace{Space: environment, destroy: func() error {
			if handle.result != provider.Canceled {
				t.Error("space destroyed before job completion")
			}
			destroyed = true
			return nil
		}}}},
		failed: make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("controller did not dispatch job")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("controller did not drain")
	}
	if len(c.Jobs()) != 0 || !destroyed {
		t.Fatal("controller retained jobs or space")
	}
}

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

func TestControllerInvalidConfigurationIsFatal(t *testing.T) {
	waiting := &waitingQueue{ready: make(chan struct{}), stopped: make(chan struct{})}
	c := &Controller{failed: make(chan struct{})}
	c.forges = []managedForge{
		{id: "healthy", queue: waiting},
		{id: "broken", queue: &invalidConfigQueue{ready: waiting.ready, fail: c.recordFailure}},
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := c.Run(ctx); !errors.Is(err, provider.ErrInvalidConfig) || !strings.Contains(err.Error(), "broken") {
		t.Fatalf("configuration error did not stop controller: %v", err)
	}
	if ctx.Err() != nil {
		t.Fatal("controller waited for external shutdown")
	}
	select {
	case <-waiting.stopped:
	default:
		t.Fatal("fatal config error did not stop other queues")
	}
}

func TestControllerShutdownBeforeSpaceCleanup(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "cancellation"
		if fail {
			name = "job-error"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			capacity, environment, job, _ := fakeController(t)
			waiting := &waitingQueue{ready: make(chan struct{}), stopped: make(chan struct{})}
			executionErr := errors.New("execution failed")
			cleanupErr := errors.New("cleanup failed")
			destroyed, spareDestroyed := 0, 0
			shared := &managedSpace{
				demand: capacity,
				environment: &closingSpace{Space: environment, destroy: func() error {
					select {
					case <-waiting.stopped:
					default:
						t.Error("space destroyed before its controller stopped")
					}
					destroyed++
					if fail {
						return cleanupErr
					}
					return nil
				}},
			}
			spare := &managedSpace{
				demand:      newDemand(1),
				environment: &closingSpace{destroy: func() error { spareDestroyed++; return nil }},
			}
			v := &Controller{
				spaces: map[string]*managedSpace{"shared": shared, "spare": spare},
				failed: make(chan struct{}),
			}
			c := &Controller{forges: []managedForge{{id: "waiting", queue: waiting, space: "shared", demand: capacity}}}
			if fail {
				environment.prepare = func(context.Context) error {
					<-waiting.ready
					cancel()
					return executionErr
				}
				c.forges = append(c.forges, managedForge{id: "failing", queue: &openQueue{job}, space: "shared", demand: capacity})
			} else {
				go func() { <-waiting.ready; cancel() }()
			}
			go func() {
				select {
				case <-v.Failed():
					cancel()
				case <-ctx.Done():
				}
			}()
			for task := range c.Serve(ctx) {
				v.Submit(task)
			}
			for _, task := range c.Jobs() {
				task.Shutdown()
			}
			err := c.Close()
			if destroyed != 0 || spareDestroyed != 0 {
				t.Fatal("controller closed spaces it does not own")
			}
			err = errors.Join(err, v.Close())
			if fail {
				if errors.Is(err, executionErr) || !errors.Is(err, cleanupErr) ||
					!strings.Contains(err.Error(), "shared") {
					t.Fatalf("job error escaped isolation or lost space cleanup error: %v", err)
				}
				select {
				case <-v.Failed():
					t.Fatal("job error stopped the visor")
				default:
				}
			} else if err != nil {
				t.Fatalf("graceful shutdown: %v", err)
			}
			v.Close()
			if destroyed != 1 || spareDestroyed != 1 {
				t.Fatalf("spaces destroyed %d and %d times", destroyed, spareDestroyed)
			}
			if available, _ := capacity.State(); available != 1 {
				t.Fatalf("visor closed an externally owned demand: available=%d", available)
			}
		})
	}
}

func TestSpacevisorDrainsAcceptedJob(t *testing.T) {
	capacity, environment, handle, _ := fakeController(t)
	started := make(chan struct{})
	handle.onRun = func() { close(started) }
	destroyed := false
	host := &managedSpace{
		demand: capacity,
		environment: &closingSpace{Space: environment, destroy: func() error {
			destroyed = true
			if handle.result != provider.Succeeded {
				t.Error("space closed before successful job completion")
			}
			return nil
		}},
	}
	v := &Controller{spaces: map[string]*managedSpace{"target": host}, failed: make(chan struct{})}
	c := &Controller{forges: []managedForge{{id: "forge", space: "target", queue: &openQueue{handle}, demand: capacity}}}
	acquisition, stop := context.WithCancel(t.Context())
	defer stop()
	jobs := c.Serve(acquisition)
	v.Submit(<-jobs)
	<-started
	stop()
	for range jobs {
		t.Fatal("unexpected extra job")
	}
	done := make(chan error, 1)
	go func() { done <- c.Close() }()
	select {
	case err := <-done:
		t.Fatalf("controller finished before its task: %v", err)
	default:
	}
	handle.local.finish(provider.Succeeded)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if len(c.Jobs()) != 0 {
		t.Fatal("completed task remains active")
	}
	if destroyed {
		t.Fatal("controller destroyed space")
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	if !destroyed {
		t.Fatal("space was not closed")
	}
}

func TestSpacevisorIsolatesJobErrors(t *testing.T) {
	for _, phase := range []string{"submission", "execution", "worker", "listener", "lease", "finish", "close"} {
		t.Run(phase, func(t *testing.T) {
			capacity, environment, handle, calls := fakeController(t)
			spareCapacity, spare, spareHandle, _ := fakeController(t)
			expected := provider.Failed
			switch phase {
			case "submission":
				environment.prepare = func(context.Context) error { return space.ErrUnavailable }
			case "execution", "worker":
				failure := space.ErrUnavailable
				if phase == "worker" {
					failure = provider.ErrUnknown
				}
				handle.local.finished.Do(func() {
					handle.local.result = provider.Unknown
					close(handle.local.ready)
					handle.local.done <- failure
					close(handle.local.done)
				})
			case "listener", "lease":
				expected = provider.Canceled
				handle.onRun = func() {
					event := provider.Event{Type: provider.NetworkError, Err: provider.ErrUnavailable}
					if phase == "lease" {
						event = provider.Event{Type: provider.LeaseLost, Err: provider.ErrLeaseLost}
					}
					handle.events <- event
				}
			case "finish", "close":
				handle.local.finish(provider.Failed)
				if phase == "finish" {
					handle.finishErr = provider.ErrUnavailable
				} else {
					handle.closeErr = provider.ErrUnknown
				}
			}
			spareHandle.local.finish(provider.Succeeded)
			v := &Controller{
				spaces: map[string]*managedSpace{
					"broken": {environment: environment, demand: capacity},
					"spare":  {environment: spare, demand: spareCapacity},
				},
				failed: make(chan struct{}),
			}
			c := &Controller{forges: []managedForge{
				{id: "forge", space: "broken", queue: &openQueue{handle}, demand: capacity},
				{id: "other", space: "spare", queue: &openQueue{spareHandle}, demand: spareCapacity},
			}}
			acquisition, stop := context.WithCancel(t.Context())
			defer stop()
			jobs := c.Serve(acquisition)
			for range 2 {
				v.Submit(<-jobs)
			}
			stop()
			for range jobs {
				t.Fatal("unexpected extra job")
			}
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-v.Failed():
				t.Fatal("lost space stopped the controller")
			default:
			}
			if handle.result != expected || spareHandle.result != provider.Succeeded {
				t.Fatalf("results: failed=%v spare=%v", handle.result, spareHandle.result)
			}
			if !slices.Contains(*calls, "finish") || (*calls)[len(*calls)-1] != "job-close" {
				t.Fatalf("lost job was not completed and closed: %v", *calls)
			}
			if available, _ := capacity.State(); available != 1 {
				t.Fatalf("lost job reservation leaked: %d", available)
			}
			if phase == "listener" || phase == "lease" {
				if !slices.Contains(*calls, "cancel") || !slices.Contains(*calls, "run-close") {
					t.Fatalf("listener failure did not stop Worker: %v", *calls)
				}
			}
			// The same space must accept another job after the failed job
			// has released its reservation and finished all cleanup.
			_, _, next, _ := fakeController(t)
			environment.job = next
			environment.prepare = nil
			next.local.finish(provider.Succeeded)
			nextController := &Controller{forges: []managedForge{
				{id: "forge", space: "broken", queue: &openQueue{next}, demand: capacity},
			}}
			nextAcquisition, stopNext := context.WithCancel(t.Context())
			defer stopNext()
			nextJobs := nextController.Serve(nextAcquisition)
			v.Submit(<-nextJobs)
			stopNext()
			for range nextJobs {
				t.Fatal("unexpected extra job")
			}
			if err := nextController.Close(); err != nil || next.result != provider.Succeeded {
				t.Fatalf("subsequent job: %v %v", next.result, err)
			}
			if err := v.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestControllerExplicitDrain(t *testing.T) {
	for _, preparing := range []bool{false, true} {
		name := "running"
		if preparing {
			name = "preparing"
		}
		t.Run(name, func(t *testing.T) {
			acquisition, stopAcquisition := context.WithCancel(t.Context())
			defer stopAcquisition()
			capacity, environment, handle, calls := fakeController(t)
			started := make(chan struct{})
			if preparing {
				environment.prepare = func(ctx context.Context) error {
					close(started)
					<-ctx.Done()
					return ctx.Err()
				}
			} else {
				handle.onRun = func() { close(started) }
			}
			v := &Controller{
				spaces: map[string]*managedSpace{"target": {environment: environment, demand: capacity}},
				failed: make(chan struct{}),
			}
			c := &Controller{forges: []managedForge{{id: "forge", space: "target", queue: &openQueue{handle}, demand: capacity}}}
			jobs := c.Serve(acquisition)
			v.Submit(<-jobs)
			<-started
			stopAcquisition()
			for range jobs {
				t.Fatal("unexpected extra job")
			}
			tasks := c.Jobs()
			if len(tasks) != 1 {
				t.Fatalf("active tasks: %d", len(tasks))
			}
			if tasks[0].ctx.Err() != nil {
				t.Fatal("stopping acquisition also stopped execution")
			}
			for _, task := range tasks {
				task.Shutdown()
			}
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
			if len(c.Jobs()) != 0 || handle.result != provider.Canceled {
				t.Fatalf("drain incomplete: tasks=%d result=%v", len(c.Jobs()), handle.result)
			}
			if !preparing && !slices.Contains(*calls, "shutdown") {
				t.Fatalf("Run did not receive shutdown: %v", *calls)
			}
			if len(*calls) < 2 || (*calls)[len(*calls)-1] != "job-close" {
				t.Fatalf("Wait returned before Handle cleanup: %v", *calls)
			}
			if err := v.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
