// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"dimidiumlabs.io/hrc/internal/provider"
	"dimidiumlabs.io/hrc/internal/space"
)

func TestRunJobCompletesWithProviderResult(t *testing.T) {
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

func TestRunJobCancellationBeforeExecution(t *testing.T) {
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

func TestRunJobStopsRuns(t *testing.T) {
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

func TestRunJobPreparationFailure(t *testing.T) {
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
