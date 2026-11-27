// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"dimidiumlabs.io/hrc/internal/provider"
	"dimidiumlabs.io/hrc/internal/space"
)

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

func TestControllerIsolatesJobErrors(t *testing.T) {
	for _, phase := range []string{"submission", "execution", "worker", "listener", "lease", "finish"} {
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
			case "finish":
				handle.local.finish(provider.Failed)
				handle.finishErr = provider.ErrUnavailable
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
			err := c.Close()
			if phase == "finish" {
				if err == nil {
					t.Fatal("finalization failure was silently discarded")
				}
			} else if err != nil {
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

func TestSpaceCollectionFailureDoesNotCancelHealthySpace(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		capacity, broken, first, _ := fakeController(t)
		otherCapacity, healthy, second, _ := fakeController(t)
		first.local.finish(provider.Succeeded)
		ready := make(chan struct{})
		second.onRun = func() { close(ready) }
		run := &cleanupRun{Run: first.local, forceFailures: 1}
		environment := &runSpace{Space: broken, run: func(context.Context, provider.Job) (provider.Run, error) {
			return run, nil
		}}
		handle := &finalizationHandle{fakeJob: first, key: "broken", gate: ready}
		c := &Controller{
			spaces: map[string]*managedSpace{"a": {environment: environment}, "b": {environment: healthy}},
			forges: []managedForge{
				{id: "broken", space: "a", queue: &gatedQueue{[]*finalizationHandle{handle}}, demand: capacity},
				{id: "healthy", space: "b", queue: &openQueue{second}, demand: otherCapacity},
			},
		}
		done := make(chan error, 1)
		go func() { done <- c.Run(t.Context()) }()
		synctest.Wait()
		jobs := c.Jobs()
		if len(jobs) != 1 || jobs[0].forge != "healthy" || jobs[0].ctx.Err() != nil {
			t.Fatal("damaged space canceled healthy work")
		}
		if handle.closeCalls != 1 || !slices.Contains(*first.calls, "job-close") {
			t.Fatal("space cleanup lost queue handle finalization")
		}
		c.Drain()
		synctest.Wait()
		second.local.finish(provider.Succeeded)
		if err := <-done; !errors.Is(err, space.ErrUnavailable) {
			t.Fatal("lost space cleanup error", err)
		}
		if c.resources.Len() != 0 || second.result != provider.Succeeded {
			t.Fatal("healthy graph did not drain")
		}
	})
}

func TestFinalizationFailureCollectsQueueButPreservesHealthyWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		capacity, broken, first, _ := fakeController(t)
		otherCapacity, healthy, second, _ := fakeController(t)
		first.local.finish(provider.Succeeded)
		ready := make(chan struct{})
		second.onRun = func() { close(ready) }
		handle := &finalizationHandle{fakeJob: first, key: "broken", finishFailures: 1, gate: ready}
		c := &Controller{
			spaces: map[string]*managedSpace{"a": {environment: broken}, "b": {environment: healthy}},
			forges: []managedForge{
				{id: "broken", space: "a", queue: &gatedQueue{[]*finalizationHandle{handle}}, demand: capacity},
				{id: "healthy", space: "b", queue: &openQueue{second}, demand: otherCapacity},
			},
		}
		done := make(chan error, 1)
		go func() { done <- c.Run(t.Context()) }()
		synctest.Wait()
		jobs := c.Jobs()
		if len(jobs) != 1 || jobs[0].forge != "healthy" || jobs[0].ctx.Err() != nil {
			t.Fatal("failed queue affected healthy execution")
		}
		select {
		case <-c.Failed():
			t.Fatal("recoverable scoped cleanup stopped the controller")
		default:
		}
		if handle.finishCalls != 2 || handle.closeCalls != 1 {
			t.Fatalf("finalization not retried before releasing queue: finish=%d close=%d", handle.finishCalls, handle.closeCalls)
		}
		c.Drain()
		synctest.Wait()
		second.local.finish(provider.Succeeded)
		if err := <-done; !errors.Is(err, provider.ErrUnavailable) {
			t.Fatal("cleanup error lost", err)
		}
		if c.resources.Len() != 0 || second.result != provider.Succeeded {
			t.Fatal("remaining healthy graph did not drain")
		}
	})
}

func TestUncollectableJobEscalatesDuringDrain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		capacity, broken, first, _ := fakeController(t)
		otherCapacity, healthy, second, _ := fakeController(t)
		ready := make(chan struct{})
		second.onRun = func() { close(ready) }
		handle := &finalizationHandle{fakeJob: first, key: "broken", closeFailures: 100, gate: ready}
		c := &Controller{
			spaces: map[string]*managedSpace{"a": {environment: broken}, "b": {environment: healthy}},
			forges: []managedForge{
				{id: "broken", space: "a", queue: &gatedQueue{[]*finalizationHandle{handle}}, demand: capacity},
				{id: "healthy", space: "b", queue: &openQueue{second}, demand: otherCapacity},
			},
		}
		done := make(chan error, 1)
		go func() { done <- c.Run(t.Context()) }()
		synctest.Wait()
		c.Drain()
		synctest.Wait()
		first.local.finish(provider.Succeeded)
		synctest.Wait()
		err := <-done
		if !errors.Is(err, provider.ErrUnavailable) || !strings.Contains(err.Error(), "remain uncollected") {
			t.Fatal("uncollectable ownership lost", err)
		}
		if len(c.Jobs()) != 1 || c.resources.Len() != 3 || second.result != provider.Canceled {
			t.Fatalf("remaining jobs=%d resources=%d healthy result=%v", len(c.Jobs()), c.resources.Len(), second.result)
		}
	})
}
