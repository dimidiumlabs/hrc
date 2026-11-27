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
)

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
					t.Fatal("job error stopped the execution controller")
				default:
				}
			} else if err != nil {
				t.Fatalf("graceful shutdown: %v", err)
			}
			v.Close()
			wantDestroyed := 1
			if fail {
				wantDestroyed = 3 // DOWN, TERM, KILL; the failed resource remains owned.
			}
			if destroyed != wantDestroyed || spareDestroyed != 1 {
				t.Fatalf("spaces destroyed %d and %d times", destroyed, spareDestroyed)
			}
			if available, _ := capacity.State(); available != 1 {
				t.Fatalf("execution controller closed an externally owned demand: available=%d", available)
			}
		})
	}
}

func TestControllerDrainsAcceptedJob(t *testing.T) {
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

func TestControllerStopsJobsAfterAcquisitionStops(t *testing.T) {
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

func TestControllerDrainAndTermination(t *testing.T) {
	for _, terminate := range []bool{false, true} {
		name := "drain"
		if terminate {
			name = "term-during-drain"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				capacity, environment, handle, calls := fakeController(t)
				destroyed := false
				c := &Controller{
					forges: []managedForge{{id: "forge", space: "build", queue: &openQueue{handle}, demand: capacity}},
					spaces: map[string]*managedSpace{"build": {environment: &closingSpace{Space: environment, destroy: func() error {
						if !slices.Contains(*calls, "job-close") {
							t.Error("infrastructure collected before handle finalization")
						}
						destroyed = true
						return nil
					}}}},
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- c.Run(ctx) }()
				synctest.Wait()
				if !handle.started {
					t.Fatal("job not dispatched")
				}
				c.Drain()
				c.Drain() // Duplicate requests are harmless.
				synctest.Wait()
				if len(c.Jobs()) != 1 || destroyed || c.Jobs()[0].ctx.Err() != nil {
					t.Fatal("drain canceled accepted work or released its infrastructure")
				}
				if terminate {
					cancel()
				} else {
					handle.local.finish(provider.Succeeded)
				}
				if err := <-done; err != nil {
					t.Fatal(err)
				}
				want := provider.Succeeded
				if terminate {
					want = provider.Canceled
				}
				if handle.result != want || !destroyed || c.resources.Len() != 0 || len(c.Jobs()) != 0 {
					t.Fatalf("result=%v destroyed=%v remaining=%d", handle.result, destroyed, c.resources.Len())
				}
				if slices.Contains(*calls, "shutdown") != terminate {
					t.Fatalf("unexpected job termination: %v", *calls)
				}
			})
		})
	}
}

func TestControllerDrainStartsAlreadyAcceptedQueuedJob(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		_, _, first, _ := fakeController(t)
		_, _, second, _ := fakeController(t)
		started, queued, resumed := make(chan struct{}), make(chan struct{}), make(chan struct{})
		a := &finalizationHandle{fakeJob: first, key: "a"}
		b := &finalizationHandle{fakeJob: second, key: "b", gate: started}
		environment := &runSpace{Space: &fakeSpace{}, run: func(ctx context.Context, job provider.Job) (provider.Run, error) {
			if job.ID() == "a" {
				close(started)
				return first.local, nil
			}
			close(queued)
			select {
			case <-first.local.ready:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			close(resumed)
			return second.local, nil
		}}
		c := &Controller{
			spaces: map[string]*managedSpace{"build": {environment: environment}},
			forges: []managedForge{{id: "forge", space: "build", queue: &gatedQueue{[]*finalizationHandle{a, b}}, demand: newDemand(2)}},
		}
		done := make(chan error, 1)
		go func() { done <- c.Run(t.Context()) }()
		synctest.Wait()
		select {
		case <-queued:
		default:
			t.Fatal("second job not accepted and queued")
		}
		c.Drain()
		synctest.Wait()
		first.local.finish(provider.Succeeded)
		synctest.Wait()
		select {
		case <-resumed:
		default:
			t.Fatal("drain prevented an accepted job from acquiring capacity")
		}
		second.local.finish(provider.Succeeded)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if first.result != provider.Succeeded || second.result != provider.Succeeded || c.resources.Len() != 0 {
			t.Fatal("queued drain did not finish and release both jobs")
		}
	})
}
