// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"context"
	"encoding/json/v2"
	"errors"
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

type closingSpace struct {
	space.Space
	destroy func() error
}

func (s *closingSpace) Destroy() error { return s.destroy() }

func TestControllerShutdownThenSpacevisorClose(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "cancellation"
		if fail {
			name = "controller-error"
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
			v := &SpaceVisor{
				spaces: map[string]*managedSpace{"shared": shared, "spare": spare},
				failed: make(chan struct{}),
			}
			c := &Controller{forges: []managedForge{{id: "waiting", queue: waiting, space: "shared", demand: capacity}}}
			if fail {
				environment.prepare = func(context.Context) error { <-waiting.ready; return executionErr }
				c.forges = append(c.forges, managedForge{id: "failing", queue: &fakeQueue{job}, space: "shared", demand: capacity})
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
				v.Submit(ctx, task)
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
				if !errors.Is(err, executionErr) || !errors.Is(err, cleanupErr) ||
					!strings.Contains(err.Error(), "failing") || !strings.Contains(err.Error(), "shared") {
					t.Fatalf("lost controller or cleanup error: %v", err)
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
	v := &SpaceVisor{spaces: map[string]*managedSpace{"target": host}, failed: make(chan struct{})}
	c := &Controller{forges: []managedForge{{id: "forge", space: "target", queue: &fakeQueue{handle}, demand: capacity}}}
	for task := range c.Serve(t.Context()) {
		v.Submit(t.Context(), task)
	}
	<-started
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
			v := &SpaceVisor{
				spaces: map[string]*managedSpace{"target": {environment: environment, demand: capacity}},
				failed: make(chan struct{}),
			}
			c := &Controller{forges: []managedForge{{id: "forge", space: "target", queue: &fakeQueue{handle}, demand: capacity}}}
			for task := range c.Serve(acquisition) {
				v.Submit(acquisition, task)
			}
			<-started
			stopAcquisition()
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
