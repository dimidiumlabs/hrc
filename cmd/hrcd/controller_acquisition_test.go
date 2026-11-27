// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"context"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"dimidiumlabs.io/hrc/internal/provider"
)

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
	task.finalization.result = provider.Succeeded
	task.executionFinished()
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
			pending.finalization.result = provider.Succeeded
			pending.executionFinished()
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
	task.finalization.result = provider.Canceled
	task.executionFinished()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-queue.sessions:
		t.Fatal("queue reopened during daemon shutdown")
	default:
	}
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
				task.finalization.result = provider.Succeeded
				task.executionFinished()
			}
			cancel()
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
			if transfer {
				if !slices.Equal(*calls, []string{"finish", "job-close"}) || handle.result != provider.Succeeded {
					t.Fatalf("completed handle was not finalized: %v", *calls)
				}
			} else {
				if !slices.Equal(*calls, []string{"finish", "job-close"}) || handle.result != provider.Canceled {
					t.Fatalf("undelivered handle leaked: %v, %v", *calls, handle.result)
				}
			}
		})
	}
}
