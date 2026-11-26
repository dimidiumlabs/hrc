// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package resource

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"testing/synctest"
)

type resource struct {
	quiesce  func(context.Context) error
	shutdown func(context.Context, Level) error
}

func (r *resource) Quiesce(ctx context.Context) error {
	if r.quiesce != nil {
		return r.quiesce(ctx)
	}
	return nil
}

func (r *resource) Shutdown(ctx context.Context, level Level) error {
	if r.shutdown != nil {
		return r.shutdown(ctx, level)
	}
	return nil
}

func own(t *testing.T, g *Heap, r Resource, dependencies ...*Ref) *Ref {
	t.Helper()
	ref, err := g.Add(r, dependencies...)
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func TestDependencyOrder(t *testing.T) {
	var g Heap
	var events []string
	makeResource := func(name string) Resource {
		return &resource{
			quiesce: func(context.Context) error {
				events = append(events, "quiesce "+name)
				return nil
			},
			shutdown: func(context.Context, Level) error {
				// Callbacks do not hold the collection lock.
				if g.Len() == 0 {
					t.Error("resource lost before successful shutdown")
				}
				events = append(events, "shutdown "+name)
				return nil
			},
		}
	}
	queue := own(t, &g, makeResource("queue"))
	space := own(t, &g, makeResource("space"))
	own(t, &g, makeResource("job-a"), queue, space)
	own(t, &g, makeResource("job-b"), queue, space)
	for range 2 {
		if err := g.Quiesce(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if g.Len() != 4 {
		t.Fatal("quiesce released resources")
	}
	for range 2 {
		if err := g.Shutdown(t.Context(), Down); err != nil {
			t.Fatal(err)
		}
	}
	want := []string{
		"quiesce queue", "quiesce space", "quiesce job-a", "quiesce job-b",
		"shutdown job-b", "shutdown job-a", "shutdown space", "shutdown queue",
	}
	if !reflect.DeepEqual(events, want) || g.Len() != 0 {
		t.Fatalf("events=%v, remaining=%d", events, g.Len())
	}
}

func TestFailureRetainsDependenciesButNotIndependentBranches(t *testing.T) {
	var g Heap
	failure := errors.New("cannot stop worker")
	var events []string
	makeResource := func(name string) *resource {
		return &resource{shutdown: func(context.Context, Level) error {
			events = append(events, name)
			return nil
		}}
	}
	queue := own(t, &g, makeResource("queue"))
	broken := own(t, &g, makeResource("broken-space"))
	healthy := own(t, &g, makeResource("healthy-space"))
	job := makeResource("broken-job")
	job.shutdown = func(context.Context, Level) error {
		events = append(events, "broken-job")
		return failure
	}
	own(t, &g, job, queue, broken)
	own(t, &g, makeResource("healthy-job"), queue, healthy)
	if err := g.Shutdown(t.Context(), Term); !errors.Is(err, failure) {
		t.Fatalf("lost cleanup error: %v", err)
	}
	if g.Len() != 3 || !reflect.DeepEqual(events, []string{"healthy-job", "broken-job", "healthy-space"}) {
		t.Fatalf("events=%v, remaining=%d", events, g.Len())
	}
	job.shutdown = func(context.Context, Level) error {
		events = append(events, "retried-job")
		return nil
	}
	if err := g.Shutdown(t.Context(), Kill); err != nil {
		t.Fatal(err)
	}
	if g.Len() != 0 || !reflect.DeepEqual(events[3:], []string{"retried-job", "broken-space", "queue"}) {
		t.Fatalf("events=%v, remaining=%d", events, g.Len())
	}
}

func TestMonotonicLevelAndNoAutomaticEscalation(t *testing.T) {
	var g Heap
	var levels []Level
	failure := errors.New("not yet stopped")
	own(t, &g, &resource{shutdown: func(_ context.Context, level Level) error {
		levels = append(levels, level)
		return failure
	}})
	for _, level := range []Level{Down, Down, Term, Kill, Down} {
		if err := g.Shutdown(t.Context(), level); !errors.Is(err, failure) {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(levels, []Level{Down, Down, Term, Kill, Kill}) {
		t.Fatal(levels)
	}
}

func TestQuiesceFailureIsRetryableAndIndependentOfShutdown(t *testing.T) {
	var g Heap
	failure := errors.New("admission not stopped")
	attempts, stopped := 0, 0
	own(t, &g, &resource{
		quiesce: func(context.Context) error {
			attempts++
			if attempts == 1 {
				return failure
			}
			return nil
		},
		shutdown: func(context.Context, Level) error { stopped++; return nil },
	})
	if err := g.Quiesce(t.Context()); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if stopped != 0 || g.Len() != 1 {
		t.Fatal("quiesce failure released resource")
	}
	if err := g.Quiesce(t.Context()); err != nil || attempts != 2 {
		t.Fatalf("retry: attempts=%d error=%v", attempts, err)
	}
	if err := g.Shutdown(t.Context(), Down); err != nil || stopped != 1 || attempts != 2 {
		t.Fatalf("shutdown: attempts=%d stopped=%d error=%v", attempts, stopped, err)
	}

	var direct Heap
	own(t, &direct, &resource{quiesce: func(context.Context) error {
		t.Fatal("Shutdown must not implicitly call Quiesce")
		return nil
	}})
	if err := direct.Shutdown(t.Context(), Term); err != nil {
		t.Fatal(err)
	}
}

func TestAcceptedWorkCanRegisterDuringQuiesce(t *testing.T) {
	var g Heap
	var parent, child *Ref
	quiesced := 0
	parent = own(t, &g, &resource{quiesce: func(context.Context) error {
		child = own(t, &g, &resource{quiesce: func(context.Context) error {
			quiesced++
			return nil
		}}, parent)
		return nil
	}})
	if err := g.Quiesce(t.Context()); err != nil {
		t.Fatal(err)
	}
	if child == nil || quiesced != 0 || g.Len() != 2 {
		t.Fatal("quiesce did not preserve registration and snapshot semantics")
	}
	if err := g.Quiesce(t.Context()); err != nil || quiesced != 1 {
		t.Fatalf("new registration not quiesced on next pass: %v", err)
	}
	if err := g.Shutdown(t.Context(), Down); err != nil || g.Len() != 0 {
		t.Fatalf("new dependency not released: %v", err)
	}
}

func TestIndependentErrorsAreJoined(t *testing.T) {
	var g Heap
	first, second := errors.New("first failure"), errors.New("second failure")
	dependency := own(t, &g, &resource{})
	for _, failure := range []error{first, second} {
		own(t, &g, &resource{shutdown: func(context.Context, Level) error {
			return failure
		}}, dependency)
	}
	err := g.Shutdown(t.Context(), Down)
	if !errors.Is(err, first) || !errors.Is(err, second) || g.Len() != 3 {
		t.Fatalf("remaining=%d errors=%v", g.Len(), err)
	}
}

func TestCancellationDuringShutdownRetainsDependencies(t *testing.T) {
	var g Heap
	dependency := own(t, &g, &resource{})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	first := true
	own(t, &g, &resource{shutdown: func(ctx context.Context, _ Level) error {
		if first {
			first = false
			cancel()
			return ctx.Err()
		}
		return nil
	}}, dependency)
	if err := g.Shutdown(ctx, Term); !errors.Is(err, context.Canceled) || g.Len() != 2 {
		t.Fatalf("remaining=%d error=%v", g.Len(), err)
	}
	if err := g.Shutdown(t.Context(), Kill); err != nil || g.Len() != 0 {
		t.Fatalf("retry: remaining=%d error=%v", g.Len(), err)
	}
}

func TestRegistrationValidationAndSeal(t *testing.T) {
	var g, foreign Heap
	if _, err := g.Add(nil); err == nil {
		t.Fatal("accepted nil resource")
	}
	dependency := own(t, &g, &resource{})
	other := own(t, &foreign, &resource{})
	copied := *dependency
	if err := g.Request(&copied, Down); err == nil {
		t.Fatal("accepted a copied, unregistered reference")
	}
	for _, dependencies := range [][]*Ref{{nil}, {other}, {dependency, dependency}, {new(Ref)}, {&copied}} {
		if _, err := g.Add(&resource{}, dependencies...); err == nil {
			t.Fatal("accepted invalid dependencies")
		}
	}
	if g.Len() != 1 {
		t.Fatal("invalid registration transferred ownership")
	}
	if err := g.Shutdown(t.Context(), Level(255)); err == nil {
		t.Fatal("accepted invalid shutdown level")
	}
	own(t, &g, &resource{}, dependency)
	if err := g.Quiesce(t.Context()); err != nil {
		t.Fatal(err)
	}
	own(t, &g, &resource{}, dependency)
	if err := g.Shutdown(t.Context(), Down); err != nil || g.Len() != 0 {
		t.Fatalf("invalid dependencies corrupted reference counts: %v", err)
	}
	if _, err := g.Add(&resource{}); err == nil {
		t.Fatal("accepted resource after shutdown")
	}
}

func TestCanceledOperationDoesNotTakeOwnershipOrReleaseDependencies(t *testing.T) {
	var g Heap
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := g.Quiesce(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	dependency := own(t, &g, &resource{})
	own(t, &g, &resource{shutdown: func(ctx context.Context, _ Level) error {
		return ctx.Err()
	}}, dependency)
	if err := g.Shutdown(ctx, Down); !errors.Is(err, context.Canceled) || g.Len() != 2 {
		t.Fatalf("canceled cleanup: remaining=%d error=%v", g.Len(), err)
	}
	if err := g.Shutdown(t.Context(), Down); err != nil || g.Len() != 0 {
		t.Fatalf("retry: remaining=%d error=%v", g.Len(), err)
	}
}

func TestConcurrentLifecycleAndCanceledWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var g Heap
		entered, release := make(chan struct{}), make(chan struct{})
		own(t, &g, &resource{quiesce: func(context.Context) error {
			close(entered)
			<-release
			return nil
		}})
		done := make(chan error, 1)
		go func() { done <- g.Quiesce(t.Context()) }()
		<-entered
		ctx, cancel := context.WithCancel(t.Context())
		waiting := make(chan error, 1)
		go func() { waiting <- g.Shutdown(ctx, Term) }()
		synctest.Wait() // Both lifecycle operations are blocked, not just unscheduled.
		cancel()
		if err := <-waiting; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if g.Len() != 1 {
			t.Fatal("waiting shutdown released resource")
		}
		// A canceled waiter must not seal registration before Shutdown starts.
		own(t, &g, &resource{})
		close(release)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				if err := g.Shutdown(t.Context(), Down); err != nil {
					t.Error(err)
				}
			})
		}
		wg.Wait()
		if g.Len() != 0 {
			t.Fatal("concurrent shutdown retained resources")
		}
	})
}
