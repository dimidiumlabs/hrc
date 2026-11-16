// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package slotpgroup

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"dimidiumlabs.io/hrc/internal/provider"
	"dimidiumlabs.io/hrc/internal/slot"
)

type controlledRun struct {
	done   chan error
	ready  atomic.Bool
	once   sync.Once
	closed atomic.Int32
	err    error
}

type testJob struct{}

func (*testJob) Type() string             { return "test" }
func (*testJob) ID() string               { return "test-job" }
func (*testJob) Marshal() ([]byte, error) { panic("slot must not serialize Job") }
func (*testJob) Unmarshal([]byte) error   { panic("slot must not decode Job") }

func (r *controlledRun) finish() {
	r.once.Do(func() { r.ready.Store(true); r.done <- r.err; close(r.done) })
}
func (r *controlledRun) Done() <-chan error { return r.done }
func (r *controlledRun) Result() (provider.Result, []byte) {
	if r.ready.Load() {
		return provider.Succeeded, nil
	}
	return provider.Unknown, nil
}
func (r *controlledRun) Stop(context.Context, provider.StopReason) error {
	r.closed.CompareAndSwap(0, 1)
	r.finish()
	return nil
}

func TestSlotOwnsSuccessiveRuns(t *testing.T) {
	var children []*controlledRun
	s, err := newSlot(filepath.Join(t.TempDir(), "slot"), func(context.Context, *Slot, provider.Job) (provider.Run, error) {
		r := &controlledRun{done: make(chan error, 1)}
		children = append(children, r)
		return r, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Destroy(context.Background())
	if s.Type() != "pgroup" || s.Name() != "slot" || s.Status() != slot.Idle {
		t.Fatal("incorrect initial diagnostics")
	}
	cache := filepath.Join(s.Cwd(), "cache")
	if err := os.WriteFile(cache, []byte("warm"), 0600); err != nil {
		t.Fatal(err)
	}
	first, err := s.Run(t.Context(), &testJob{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Run(t.Context(), &testJob{}); !errors.Is(err, slot.ErrBusy) {
		t.Fatalf("concurrent Run: %v", err)
	}
	children[0].finish()
	<-first.Done()
	if s.Status() != slot.Idle || children[0].closed.Load() != 0 {
		t.Fatal("completed Run was not cleaned up")
	}
	second, err := s.Run(t.Context(), &testJob{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Stop(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	select {
	case <-second.Done():
		t.Fatal("late Stop killed the next Run")
	default:
	}
	if s.Status() != slot.Running {
		t.Fatal("late Stop changed slot state")
	}
	if err := s.Stop(t.Context(), second); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(cache); err != nil || string(data) != "warm" {
		t.Fatal("Stop removed persistent workspace")
	}
	third, err := s.Run(t.Context(), &testJob{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Destroy(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-third.Done():
	default:
		t.Fatal("Destroy did not stop execution")
	}
	if s.Status() != slot.Destroyed {
		t.Fatal("destroyed environment still available")
	}
	if _, err := os.Stat(s.Cwd()); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("workspace survived Destroy")
	}
	if _, err := s.Run(t.Context(), &testJob{}); !errors.Is(err, slot.ErrDestroyed) {
		t.Fatalf("Run after Destroy: %v", err)
	}
	if err := s.Stop(t.Context(), &controlledRun{}); !errors.Is(err, slot.ErrUnknownRun) {
		t.Fatalf("foreign Run: %v", err)
	}
	for _, child := range children {
		if child.closed.Load() != 1 {
			t.Fatal("provider cleanup was not performed exactly once")
		}
	}
}

func TestDestroyInterruptsPreparation(t *testing.T) {
	entered := make(chan struct{})
	s, err := newSlot(filepath.Join(t.TempDir(), "slot"), func(ctx context.Context, _ *Slot, _ provider.Job) (provider.Run, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Destroy(context.Background())
	started := make(chan error, 1)
	go func() { _, err := s.Run(t.Context(), &testJob{}); started <- err }()
	<-entered
	if s.Status() != slot.Starting {
		t.Fatal("preparation status is not observable")
	}
	if err := s.Destroy(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := <-started; !errors.Is(err, context.Canceled) || !errors.Is(err, slot.ErrDestroyed) {
		t.Fatalf("preparation error: %v", err)
	}
	if _, err := os.Stat(s.Cwd()); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("prepared workspace survived Destroy")
	}
}

func TestStopPreservesDoneError(t *testing.T) {
	failure := errors.New("execution failed")
	s, err := newSlot(filepath.Join(t.TempDir(), "slot"), func(context.Context, *Slot, provider.Job) (provider.Run, error) {
		return &controlledRun{done: make(chan error, 1), err: failure}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Destroy(context.Background())
	run, err := s.Run(t.Context(), &testJob{})
	if err != nil {
		t.Fatal(err)
	}
	if result, data := run.Result(); result != provider.Unknown || data != nil {
		t.Fatal("unfinished Run has a result")
	}
	if err := s.Stop(t.Context(), run); err != nil {
		t.Fatal(err)
	}
	if err := <-run.Done(); !errors.Is(err, failure) {
		t.Fatalf("Stop consumed completion error: %v", err)
	}
	for range 2 {
		if result, _ := run.Result(); result != provider.Succeeded {
			t.Fatal("result was not retained")
		}
	}
	if _, open := <-run.Done(); open {
		t.Fatal("Done must close after one value")
	}
}

func TestUnsupportedWorker(t *testing.T) {
	target, err := New(filepath.Join(t.TempDir(), "slot"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Destroy(context.Background())
	if _, err := target.Run(t.Context(), &testJob{}); !errors.Is(err, slot.ErrProviderUnavailable) {
		t.Fatalf("unknown worker: %v", err)
	}
	if target.Status() != slot.Idle {
		t.Fatal("unsupported job made slot unavailable")
	}
}
