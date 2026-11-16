// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dimidiumlabs.io/hrc/internal/provider"
	"dimidiumlabs.io/hrc/internal/slot"
	"dimidiumlabs.io/hrc/internal/space"
)

type testRequest struct{ Mode string }
type testJob struct {
	payload []byte
	request testRequest
}

func (*testJob) Type() string               { return "test" }
func (*testJob) ID() string                 { return "test-job" }
func (j *testJob) Marshal() ([]byte, error) { return j.payload, nil }
func (j *testJob) Unmarshal(data []byte) error {
	if err := json.Unmarshal(data, &j.request); err != nil {
		return err
	}
	j.payload = bytes.Clone(data)
	return nil
}

// Runs and slots are test doubles: scheduler tests need no OS processes or paths.
type testRun struct {
	done   chan error
	ready  chan struct{}
	once   sync.Once
	result provider.Result
	data   []byte
}

func (r *testRun) finish(result provider.Result) {
	r.once.Do(func() { r.result = result; close(r.ready); r.done <- nil; close(r.done) })
}
func (r *testRun) Done() <-chan error { return r.done }
func (r *testRun) Result() (provider.Result, []byte) {
	select {
	case <-r.ready:
		return r.result, r.data
	default:
		return provider.Unknown, nil
	}
}
func (r *testRun) Stop(context.Context, provider.StopReason) error {
	r.finish(provider.Canceled)
	return nil
}

type executionSlot struct {
	slot.Slot
	harness *schedulerHarness
	name    string
	active  atomic.Bool
	closed  atomic.Bool
}

func (*executionSlot) Type() string   { return "test" }
func (s *executionSlot) Name() string { return s.name }
func (*executionSlot) Cwd() string    { return "" }
func (s *executionSlot) Status() slot.Status {
	if s.closed.Load() {
		return slot.Destroyed
	}
	if s.active.Load() {
		return slot.Running
	}
	return slot.Idle
}
func (s *executionSlot) Run(_ context.Context, job provider.Job) (provider.Run, error) {
	s.active.Store(true)
	j := job.(*testJob)
	run := &testRun{done: make(chan error, 1), ready: make(chan struct{}), data: j.payload}
	s.harness.started <- s.name
	if j.request.Mode != "wait" {
		run.finish(provider.Succeeded)
	}
	return run, nil
}
func (s *executionSlot) Stop(ctx context.Context, run provider.Run) error {
	err := run.Stop(ctx, provider.StopForce)
	s.active.Store(false)
	return err
}
func (s *executionSlot) Destroy(context.Context) error {
	if s.closed.CompareAndSwap(false, true) {
		s.harness.destroyed.Add(1)
	}
	return nil
}

type schedulerHarness struct {
	client             *space.Client
	done               chan error
	created, destroyed atomic.Int32
	started            chan string
	once               sync.Once
}

func (h *schedulerHarness) close() {
	h.once.Do(func() { h.client.Close(); <-h.done })
}
func newSchedulerHarness(t *testing.T, reuse bool) *schedulerHarness {
	t.Helper()
	h := &schedulerHarness{done: make(chan error, 1), started: make(chan string, 10)}
	server, client := net.Pipe()
	config := &space.Config{Capacity: 1, Ephemeral: !reuse}
	go func() {
		h.done <- serve(t.Context(), server, func(config *space.Config) (*scheduler, error) {
			s, err := newScheduler(int(config.Capacity), !config.Ephemeral, func(_ context.Context, name string) (slot.Slot, error) {
				h.created.Add(1)
				return &executionSlot{harness: h, name: name}, nil
			})
			if err != nil {
				return nil, err
			}
			s.decode = func(name string, data []byte) (provider.Job, error) {
				if name != "test" {
					return nil, slot.ErrProviderUnavailable
				}
				job := &testJob{}
				return job, job.Unmarshal(data)
			}
			return s, nil
		})
	}()
	var err error
	h.client, err = space.NewClient(t.Context(), client, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.close)
	return h
}
func (h *schedulerHarness) start(t *testing.T, payload []byte) provider.Run {
	t.Helper()
	run, err := h.client.Start(t.Context(), "test", payload)
	if err != nil {
		t.Fatal(err)
	}
	return run
}
func (h *schedulerHarness) awaitState(t *testing.T, want space.State) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		got, err := h.client.State(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("state=%+v, want %+v", got, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestSchedulerIPCStateAndQueuedCancellation(t *testing.T) {
	h := newSchedulerHarness(t, true)
	h.awaitState(t, space.State{Capacity: 1})
	first := h.start(t, []byte(`{"Mode":"wait"}`))
	<-h.started
	second := h.start(t, []byte(`{"Mode":"wait"}`))
	h.awaitState(t, space.State{Capacity: 1, Slots: 1, BusySlots: 1, Running: 1, Queued: 1})
	if err := second.Stop(t.Context(), provider.StopCancel); err != nil {
		t.Fatal(err)
	}
	if result, _, err := waitRun(second); err != nil || result != provider.Canceled {
		t.Fatalf("queued cancellation: %v %v", result, err)
	}
	if h.created.Load() != 1 {
		t.Fatal("queued job created a slot")
	}
	if err := first.Stop(t.Context(), provider.StopCancel); err != nil {
		t.Fatal(err)
	}
	if result, _, err := waitRun(first); err != nil || result != provider.Canceled {
		t.Fatalf("active cancellation: %v %v", result, err)
	}
	h.awaitState(t, space.State{Capacity: 1, Slots: 1})
	h.close()
	if h.destroyed.Load() != 1 {
		t.Fatal("slot survived disconnect")
	}
}

func TestSchedulerIPCPayloadAndReuse(t *testing.T) {
	for _, reuse := range []bool{false, true} {
		h := newSchedulerHarness(t, reuse)
		payload := []byte(`{"Mode":"success", "future":"Привет 🌍"}`)
		var previous string
		for i := 0; i < 2; i++ {
			run := h.start(t, payload)
			if result, data, err := waitRun(run); err != nil || result != provider.Succeeded || !bytes.Equal(data, payload) {
				t.Fatalf("outcome lost: %v %s %v", result, data, err)
			}
			name := <-h.started
			if i > 0 && (previous == name) != reuse {
				t.Fatal("incorrect reuse")
			}
			previous = name
		}
		want := int32(2)
		if reuse {
			want = 1
		}
		if h.created.Load() != want {
			t.Fatal("wrong slot count")
		}
		h.close()
		if h.destroyed.Load() != want {
			t.Fatal("slots survived disconnect")
		}
	}
}

func TestSchedulerIPCRejectsJobsBeforeCreatingSlots(t *testing.T) {
	h := newSchedulerHarness(t, false)
	run, err := h.client.Start(t.Context(), "missing-provider", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := waitRun(run); !errors.Is(err, slot.ErrProviderUnavailable) {
		t.Fatalf("typed error lost: %v", err)
	}
	run = h.start(t, []byte("{"))
	if result, _, err := waitRun(run); result != provider.Unknown || err == nil {
		t.Fatal("invalid job accepted")
	}
	if h.created.Load() != 0 {
		t.Fatal("invalid job created a slot")
	}
	run = h.start(t, []byte(`{"Mode":"success"}`))
	if result, _, err := waitRun(run); result != provider.Succeeded || err != nil {
		t.Fatalf("valid job after rejection: %v %v", result, err)
	}
}

func TestSchedulerIPCDisconnectStopsExecution(t *testing.T) {
	h := newSchedulerHarness(t, false)
	run := h.start(t, []byte(`{"Mode":"wait"}`))
	<-h.started
	h.close()
	if _, _, err := waitRun(run); err == nil {
		t.Fatal("disconnected run reported success")
	}
	if h.destroyed.Load() != 1 {
		t.Fatal("active slot survived disconnect")
	}
}

type cancelRaceRun struct {
	done  chan error
	ready atomic.Bool
	once  sync.Once
}

func (r *cancelRaceRun) Done() <-chan error { return r.done }
func (r *cancelRaceRun) Result() (provider.Result, []byte) {
	if r.ready.Load() {
		return provider.Succeeded, []byte{1}
	}
	return provider.Unknown, nil
}
func (r *cancelRaceRun) Stop(_ context.Context, reason provider.StopReason) error {
	r.once.Do(func() { r.ready.Store(true); r.done <- nil; close(r.done) })
	if reason != provider.StopForce {
		return os.ErrProcessDone
	}
	return nil
}

func TestCancellationRacingWithCompletionPreservesOutcome(t *testing.T) {
	created := make(chan struct{})
	s, err := newScheduler(1, true, func(context.Context, string) (slot.Slot, error) {
		return &raceSlot{created: created}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	e := &execution{ctx: ctx, lifetime: t.Context(), cancel: cancel, changed: make(chan struct{}, 1)}
	done := make(chan struct{})
	var result provider.Result
	var data []byte
	var failure error
	go func() {
		result, data, failure = s.execute(e, &testJob{})
		close(done)
	}()
	<-created
	e.request(provider.StopCancel)
	<-done
	if failure != nil || result != provider.Succeeded || !bytes.Equal(data, []byte{1}) {
		t.Fatalf("completion lost: %v, %v", result, failure)
	}
}

type raceSlot struct {
	slot.Slot
	created chan struct{}
}

func (s *raceSlot) Run(context.Context, provider.Job) (provider.Run, error) {
	close(s.created)
	return &cancelRaceRun{done: make(chan error, 1)}, nil
}
func (*raceSlot) Stop(ctx context.Context, run provider.Run) error {
	return run.Stop(ctx, provider.StopForce)
}
func (*raceSlot) Destroy(context.Context) error { return nil }

type testSlot struct {
	slot.Slot
	closed atomic.Int32
}

func (s *testSlot) Destroy(context.Context) error { s.closed.Add(1); return nil }

func TestCapacityAndReuse(t *testing.T) {
	for _, reuse := range []bool{false, true} {
		var created atomic.Int32
		s, err := newScheduler(1, reuse, func(context.Context, string) (slot.Slot, error) {
			created.Add(1)
			return &testSlot{}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		first, err := s.acquire(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		ready := make(chan *allocation, 1)
		failures := make(chan error, 1)
		go func() {
			a, err := s.acquire(t.Context())
			if err != nil {
				failures <- err
			} else {
				ready <- a
			}
		}()
		select {
		case <-ready:
			t.Fatal("two Runs acquired one place")
		case <-time.After(10 * time.Millisecond):
		}
		if err := s.release(first, true); err != nil {
			t.Fatal(err)
		}
		var second *allocation
		select {
		case second = <-ready:
		case err := <-failures:
			t.Fatal(err)
		}
		if (first == second) != reuse {
			t.Fatal("wrong reuse policy")
		}
		if !reuse && first.Slot.(*testSlot).closed.Load() != 1 {
			t.Fatal("ephemeral slot survived release")
		}
		if err := s.release(second, false); err != nil {
			t.Fatal(err)
		}
		if second.Slot.(*testSlot).closed.Load() != 1 {
			t.Fatal("unusable slot was retained")
		}
		third, err := s.acquire(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		s.close()
		if third.Slot.(*testSlot).closed.Load() != 1 {
			t.Fatal("active slot survived space close")
		}
		if _, err := s.acquire(t.Context()); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("acquire after close: %v", err)
		}
	}
}

func TestPreparationCancellationAndFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	instance := &testSlot{}
	attempts := 0
	s, err := newScheduler(1, true, func(context.Context, string) (slot.Slot, error) {
		attempts++
		if attempts == 1 {
			cancel()
			return instance, nil
		}
		return nil, errors.New("cannot prepare slot")
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	if _, err := s.acquire(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	if instance.closed.Load() != 1 {
		t.Fatal("canceled preparation leaked a slot")
	}
	if _, err := s.acquire(t.Context()); err == nil {
		t.Fatal("preparation failure lost")
	}
	if s.used != 0 {
		t.Fatal("preparation leaked capacity")
	}
}

func waitRun(run provider.Run) (provider.Result, []byte, error) {
	err := <-run.Done()
	result, data := run.Result()
	return result, data, err
}
