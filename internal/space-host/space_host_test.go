// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package spacehost

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"dimidiumlabs.io/hrc/internal/provider"
	"dimidiumlabs.io/hrc/internal/runtime"
	"dimidiumlabs.io/hrc/internal/space"
)

// The child implements only the scheduler wire contract, with no slots or provider.
func TestMain(m *testing.M) {
	if len(os.Args) >= 2 && os.Args[1] == "space-test-exit" {
		os.Exit(12)
	}
	if len(os.Args) == 4 && os.Args[1] == "space-test-stall" {
		if err := os.WriteFile("scheduler-started", []byte(os.Args[3]), 0600); err != nil {
			os.Exit(10)
		}
		time.Sleep(time.Hour) // Started, but never connects to the controller.
		os.Exit(0)
	}
	if len(os.Args) == 4 && os.Args[1] == "space-test-peer" {
		conn, err := net.Dial("unix", os.Args[3])
		if err != nil {
			os.Exit(10)
		}

		if err := serveTestPeer(conn); err != nil {
			os.Exit(11)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

type testJob struct {
	kind string
	data []byte
}

func (j *testJob) Type() string                { return j.kind }
func (*testJob) ID() string                    { return "job" }
func (j *testJob) Marshal() ([]byte, error)    { return j.data, nil }
func (j *testJob) Unmarshal(data []byte) error { j.data = bytes.Clone(data); return nil }

func testSpace(t *testing.T) *SpaceHost {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	// Exercise sibling lookup with the same minimal PATH as a service.
	t.Setenv("PATH", "")
	config := &space.Config{Capacity: 2}
	s, err := New("test-space", filepath.Join(t.TempDir(), "space"), []string{filepath.Base(self), "space-test-peer"}, config, &runtime.Pool{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Destroy(); err != nil {
			t.Error(err)
		}
	})
	return s
}

func TestStatusDoesNotWaitForLifecycleLock(t *testing.T) {
	s := &SpaceHost{}
	s.SetStatus(space.Ready, nil)
	s.mu.Lock()
	defer s.mu.Unlock()
	status := make(chan space.Status, 1)
	go func() { status <- s.Status() }()
	select {
	case got := <-status:
		if got != space.Ready {
			t.Fatalf("status: %v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("Status blocked on a lifecycle operation")
	}
}

func TestInspectionUsesExplicitStatus(t *testing.T) {
	failure := errors.New("stored lifecycle failure")
	s := &SpaceHost{}
	for _, status := range []space.Status{space.Idle, space.Connecting, space.Starting, space.Destroying, space.Broken} {
		s.SetStatus(status, failure)
		if got := s.Status(); got != status {
			t.Fatalf("Status inferred %v instead of %v", got, status)
		}
		state, err := s.State(t.Context())
		var want error
		if status == space.Broken {
			want = failure
		}
		if err != want || state != (space.State{}) {
			t.Fatalf("status=%v: state=%+v error=%v, want %v", status, state, err, want)
		}
		if s.Status() != status {
			t.Fatal("State changed lifecycle status")
		}
	}
}

func TestDestroyFailureIsPublished(t *testing.T) {
	child, err := runtime.StartChild(t.Context(), runtime.Command{Path: os.Args[0], Args: []string{"space-test-exit"}})
	if err != nil {
		t.Fatal(err)
	}
	<-child.Done()
	s := &SpaceHost{child: child}
	s.SetStatus(space.Starting, nil)
	err = s.Destroy()
	var exitErr *runtime.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 12 || s.Status() != space.Broken {
		t.Fatalf("destroy failure: status=%v error=%v", s.Status(), err)
	}
	if !childExited(child) || s.child != nil {
		t.Fatal("Destroy did not reap and release the failed process")
	}
	if state, stateErr := s.State(t.Context()); stateErr != err || state != (space.State{}) {
		t.Fatalf("stored destroy failure: %+v %v, want %v", state, stateErr, err)
	}
	if destroyErr := s.Destroy(); destroyErr != nil || s.Status() != space.Idle || s.Err() != nil {
		t.Fatalf("repeated Destroy did not recover: status=%v error=%v", s.Status(), destroyErr)
	}
}

func TestCleanupReapsUnconnectedProcess(t *testing.T) {
	child, err := runtime.StartChild(t.Context(), runtime.Command{Path: os.Args[0], Args: []string{"space-test-exit"}})
	if err != nil {
		t.Fatal(err)
	}
	<-child.Done()
	s := &SpaceHost{child: child}
	s.SetStatus(space.Connecting, nil)
	exitErr, cleanupErr := s.cleanup()
	if cleanupErr != nil {
		t.Fatalf("cleanup of exited process: %v", cleanupErr)
	}
	var processErr *runtime.ExitError
	if !errors.As(exitErr, &processErr) || !childExited(child) || s.child != nil {
		t.Fatalf("cleanup did not reap the process and return its outcome: %v", exitErr)
	}
}

func TestLazyStartupAndLifecycle(t *testing.T) {
	s := testSpace(t)
	if s.Type() != "host" || s.Name() != "test-space" || s.Status() != space.Idle {
		t.Fatal("incorrect initial identity or status")
	}
	if state, err := s.State(t.Context()); err != nil || state != (space.State{}) {
		t.Fatalf("idle state: %+v %v", state, err)
	}
	if _, err := os.Stat(s.Cwd()); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("inspection created workspace")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.Run(ctx, &testJob{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled startup: %v", err)
	}
	if s.Status() != space.Idle {
		t.Fatal("canceled preparation changed status")
	}
	payload := []byte(`{"opaque":"Привет 🌍"}`)
	run, err := s.Run(t.Context(), &testJob{kind: "test", data: payload})
	if err != nil {
		t.Fatal(err)
	}
	if err := <-run.Done(); err != nil {
		t.Fatal(err)
	}
	result, data := run.Result()
	if result != provider.Succeeded || !bytes.Equal(data, payload) {
		t.Fatal("opaque job/result lost")
	}
	if state, err := s.State(t.Context()); err != nil || state != (space.State{Capacity: 2}) || s.Status() != space.Ready {
		t.Fatalf("ready state: %+v %v", state, err)
	}
	process := s.child
	if _, err := s.Run(ctx, &testJob{kind: "test"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled submission to ready space: %v", err)
	}
	if s.Status() != space.Ready || s.child != process {
		t.Fatal("canceled submission changed a ready scheduler")
	}
	if err := s.Destroy(); err != nil {
		t.Fatal(err)
	}
	if state, err := s.State(t.Context()); err != nil || state != (space.State{}) || s.Status() != space.Idle || s.Err() != nil {
		t.Fatalf("idle state after Destroy: %+v %v", state, err)
	}
	if err := s.Destroy(); err != nil || s.Status() != space.Idle {
		t.Fatalf("repeated Destroy: status=%v error=%v", s.Status(), err)
	}
	next, err := s.Run(t.Context(), &testJob{kind: "test", data: payload})
	if err != nil {
		t.Fatalf("Run after Destroy: %v", err)
	}
	if err := <-next.Done(); err != nil {
		t.Fatal(err)
	}
	if result, data := next.Result(); result != provider.Succeeded || !bytes.Equal(data, payload) || s.Status() != space.Ready {
		t.Fatal("space did not restart successfully after Destroy")
	}
}

func TestCleanupMakesHostRestartable(t *testing.T) {
	s := testSpace(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	run, err := s.Run(ctx, &testJob{kind: "wait"})
	if err != nil {
		t.Fatal(err)
	}
	process, socket := s.child, spaceSocket(s)
	s.mu.Lock()
	s.SetStatus(space.Destroying, nil)
	exitErr, err := s.cleanup()
	s.mu.Unlock()
	if exitErr != nil || err != nil {
		t.Fatalf("cleanup: exit=%v error=%v", exitErr, err)
	}
	if !childExited(process) || s.child != nil || spaceSocket(s) != "" {
		t.Fatal("cleanup returned before reaping and releasing process resources")
	}
	if _, err := os.Stat(socket); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cleanup retained scheduler socket: %v", err)
	}
	if err := <-run.Done(); !errors.Is(err, space.ErrUnavailable) {
		t.Fatalf("old Run: %v", err)
	}
	// Cleanup is idempotent; a caller can then start a fresh generation.
	s.mu.Lock()
	_, err = s.cleanup()
	s.SetStatus(space.Idle, nil)
	s.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	next, err := s.Run(ctx, &testJob{kind: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if err := <-next.Done(); err != nil {
		t.Fatal(err)
	}
	if s.Status() != space.Ready {
		t.Fatal("host did not restart after cleanup")
	}
}

func TestCanceledStartupCleansUpBeforeIdle(t *testing.T) {
	s := testSpace(t)
	s.args[1] = "space-test-stall"
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	completed := make(chan error, 1)
	go func() {
		_, err := s.Run(ctx, &testJob{kind: "test"})
		completed <- err
	}()
	var socket []byte
	for {
		var err error
		socket, err = os.ReadFile(filepath.Join(s.Cwd(), "scheduler-started"))
		if err == nil && len(socket) != 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("scheduler did not start")
		case <-time.After(time.Millisecond):
		}
	}
	if s.Status() != space.Connecting {
		t.Fatal("unconnected scheduler did not report Connecting")
	}
	cancel()
	if err := <-completed; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled startup: %v", err)
	}
	if s.Status() != space.Idle || s.Err() != nil {
		t.Fatalf("rollback status: %v %v", s.Status(), s.Err())
	}
	if s.child != nil || spaceSocket(s) != "" {
		t.Fatal("Idle published with partial startup resources retained")
	}
	if _, err := os.Stat(filepath.Dir(string(socket))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Idle published before socket cleanup: %v", err)
	}
	// Rollback must leave the space usable by a subsequent job.
	s.args[1] = "space-test-peer"
	run, err := s.Run(t.Context(), &testJob{kind: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if err := <-run.Done(); err != nil {
		t.Fatal(err)
	}
	if s.Status() != space.Ready {
		t.Fatal("replacement startup did not publish Ready")
	}
}

func TestProcessExitBeforeConnectionFailsPromptly(t *testing.T) {
	s := testSpace(t)
	s.args[1] = "space-test-exit"
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if _, err := s.Run(ctx, &testJob{kind: "test"}); !errors.Is(err, space.ErrUnavailable) {
		t.Fatalf("early child exit: %v", err)
	}
	if ctx.Err() != nil || s.Status() != space.Broken || s.child != nil {
		t.Fatalf("early exit waited for timeout or retained child: %v %v", s.Status(), s.Err())
	}
	s.args[1] = "space-test-peer"
	run, err := s.Run(ctx, &testJob{kind: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if err := <-run.Done(); err != nil {
		t.Fatal(err)
	}
}

func TestStartupFailureCanRecover(t *testing.T) {
	s := testSpace(t)
	command := s.args[0]
	s.args[0] = filepath.Join(t.TempDir(), "missing-hrcm")
	if _, err := s.Run(t.Context(), &testJob{kind: "test"}); !errors.Is(err, space.ErrUnavailable) {
		t.Fatalf("startup failure: %v", err)
	}
	if s.Status() != space.Broken {
		t.Fatal("startup failure not reflected in status")
	}
	if state, err := s.State(t.Context()); !errors.Is(err, space.ErrUnavailable) || state != (space.State{}) {
		t.Fatalf("stored startup failure: %+v %v", state, err)
	}
	// Cancellation also rolls back an earlier failed generation before Idle.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.Run(ctx, &testJob{kind: "test"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled recovery: %v", err)
	}
	if s.Status() != space.Idle || s.Err() != nil || s.child != nil || spaceSocket(s) != "" {
		t.Fatal("canceled recovery did not clean the failed generation before Idle")
	}
	s.args[0] = command
	run, err := s.Run(t.Context(), &testJob{kind: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if err := <-run.Done(); err != nil {
		t.Fatal(err)
	}
	if result, _ := run.Result(); result != provider.Succeeded {
		t.Fatalf("replacement Run: %v", result)
	}
}

func TestDisconnectDelegatesCleanup(t *testing.T) {
	s := testSpace(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	warm, err := s.Run(ctx, &testJob{kind: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if err := <-warm.Done(); err != nil {
		t.Fatal(err)
	}
	process, socket, generation := s.child, spaceSocket(s), s.Daemon.Done()
	lost, err := s.Run(ctx, &testJob{kind: "crash"})
	if err != nil {
		t.Fatal(err)
	}
	if err := <-lost.Done(); !errors.Is(err, space.ErrUnavailable) {
		t.Fatalf("lost Run: %v", err)
	}
	// No Run or Destroy call should be needed to reclaim the dead scheduler.
	for {
		s.mu.Lock()
		clean := s.child == nil && spaceSocket(s) == ""
		s.mu.Unlock()
		if clean {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("disconnect did not invoke host cleanup")
		case <-time.After(time.Millisecond):
		}
	}
	if !childExited(process) || s.Status() != space.Broken {
		t.Fatal("disconnect cleanup did not reap the scheduler or preserve Broken")
	}
	var exitErr *runtime.ExitError
	if !errors.As(s.Err(), &exitErr) || exitErr.ExitCode() != 12 {
		t.Fatalf("disconnect cleanup lost the process outcome: %v", s.Err())
	}
	if _, err := os.Stat(socket); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("disconnect retained socket: %v", err)
	}
	if err := s.Destroy(); err != nil || s.Status() != space.Idle || s.Err() != nil {
		t.Fatalf("Destroy after crash: status=%v error=%v", s.Status(), err)
	}
	next, err := s.Run(ctx, &testJob{kind: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if err := <-next.Done(); err != nil {
		t.Fatal(err)
	}
	replacement := s.child
	s.Disconnected(generation) // A delayed old notification must be harmless.
	if s.Status() != space.Ready || s.child != replacement {
		t.Fatal("stale disconnect cleaned up the replacement scheduler")
	}
}

func TestLostSchedulerFailsRunsAndRestarts(t *testing.T) {
	for _, kind := range []string{"disconnect", "crash", "transport"} {
		t.Run(kind, func(t *testing.T) {
			s := testSpace(t)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			waiting, err := s.Run(ctx, &testJob{kind: "wait"})
			if err != nil {
				t.Fatal(err)
			}
			for {
				state, err := s.State(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if state.Running == 1 {
					break
				}
				time.Sleep(time.Millisecond)
			}
			oldProcess, oldSocket := s.child, spaceSocket(s)
			jobKind := kind
			if kind == "transport" {
				jobKind = "wait"
			}
			run, err := s.Run(ctx, &testJob{kind: jobKind})
			if err != nil {
				t.Fatal(err)
			}
			if kind == "transport" {
				s.Daemon.Close()
			}
			for _, lost := range []provider.Run{run, waiting} {
				select {
				case err := <-lost.Done():
					if !errors.Is(err, space.ErrUnavailable) {
						t.Fatalf("lost Run: %v", err)
					}
				case <-ctx.Done():
					t.Fatal("lost Run did not finish")
				}
			}
			// Loss must update the published status without an inspection
			// call discovering the failure as a side effect.
			for s.Status() != space.Broken {
				select {
				case <-ctx.Done():
					t.Fatal("failed scheduler not reflected in status")
				case <-time.After(time.Millisecond):
				}
			}
			if state, err := s.State(ctx); !errors.Is(err, space.ErrUnavailable) || state != (space.State{}) {
				t.Fatalf("stored connection failure: %+v %v", state, err)
			}
			payload := []byte("new job")
			next, err := s.Run(ctx, &testJob{kind: "test", data: payload})
			if err != nil {
				t.Fatal(err)
			}
			if err := <-next.Done(); err != nil {
				t.Fatal(err)
			}
			result, data := next.Result()
			if result != provider.Succeeded || !bytes.Equal(data, payload) {
				t.Fatalf("replacement Run: %v %q", result, data)
			}
			if s.child == oldProcess {
				t.Fatal("scheduler generation was not replaced")
			}
			if _, err := os.Stat(oldSocket); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("old scheduler socket leaked: %v", err)
			}
			if state, err := s.State(ctx); err != nil || state.Running != 0 || s.Status() != space.Ready {
				t.Fatalf("replacement state (lost job must not be replayed): %+v %v", state, err)
			}
		})
	}
}

func spaceSocket(s *SpaceHost) string {
	if s.child == nil {
		return ""
	}
	return s.child.SocketPath()
}
func childExited(c *runtime.Child) bool {
	select {
	case <-c.Done():
		return true
	default:
		return false
	}
}
