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

	"dimidiumlabs.io/hrc/internal/provider"
	"dimidiumlabs.io/hrc/internal/space"
)

// The child implements only the scheduler wire contract, with no slots or provider.
func TestMain(m *testing.M) {
	if len(os.Args) == 4 && os.Args[1] == "space-test-peer" {
		conn, err := net.Dial("unix", os.Args[3])
		if err != nil {
			os.Exit(10)
		}

		var capacity uint64
		server := &space.Server{
			Configure: func(config *space.Config) error { capacity = config.Capacity; return nil },
			State:     func() space.State { return space.State{Capacity: capacity} },
			Execute: func(_ context.Context, name string, payload []byte, _ <-chan provider.StopReason) (provider.Result, []byte, error) {
				if name == "disconnect" {
					os.Exit(0)
				}
				return provider.Succeeded, payload, nil
			},
		}
		if err := server.Serve(context.Background(), conn); err != nil {
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

func testSpace(t *testing.T) *Space {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	// Exercise sibling lookup with the same minimal PATH as a service.
	t.Setenv("PATH", "")
	config := &space.Config{Capacity: 2}
	s, err := New("test-space", filepath.Join(t.TempDir(), "space"), []string{filepath.Base(self), "space-test-peer"}, config)
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
	if err := s.Destroy(); err != nil {
		t.Fatal(err)
	}
	if state, err := s.State(t.Context()); err != nil || state != (space.State{}) || s.Status() != space.Destroyed {
		t.Fatalf("destroyed state: %+v %v", state, err)
	}
	if _, err := s.Run(t.Context(), &testJob{}); !errors.Is(err, space.ErrClosed) {
		t.Fatalf("Run after Destroy: %v", err)
	}
}

func TestDisconnectDoesNotReplayJob(t *testing.T) {
	s := testSpace(t)
	run, err := s.Run(t.Context(), &testJob{kind: "disconnect"})
	if err != nil {
		t.Fatal(err)
	}
	if err := <-run.Done(); err == nil {
		t.Fatal("disconnect reported success")
	}
	if s.Status() != space.Failed {
		t.Fatal("failed scheduler not reflected in status")
	}
	if _, err := s.Run(t.Context(), &testJob{kind: "test"}); err == nil {
		t.Fatal("space restarted after losing a job")
	}
}
