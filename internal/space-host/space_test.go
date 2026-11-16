// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package spacehost

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"

	"dimidiumlabs.io/hrc/internal/provider"
	ipc "dimidiumlabs.io/hrc/internal/scheduler-ipc"
	"dimidiumlabs.io/hrc/internal/space"
)

// The child implements only the scheduler wire contract, with no slots or provider.
func TestMain(m *testing.M) {
	if len(os.Args) == 4 && os.Args[1] == "space-test-peer" {
		conn, err := net.Dial("unix", os.Args[3])
		if err != nil {
			os.Exit(10)
		}
		wire := ipc.NewConn(conn)
		defer wire.Close()
		initial, err := wire.Receive()
		if err != nil || initial.Type != ipc.Init {
			os.Exit(11)
		}
		var config ipc.Config
		if json.Unmarshal(initial.Payload, &config) != nil {
			os.Exit(12)
		}
		if err := wire.Send(context.Background(), ipc.Message{Type: ipc.Ready, Version: ipc.Version, Capacity: config.Capacity}); err != nil {
			os.Exit(13)
		}
		for {
			message, err := wire.Receive()
			if errors.Is(err, io.EOF) {
				os.Exit(0)
			}
			if err != nil {
				os.Exit(14)
			}
			response := ipc.Message{ID: message.ID}
			switch message.Type {
			case ipc.Submit:
				if message.Provider == "disconnect" {
					os.Exit(0)
				}
				response.Type, response.Result, response.Data = ipc.Finished, provider.Succeeded, message.Payload
			case ipc.Stop:
				os.Exit(0)
			case ipc.Inspect:
				response.Type, response.State = ipc.Snapshot, space.State{Capacity: uint64(config.Capacity)}
			default:
				os.Exit(15)
			}
			if wire.Send(context.Background(), response) != nil {
				os.Exit(16)
			}
		}
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
	data, _ := json.Marshal(ipc.Config{Capacity: 2})
	s, err := New("test-space", filepath.Join(t.TempDir(), "space"), []string{self, "space-test-peer"}, data)
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
