// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	runnerv1 "code.forgejo.org/forgejo/actions-proto/runner/v1"
	"code.forgejo.org/forgejo/actions-proto/runner/v1/runnerv1connect"
	"code.forgejo.org/forgejo/runner/v13/hrcbridge"
	"connectrpc.com/connect"
	"dimidiumlabs.io/hrc/internal/config"
	"dimidiumlabs.io/hrc/internal/provider"
	"google.golang.org/protobuf/proto"
)

type reservation struct{ closed int }

func (*reservation) Wait(context.Context) error { return nil }
func (r *reservation) Close() error             { r.closed++; return nil }

type finalizer struct {
	runnerv1connect.UnimplementedRunnerServiceHandler
	mu    sync.Mutex
	calls int
	fail  bool
	state *runnerv1.TaskState
}

func (f *finalizer) UpdateTask(_ context.Context, req *connect.Request[runnerv1.UpdateTaskRequest]) (*connect.Response[runnerv1.UpdateTaskResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.fail {
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("offline"))
	}
	f.state = proto.Clone(req.Msg.State).(*runnerv1.TaskState)
	return connect.NewResponse(&runnerv1.UpdateTaskResponse{State: req.Msg.State}), nil
}

func TestFinishAndReservationOwnership(t *testing.T) {
	for _, tc := range []struct {
		name       string
		data       []byte
		result     provider.Result
		wantCalls  int
		wantResult runnerv1.Result
	}{
		{"already reported", []byte(bridge.Reported), provider.Succeeded, 0, 0},
		{"unstarted", nil, provider.Failed, 1, runnerv1.Result_RESULT_FAILURE},
		{"cancelled", nil, provider.Canceled, 1, runnerv1.Result_RESULT_CANCELLED},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &finalizer{}
			path, handler := runnerv1connect.NewRunnerServiceHandler(f)
			mux := http.NewServeMux()
			mux.Handle("/api/actions"+path, http.StripPrefix("/api/actions", handler))
			server := httptest.NewServer(mux)
			defer server.Close()
			r := &reservation{}
			task, err := proto.Marshal(&runnerv1.Task{Id: 23})
			if err != nil {
				t.Fatal(err)
			}
			h := &handle{connection: bridge.Connection{URL: server.URL}, task: task, reservation: r, job: &provider.OpaqueJob{Key: "23"}}
			for range 2 {
				if err := h.Finish(context.Background(), tc.result, tc.data); err != nil {
					t.Fatal(err)
				}
			}
			if r.closed != 0 {
				t.Fatal("Finish released reservation before Close")
			}
			for range 2 {
				if err := h.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			if r.closed != 1 {
				t.Fatalf("reservation closed %d times", r.closed)
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.calls != tc.wantCalls {
				t.Fatalf("finalizations=%d want=%d", f.calls, tc.wantCalls)
			}
			if tc.wantCalls != 0 && (f.state.Id != 23 || f.state.Result != tc.wantResult || f.state.StoppedAt == nil) {
				t.Fatalf("incorrect fallback finalization: %v", f.state)
			}
		})
	}
}

func TestFailedFinalizationCanBeRetried(t *testing.T) {
	f := &finalizer{fail: true}
	path, handler := runnerv1connect.NewRunnerServiceHandler(f)
	mux := http.NewServeMux()
	mux.Handle("/api/actions"+path, http.StripPrefix("/api/actions", handler))
	server := httptest.NewServer(mux)
	defer server.Close()
	task, _ := proto.Marshal(&runnerv1.Task{Id: 24})
	h := &handle{connection: bridge.Connection{URL: server.URL}, task: task, job: &provider.OpaqueJob{Key: "24"}}
	if err := h.Finish(context.Background(), provider.Failed, nil); err == nil {
		t.Fatal("expected RPC failure")
	}
	f.mu.Lock()
	f.fail = false
	f.mu.Unlock()
	if err := h.Finish(context.Background(), provider.Failed, nil); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.calls != 2 {
		t.Fatalf("finalizations=%d", f.calls)
	}
}

func TestConnectionValidation(t *testing.T) {
	for _, tc := range []struct {
		name, url, id string
		typ           config.ProviderType
	}{
		{"url", "file:///tmp/forgejo", "334f3153-ecc4-4f96-ad08-63fc026d9780", config.ProviderForgejo},
		{"uuid", "http://127.0.0.1", "invalid", config.ProviderForgejo},
		{"nil uuid", "http://127.0.0.1", "00000000-0000-0000-0000-000000000000", config.ProviderForgejo},
		{"type", "http://127.0.0.1", "334f3153-ecc4-4f96-ad08-63fc026d9780", config.ProviderGitHub},
		{"missing type", "http://127.0.0.1", "334f3153-ecc4-4f96-ad08-63fc026d9780", config.ProviderUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &forgeConfig{URL: tc.url, UUID: tc.id, Token: config.Secret{Src: "inline", Value: "test"}}
			cfg.Type = tc.typ
			if _, err := cfg.connection(context.Background()); !errors.Is(err, provider.ErrInvalidConfig) {
				t.Fatalf("err=%v", err)
			}
		})
	}
}
