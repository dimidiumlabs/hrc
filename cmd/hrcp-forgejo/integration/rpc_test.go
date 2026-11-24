// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package integration_test

import (
	"context"
	"encoding/json/v2"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	runnerv1 "code.forgejo.org/forgejo/actions-proto/runner/v1"
	"code.forgejo.org/forgejo/actions-proto/runner/v1/runnerv1connect"
	"connectrpc.com/connect"
	"dimidiumlabs.io/hrc/internal/provider"
	hrcruntime "dimidiumlabs.io/hrc/internal/runtime"
	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

var queueBinary = flag.String("queue", "", "queue runfile")
var workerBinary = flag.String("worker", "", "worker runfile")

const identity = "334f3153-ecc4-4f96-ad08-63fc026d9780"

// A real Connect HTTP endpoint with synthetic tasks, not a real Forgejo server.
type forge struct {
	runnerv1connect.UnimplementedRunnerServiceHandler
	mu                    sync.Mutex
	task                  *runnerv1.Task
	fetches, declarations int
	logs                  string
	final                 *runnerv1.TaskState
	rejectFinal           bool
	cancelJob             bool
	cancelMarker          string
	cancellationSent      bool
	terminalAttempts      int
}

func (f *forge) Declare(_ context.Context, req *connect.Request[runnerv1.DeclareRequest]) (*connect.Response[runnerv1.DeclareResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.declarations++
	if len(req.Msg.Labels) != 1 || req.Msg.Labels[0] != "poc" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("unexpected labels"))
	}
	return connect.NewResponse(&runnerv1.DeclareResponse{Runner: &runnerv1.Runner{Name: "poc"}}), nil
}

func (f *forge) FetchTask(_ context.Context, req *connect.Request[runnerv1.FetchTaskRequest]) (*connect.Response[runnerv1.FetchTaskResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fetches++
	key, err := uuid.Parse(req.Header().Get("x-runner-request-key"))
	if req.Msg.GetTaskCapacity() != 1 || err != nil || key == uuid.Nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("wrong capacity or idempotency key"))
	}
	return connect.NewResponse(&runnerv1.FetchTaskResponse{Task: f.task}), nil
}

func (f *forge) UpdateTask(_ context.Context, req *connect.Request[runnerv1.UpdateTaskRequest]) (*connect.Response[runnerv1.UpdateTaskResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	state := proto.Clone(req.Msg.State).(*runnerv1.TaskState)
	if state.StoppedAt != nil {
		f.terminalAttempts++
		if f.rejectFinal {
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("terminal reporting unavailable"))
		}
		f.final = state
	} else if f.cancelJob {
		if _, err := os.Stat(f.cancelMarker); err == nil {
			state.Result = runnerv1.Result_RESULT_CANCELLED
			f.cancellationSent = true
		}
	}
	keys := make([]string, 0, len(req.Msg.Outputs))
	for key := range req.Msg.Outputs {
		keys = append(keys, key)
	}
	return connect.NewResponse(&runnerv1.UpdateTaskResponse{State: state, SentOutputs: keys}), nil
}

func (f *forge) UpdateLog(_ context.Context, req *connect.Request[runnerv1.UpdateLogRequest]) (*connect.Response[runnerv1.UpdateLogResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, row := range req.Msg.Rows {
		f.logs += row.Content + "\n"
	}
	return connect.NewResponse(&runnerv1.UpdateLogResponse{AckIndex: req.Msg.Index + int64(len(req.Msg.Rows))}), nil
}

func (f *forge) UpdateStepSummary(context.Context, *connect.Request[runnerv1.UpdateStepSummaryRequest]) (*connect.Response[runnerv1.UpdateStepSummaryResponse], error) {
	return connect.NewResponse(&runnerv1.UpdateStepSummaryResponse{}), nil
}

func serve(t *testing.T, f *forge) string {
	t.Helper()
	path, handler := runnerv1connect.NewRunnerServiceHandler(f)
	mux := http.NewServeMux()
	mux.Handle("/api/actions"+path, http.StripPrefix("/api/actions", handler))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-runner-uuid") != identity || r.Header.Get("x-runner-token") != "poc-token" {
			http.Error(w, "missing runner credentials", http.StatusUnauthorized)
			return
		}
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	return server.URL
}

func task(t *testing.T, script string) *runnerv1.Task {
	t.Helper()
	ctx, err := structpb.NewStruct(map[string]any{
		"job": "poc", "event_name": "push", "event": map[string]any{},
		"repository": "owner/repo", "repository_owner": "owner",
		"ref": "refs/heads/main", "ref_name": "main", "ref_type": "branch",
		"sha": strings.Repeat("0", 40), "run_id": "1", "run_number": "1", "run_attempt": "1",
		"forgejo_default_actions_url": "https://code.forgejo.org", "forgejo_server_version": "14.0.0",
	})
	if err != nil {
		t.Fatal(err)
	}
	return &runnerv1.Task{
		Id: 42, Context: ctx,
		WorkflowPayload: []byte("name: poc\non: push\njobs:\n  poc:\n    runs-on: poc\n    steps:\n      - name: Execute\n        shell: bash\n        run: |\n          " + strings.ReplaceAll(script, "\n", "\n          ") + "\n"),
	}
}

// singleJobDemand grants one acquisition; subsequent waits block until the
// test cancels acquisition. This keeps the synthetic server's task single-use.
type singleJobDemand struct {
	mu       sync.Mutex
	reserved bool
	changed  chan struct{}
	released chan struct{}
}

func (d *singleJobDemand) State() (int, <-chan struct{}) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.reserved {
		return 0, d.changed
	}
	return 1, d.changed
}

func (d *singleJobDemand) Reserve() provider.Reservation {
	d.mu.Lock()
	defer d.mu.Unlock()
	first := !d.reserved
	if first {
		d.reserved = true
		close(d.changed)
		d.changed = make(chan struct{})
	}
	return &reservation{d: d, first: first}
}

type reservation struct {
	d     *singleJobDemand
	first bool
	once  sync.Once
}

func (r *reservation) Wait(ctx context.Context) error {
	if r.first {
		return ctx.Err()
	}
	<-ctx.Done()
	return ctx.Err()
}
func (r *reservation) Close() error {
	if r.first {
		r.once.Do(func() { close(r.d.released) })
	}
	return nil
}

func binary(t *testing.T, name string) string {
	t.Helper()
	path, err := runfiles.Rlocation(name)
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func acquire(t *testing.T, ctx context.Context, pool *hrcruntime.Pool, url string) provider.Handle {
	t.Helper()
	settings, err := json.Marshal(map[string]any{
		"type": "forgejo", "url": url, "uuid": identity,
		"token": map[string]string{"type": "inline", "value": "poc-token"},
	})
	if err != nil {
		t.Fatal(err)
	}
	demand := &singleJobDemand{changed: make(chan struct{}), released: make(chan struct{})}
	acquisition, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	q := &provider.ProcessQueue{
		Pool: pool, Command: []string{binary(t, *queueBinary)}, Config: settings,
	}
	jobs := q.Serve(acquisition, demand)
	var handle provider.Handle
	select {
	case handle = <-jobs:
		if handle == nil {
			t.Fatal("queue closed without delivering a job")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	t.Cleanup(func() {
		if err := handle.Close(ctx); err != nil {
			t.Error(err)
		}
		select {
		case <-demand.released:
		case <-ctx.Done():
			t.Error("queue leaked acquisition reservation")
		}
	})
	if handle.Job().ID() != "42" || handle.Job().Type() != "forgejo" {
		t.Fatalf("unexpected job: %s / %s", handle.Job().ID(), handle.Job().Type())
	}
	// Stop acquisition before starting execution. The real queue process must
	// keep serving Finish/Close on the delivered handle over its HRC channel.
	cancel()
	select {
	case _, open := <-jobs:
		if open {
			t.Fatal("queue delivered a second job")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	return handle
}

func execute(t *testing.T, ctx context.Context, pool *hrcruntime.Pool, payload []byte) (provider.Result, []byte, error) {
	t.Helper()
	root := t.TempDir()
	settings, err := json.Marshal(struct{ Dir, LogRoot string }{t.TempDir(), root})
	if err != nil {
		t.Fatal(err)
	}
	run, err := provider.StartWorker(ctx, provider.ProcessConfig{
		Command: []string{binary(t, *workerBinary)}, Config: settings,
	}, payload, hrcruntime.Command{
		Dir: t.TempDir(),
		Env: append(os.Environ(), "HOME="+t.TempDir(), "XDG_CACHE_HOME="+t.TempDir()),
	}, pool)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = run.Stop(ctx, provider.StopForce) })
	select {
	case err = <-run.Done():
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	result, data := run.Result()
	return result, data, err
}

func TestQueueAndWorkerIPC(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("workflows use bash")
	}
	for _, tc := range []struct {
		name, script           string
		result                 runnerv1.Result
		hrcResult              provider.Result
		rejectFinal, cancelJob bool
	}{
		{name: "success", script: "printf 'executed' > '%s'\necho hrc-forgejo-step-output", result: runnerv1.Result_RESULT_SUCCESS, hrcResult: provider.Succeeded},
		{name: "failure", script: "printf 'executed' > '%s'\nexit 7", result: runnerv1.Result_RESULT_FAILURE, hrcResult: provider.Failed},
		{name: "reporting-failure", script: "printf 'executed' > '%s'", rejectFinal: true},
		// Upstream stops execution, but reports FAILURE rather than CANCELLED.
		{name: "server-cancellation", script: "printf 'executed' > '%s'\nwhile :; do sleep 0.05; done", result: runnerv1.Result_RESULT_FAILURE, hrcResult: provider.Failed, cancelJob: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			t.Cleanup(cancel)
			pool := &hrcruntime.Pool{}
			t.Cleanup(func() {
				if err := pool.Close(); err != nil {
					t.Error(err)
				}
			})
			marker := filepath.Join(t.TempDir(), "executed")
			f := &forge{task: task(t, fmt.Sprintf(tc.script, marker)), rejectFinal: tc.rejectFinal, cancelJob: tc.cancelJob, cancelMarker: marker}
			handle := acquire(t, ctx, pool, serve(t, f))
			if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("queue executed the job")
			}
			payload, err := handle.Job().Marshal()
			if err != nil {
				t.Fatal(err)
			}
			result, data, completionErr := execute(t, ctx, pool, payload)
			if tc.rejectFinal {
				if completionErr == nil || errors.Is(completionErr, provider.ErrUnavailable) || result != provider.Unknown || len(data) != 0 {
					t.Fatalf("unacknowledged reporting outcome: %v, %q, %v", result, data, completionErr)
				}
			} else if completionErr != nil || result != tc.hrcResult || string(data) != "forgejo-reported-v1" {
				t.Fatalf("worker outcome: %v, %q, %v", result, data, completionErr)
			}
			if content, err := os.ReadFile(marker); err != nil || string(content) != "executed" {
				t.Fatalf("job did not execute: %q, %v", content, err)
			}
			// Inspect worker behavior before queue fallback finalization.
			func() {
				f.mu.Lock()
				defer f.mu.Unlock()
				if f.fetches != 1 || f.declarations != 1 {
					t.Fatalf("fetches=%d declarations=%d; worker must not acquire", f.fetches, f.declarations)
				}
				if tc.cancelJob && !f.cancellationSent {
					t.Fatal("worker never received server cancellation")
				}
				if tc.rejectFinal {
					if f.final != nil || f.terminalAttempts == 0 {
						t.Fatal("missing rejected terminal reporting")
					}
				} else if f.final == nil || f.final.Result != tc.result || f.terminalAttempts != 1 {
					t.Fatalf("terminal state=%v attempts=%d, want %v", f.final, f.terminalAttempts, tc.result)
				}
				if tc.name == "success" && !strings.Contains(f.logs, "hrc-forgejo-step-output") {
					t.Fatalf("step output not reported: %s", f.logs)
				}
			}()
			if tc.rejectFinal {
				// The queue must surface failed fallback reporting, then permit a
				// retry when the server recovers, using the same delivered handle.
				if err := handle.Finish(ctx, provider.Failed, data); err == nil {
					t.Fatal("fallback ignored reporting failure")
				}
				f.mu.Lock()
				f.rejectFinal = false
				f.mu.Unlock()
				if err := handle.Finish(ctx, provider.Failed, data); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := handle.Finish(ctx, result, data); err != nil {
					t.Fatal(err)
				}
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if tc.rejectFinal {
				if f.final == nil || f.final.Result != runnerv1.Result_RESULT_FAILURE {
					t.Fatalf("fallback state: %v", f.final)
				}
			} else if f.terminalAttempts != 1 {
				t.Fatalf("queue duplicated acknowledged finalization: %d", f.terminalAttempts)
			}
		})
	}
}

func TestMalformedPayloadIPC(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		t.Run(fmt.Sprint("protobuf=", malformed), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			t.Cleanup(cancel)
			pool := &hrcruntime.Pool{}
			t.Cleanup(func() {
				if err := pool.Close(); err != nil {
					t.Error(err)
				}
			})
			payload := []byte("not an assignment")
			if malformed {
				var err error
				payload, err = json.Marshal(map[string]any{
					"connection": map[string]string{"url": "http://127.0.0.1:1", "uuid": identity, "token": "poc-token"},
					"task":       []byte("not a protobuf task"),
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			result, data, err := execute(t, ctx, pool, payload)
			if err == nil || errors.Is(err, provider.ErrUnavailable) || result != provider.Unknown || len(data) != 0 {
				t.Fatalf("malformed payload outcome: %v, %q, %v", result, data, err)
			}
			if !malformed && !errors.Is(err, provider.ErrInvalidJob) {
				t.Fatalf("assignment validation error: %v", err)
			}
		})
	}
}
