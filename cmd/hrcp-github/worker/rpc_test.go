// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: MIT

package worker_test

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"dimidiumlabs.io/hrc/internal/config"
	"dimidiumlabs.io/hrc/internal/provider"
	"dimidiumlabs.io/hrc/internal/runtime"
	"github.com/bazelbuild/rules_go/go/runfiles"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var worker = flag.String("worker", "", "C# worker executable")
var channelTest = flag.String("channel-test", "", "C# channel contract tests")

func TestChannelContract(t *testing.T) {
	path, err := runfiles.Rlocation(*channelTest)
	if err != nil {
		t.Fatal(err)
	}
	output, err := exec.Command(path, "--message-size", strconv.Itoa(config.GRPCMaxMessageSize)).CombinedOutput()
	if err != nil {
		t.Fatalf("C# channel contract: %v\n%s", err, output)
	}
}

func launch(t *testing.T) (*provider.WorkerSession, context.Context) {
	t.Helper()
	path, err := runfiles.Rlocation(*worker)
	if err != nil {
		t.Fatalf("worker path: %v", err)
	}
	t.Chdir(t.TempDir())
	// Reused slots contain job-owned files, not trusted host configuration.
	if err := os.WriteFile("appsettings.json", []byte("invalid workspace configuration"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	t.Cleanup(cancel)
	child, err := provider.LaunchWorker(ctx, runtime.Command{Path: path}, &runtime.Pool{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := child.Close(); err != nil {
			t.Error(err)
		}
		if err := child.Err(); err != nil {
			t.Error(err)
		}
	})
	return child, ctx
}

func TestLifecycle(t *testing.T) {
	for _, reason := range []provider.StopReason{provider.StopShutdown, provider.StopCancel, provider.StopForce} {
		t.Run(string(rune('0'+reason)), func(t *testing.T) {
			child, ctx := launch(t)
			if state, err := child.Status(ctx); err != nil || state != provider.WorkerState_READY {
				t.Fatalf("status = %v, %v", state, err)
			}
			if err := child.Shutdown(ctx, reason); err != nil {
				t.Fatal(err)
			}
			select {
			case <-child.Done():
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if _, err := os.Stat("_diag"); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("runner initialized before a job: %v", err)
			}
		})
	}
}

func TestParentDisconnect(t *testing.T) {
	child, ctx := launch(t)
	// Close the parent endpoint without a shutdown request. The worker must
	// exit normally, without needing the fallback kill.
	if err := child.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-child.Done():
	case <-ctx.Done():
		t.Fatal("worker did not exit after parent disconnect")
	}
}

func TestWorkerValidation(t *testing.T) {
	for _, test := range []struct {
		name, config, job string
		want              error
	}{
		{"malformed", `{}`, `{`, provider.ErrInvalidJob},
		{"unsafe-id", `{"Dir":"."}`, `{"ID":"../escape","Job":{}}`, provider.ErrInvalidJob},
		{"missing-job", `{}`, `{"ID":"test"}`, provider.ErrInvalidJob},
		{"invalid-settings", `{}`, `{"ID":"test","Settings":false,"Job":{}}`, provider.ErrInvalidJob},
		{"large-payload", `{}`, `{"ID":"test","Job":{"ignored":"` + strings.Repeat("x", config.GRPCMaxMessageSize-128) + `"}}`, provider.ErrInvalidConfig},
		{"missing-config", `{}`, `{"ID":"test","Job":{}}`, provider.ErrInvalidConfig},
		{"reserved-env", `{"Dir":".","Env":["RUNNER_TRACKING_GROUP=fake"]}`, `{"ID":"test","Job":{}}`, provider.ErrInvalidConfig},
	} {
		t.Run(test.name, func(t *testing.T) {
			child, ctx := launch(t)
			stream := child
			if err := stream.Send(&provider.TaskCommand{Command: &provider.TaskCommand_Start{Start: &provider.TaskStart{Config: []byte(test.config), Payload: []byte(test.job)}}}); err != nil {
				t.Fatal(err)
			}
			event, err := stream.Recv()
			if err != nil {
				t.Fatal(err)
			}
			if !errors.Is(event.GetOutcome().GetError().Err(), test.want) {
				t.Fatalf("outcome = %v, want %v", event, test.want)
			}
			if state, err := child.Status(ctx); err != nil || state != provider.WorkerState_READY {
				t.Fatalf("control after outcome = %v, %v", state, err)
			}
		})
	}
}

func TestWorkerRejectsOversizedMessage(t *testing.T) {
	path, err := runfiles.Rlocation(*worker)
	if err != nil {
		t.Fatal(err)
	}
	child, err := provider.LaunchWorker(t.Context(), runtime.Command{Path: path}, &runtime.Pool{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := child.Close(); err != nil {
			t.Error(err)
		}
		// gRPC terminates the stream on a send-size violation. Depending on parent
		// retirement timing, C# observes ResourceExhausted (exit 1) or cancellation
		// (exit 0). This is an intentional protocol failure, not normal shutdown.
		if err := child.Err(); err != nil {
			var exit *runtime.ExitError
			if !errors.As(err, &exit) || exit.Code != 1 {
				t.Error(err)
			}
		}
	})
	err = child.Send(&provider.TaskCommand{Command: &provider.TaskCommand_Start{Start: &provider.TaskStart{
		Config: []byte("{}"), Payload: make([]byte, config.GRPCMaxMessageSize),
	}}})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("oversized message: %v", err)
	}
}

func TestSingleExecution(t *testing.T) {
	child, ctx := launch(t)
	stream := child
	if err := stream.Send(&provider.TaskCommand{Command: &provider.TaskCommand_Start{Start: &provider.TaskStart{
		Config: []byte("{}"), Payload: []byte(`{"ID":"test","Job":{}}`),
	}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatal(err)
	}
	child.Close()
	select {
	case <-child.Done():
	case <-ctx.Done():
		t.Fatal("worker survived its one execution")
	}
}

func TestWorkerEnvironmentInheritance(t *testing.T) {
	path, err := runfiles.Rlocation(*channelTest)
	if err != nil {
		t.Fatal(err)
	}
	t.Chdir(t.TempDir())
	t.Setenv("RUNNER_TRACKING_GROUP", "tracking-from-parent")
	t.Setenv("HRC_TEST_DOTENV", "inherited")
	resources := t.TempDir()
	dotenv := "RUNNER_TRACKING_GROUP=forged\nHRC_TEST_DOTENV=dotenv\nHRC_TEST_FILE_ONLY=ignored\n"
	for _, dir := range []string{resources, "."} {
		if err := os.WriteFile(filepath.Join(dir, ".env"), []byte(dotenv), 0600); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
	defer cancel()
	// The C# fixture runs the real adapter, then launches another copy of
	// itself with ordinary environment inheritance and validates both processes.
	child, err := provider.LaunchWorker(ctx, runtime.Command{Path: path, Args: []string{"--worker-env"}}, &runtime.Pool{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := child.Close(); err != nil {
			t.Error(err)
		}
		if err := child.Err(); err != nil {
			t.Error(err)
		}
	})
	logs := t.TempDir()
	config, _ := json.Marshal(map[string]any{"Dir": resources, "LogRoot": logs, "Env": []string{"HRC_TEST_USER=explicit"}})
	if err := child.Send(&provider.TaskCommand{Command: &provider.TaskCommand_Start{Start: &provider.TaskStart{
		Config: config, Payload: []byte(`{"ID":"env-test","Settings":{"agentId":7},"Job":{"resources":{}}}`),
	}}}); err != nil {
		t.Fatal(err)
	}
	event, err := child.Recv()
	if err != nil || !errors.Is(event.GetOutcome().GetError().Err(), provider.ErrUnknown) {
		t.Fatalf("execution did not reach upstream Worker: %v, %v", event, err)
	}
	if files, err := filepath.Glob(filepath.Join(logs, "jobs", "env-test", "Worker_*.log")); err != nil || len(files) != 1 {
		t.Fatalf("direct diagnostics path = %v, %v", files, err)
	}
	if err := child.Close(); err != nil {
		t.Fatal(err)
	}
	if err := child.Err(); err != nil {
		t.Fatalf("worker/child environment contract: %v", err)
	}
}

func TestRuntimeWorkerAdapter(t *testing.T) {
	path, err := runfiles.Rlocation(*worker)
	if err != nil {
		t.Fatal(err)
	}
	t.Chdir(t.TempDir())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	run, err := provider.StartWorker(ctx, provider.ProcessConfig{
		Command: []string{path}, Config: json.RawMessage(`{}`),
	}, []byte(`{"ID":"test","Job":{}}`), runtime.Command{}, &runtime.Pool{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := run.Stop(ctx, provider.StopForce); err != nil {
			t.Error(err)
		}
	})
	select {
	case err := <-run.Done():
		if !errors.Is(err, provider.ErrInvalidConfig) || errors.Is(err, provider.ErrUnavailable) {
			t.Fatalf("completion = %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	result, data := run.Result()
	if result != provider.Unknown || len(data) != 0 {
		t.Fatalf("outcome = %v, %x", result, data)
	}
}

func TestRealWorkerUsesRuntimeConfiguration(t *testing.T) {
	child, ctx := launch(t)
	resources, err := filepath.Abs("resources")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(resources, 0755); err != nil {
		t.Fatal(err)
	}
	legacy := []string{".runner", ".credentials", ".credentials_rsaparams"}
	for _, name := range legacy {
		if err := os.WriteFile(name, []byte("not configuration"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	config, _ := json.Marshal(map[string]any{"Dir": resources})
	stream := child
	if err := stream.Send(&provider.TaskCommand{Command: &provider.TaskCommand_Start{Start: &provider.TaskStart{
		Config: config, Payload: []byte(`{"ID":"runtime-test","Settings":{"agentId":7,"agentName":"test"},"Job":{"resources":{}}}`),
	}}}); err != nil {
		t.Fatal(err)
	}
	// An immediate cancel must follow NewJob in the upstream channel, even
	// before the execution task has initialized HostContext.
	if err := stream.Send(&provider.TaskCommand{Command: &provider.TaskCommand_Stop{Stop: &provider.TaskStop{Reason: uint32(provider.StopCancel)}}}); err != nil {
		t.Fatal(err)
	}
	// The deliberately incomplete job fails inside upstream Worker, after the
	// real HostContext and HrcWorkerHost channel have been initialized. No GitHub
	// credentials or network service is needed for this transport integration.
	event, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if event.Outcome == nil || !errors.Is(event.Outcome.Error.Err(), provider.ErrUnknown) || len(event.Outcome.Data) != 0 {
		t.Fatalf("outcome = %v", event)
	}
	// Worker has disposed IProcessChannel, but the same host must still serve
	// control requests until its parent retires the RPC.
	if state, err := child.Status(ctx); err != nil || state != provider.WorkerState_READY {
		t.Fatalf("control after upstream disposal = %v, %v", state, err)
	}
	logs, err := filepath.Glob(filepath.Join("_diag", "jobs", "runtime-test", "Worker_*.log"))
	if err != nil || len(logs) != 1 {
		t.Fatalf("diagnostics = %v, %v", logs, err)
	}
	contents, err := os.ReadFile(logs[0])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(contents), "Using gRPC process channel.") {
		t.Fatalf("worker did not use the gRPC channel:\n%s", contents)
	}
	if !strings.Contains(string(contents), "Message received.") || strings.Contains(string(contents), "Actual 'CancelRequest'") {
		t.Fatalf("early cancellation overtook NewJob:\n%s", contents)
	}
	for _, name := range legacy {
		data, err := os.ReadFile(name)
		if err != nil || string(data) != "not configuration" {
			t.Fatalf("legacy file %s changed: %v", name, err)
		}
	}
}
