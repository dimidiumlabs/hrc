// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"dimidiumlabs.io/hrc/internal/config"
	"dimidiumlabs.io/hrc/internal/provider"
	"dimidiumlabs.io/hrc/internal/runtime"
	"dimidiumlabs.io/hrc/internal/space"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func dial(conn net.Conn) (*grpc.ClientConn, error) {
	var once sync.Once
	return grpc.NewClient("passthrough:///test-monitor",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDisableRetry(),
		grpc.WithIdleTimeout(0),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			var accepted net.Conn
			once.Do(func() { accepted = conn })
			if accepted == nil {
				return nil, net.ErrClosed
			}
			return accepted, nil
		}),
	)
}

type rpcExecute func(context.Context, string, []byte, <-chan provider.StopReason) (provider.Result, []byte, error)

type rpcExecutor struct {
	capacity uint64
	execute  rpcExecute
}

func (e rpcExecutor) state() space.State { return space.State{Capacity: e.capacity} }
func (e rpcExecutor) run(ctx context.Context, name string, data []byte, stops <-chan provider.StopReason) (provider.Result, []byte, error) {
	return e.execute(ctx, name, data, stops)
}

func rpcPeer(t *testing.T, execute rpcExecute) (net.Conn, <-chan error) {
	t.Helper()
	left, right := net.Pipe()
	r := newRPC(runtime.WatchConnection(right))
	done := make(chan error, 1)
	go func() {
		err := r.Handshake(t.Context())
		if err == nil {
			err = r.serve(t.Context(), rpcExecutor{r.config.Capacity, execute})
		}
		if errors.Is(err, net.ErrClosed) {
			err = nil
		}
		done <- err
		close(done)
	}()
	t.Cleanup(func() {
		left.Close()
		r.Close()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	return left, done
}

func testRPC(t *testing.T, execute rpcExecute) space.MonitorClient {
	t.Helper()
	left, _ := rpcPeer(t, execute)
	connection, err := dial(left)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.Close() })
	client := space.NewMonitorClient(connection)
	reply, err := client.Init(t.Context(), &space.InitRequest{Version: space.Version, Config: &space.Config{Capacity: 2}})
	if err != nil {
		t.Fatal(err)
	}
	if err := reply.Error.Err(); err != nil {
		t.Fatal(err)
	}
	return client
}

func testDaemon(t *testing.T, execute rpcExecute) (*space.Daemon, <-chan error) {
	t.Helper()
	left, done := rpcPeer(t, execute)
	client, err := space.NewDaemon(t.Context(), left, &space.Config{Capacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	return client, done
}

func TestIdleConnectionStaysAvailable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, _ := testDaemon(t, nil)
		synctest.Wait()
		time.Sleep(time.Hour) // Advance beyond gRPC's default idle timeout.
		state, err := client.State(t.Context())
		if err != nil || state.Capacity != 2 {
			t.Fatalf("inspect after idle: %+v %v", state, err)
		}
	})
}

func TestRPCMessageSizeLimits(t *testing.T) {
	for _, direction := range []string{"receive", "send"} {
		for _, oversized := range []bool{false, true} {
			name, size := "within-limit", config.GRPCMaxMessageSize-128
			if oversized {
				name, size = "over-limit", config.GRPCMaxMessageSize
			}
			t.Run(direction+"/"+name, func(t *testing.T) {
				input, output := []byte("job"), bytes.Repeat([]byte("x"), size)
				if direction == "receive" {
					input, output = output, []byte("accepted")
				}
				client := testRPC(t, func(context.Context, string, []byte, <-chan provider.StopReason) (provider.Result, []byte, error) {
					return provider.Succeeded, output, nil
				})
				stream, err := client.Run(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				err = stream.Send(&space.RunCommand{Command: &space.RunCommand_Start{Start: &space.StartRequest{Provider: "test", Payload: input}}})
				if err != nil && !errors.Is(err, io.EOF) {
					t.Fatal(err)
				}
				event, err := stream.Recv()
				if oversized {
					if status.Code(err) != codes.ResourceExhausted {
						t.Fatalf("oversized message: %v", err)
					}
				} else if err != nil || !bytes.Equal(event.GetFinishResult().GetData(), output) {
					t.Fatalf("message: %v %v", event, err)
				}
			})
		}
	}
}

func TestHandshake(t *testing.T) {
	t.Run("timeout", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			left, right := net.Pipe()
			defer left.Close()
			r := newRPC(runtime.WatchConnection(right))
			defer r.Close()
			if err := r.Handshake(t.Context()); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("handshake timeout: %v", err)
			}
		})
	})
	t.Run("disconnect", func(t *testing.T) {
		left, right := net.Pipe()
		r := newRPC(runtime.WatchConnection(right))
		defer r.Close()
		left.Close()
		if err := r.Handshake(t.Context()); !errors.Is(err, net.ErrClosed) {
			t.Fatalf("handshake disconnect: %v", err)
		}
	})
	t.Run("initialization", func(t *testing.T) {
		root := t.TempDir()
		t.Chdir(root)
		left, right := net.Pipe()
		r := newRPC(runtime.WatchConnection(right))
		defer r.Close()
		connection, err := dial(left)
		if err != nil {
			t.Fatal(err)
		}
		defer connection.Close()
		client := space.NewMonitorClient(connection)
		if _, err := client.Inspect(t.Context(), &space.InspectRequest{}); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("Inspect before Init: %v", err)
		}
		stream, err := client.Run(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := stream.Recv(); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("Run before Init: %v", err)
		}
		handshake := make(chan error, 1)
		go func() { handshake <- r.Handshake(t.Context()) }()
		reply, err := client.Init(t.Context(), &space.InitRequest{Version: space.Version, Config: &space.Config{
			SpaceType: "host", Capacity: 2, Slot: "pgroup", SlotConfig: []byte("{}"),
		}})
		if err != nil {
			t.Fatal(err)
		}
		if err := reply.Error.Err(); err != nil {
			t.Fatal(err)
		}
		if err := <-handshake; err != nil {
			t.Fatal(err)
		}
		entries, err := os.ReadDir(root)
		if err != nil || len(entries) != 0 {
			t.Fatalf("handshake prepared environment: %v %v", entries, err)
		}
		m, err := NewMonitor(r)
		if err != nil {
			t.Fatal(err)
		}
		defer m.close()
		for _, dir := range []string{"cache", "tmp", "slots"} {
			if info, err := os.Stat(filepath.Join(root, dir)); err != nil || !info.IsDir() {
				t.Fatalf("environment directory %s: %v", dir, err)
			}
		}
	})
}

func TestStreamCancellationIsIsolated(t *testing.T) {
	started := make(chan string, 2)
	stopped := make(chan string, 2)
	release := make(chan struct{})
	client, _ := testDaemon(t, func(ctx context.Context, name string, _ []byte, _ <-chan provider.StopReason) (provider.Result, []byte, error) {
		started <- name
		select {
		case <-ctx.Done():
			stopped <- name
			return provider.Canceled, nil, nil
		case <-release:
			return provider.Succeeded, []byte(name), nil
		}
	})
	first, err := client.Start(t.Context(), "first", nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := client.Start(t.Context(), "second", nil)
	if err != nil {
		t.Fatal(err)
	}
	<-started
	<-started
	force, cancelForce := context.WithTimeout(t.Context(), time.Millisecond)
	defer cancelForce()
	if err := first.Stop(force, provider.StopForce); !errors.Is(err, space.ErrUnavailable) {
		t.Fatalf("force timeout: %v", err)
	}
	if err := <-first.Done(); !errors.Is(err, space.ErrUnavailable) {
		t.Fatalf("lost stream: %v", err)
	}
	if name := <-stopped; name != "first" {
		t.Fatalf("stopped %q", name)
	}
	if _, err := client.State(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-second.Done():
		t.Fatal("neighbor Run stopped")
	default:
	}
	close(release)
	if err := <-second.Done(); err != nil {
		t.Fatal(err)
	}
	result, data := second.Result()
	if result != provider.Succeeded || string(data) != "second" {
		t.Fatalf("neighbor result: %v %q", result, data)
	}
}

func TestDisconnectStopsAllRunsAndWaitsForCleanup(t *testing.T) {
	started := make(chan struct{}, 2)
	canceled := make(chan struct{}, 2)
	cleanup := make(chan struct{})
	client, done := testDaemon(t, func(ctx context.Context, _ string, _ []byte, _ <-chan provider.StopReason) (provider.Result, []byte, error) {
		started <- struct{}{}
		<-ctx.Done()
		canceled <- struct{}{}
		<-cleanup
		return provider.Canceled, nil, nil
	})
	runs := make([]provider.Run, 2)
	for i := range runs {
		var err error
		runs[i], err = client.Start(t.Context(), "test", nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	<-started
	<-started
	client.Close()
	<-canceled
	<-canceled
	select {
	case <-done:
		t.Fatal("server returned before cleanup")
	default:
	}
	close(cleanup)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	for _, run := range runs {
		if err := <-run.Done(); !errors.Is(err, space.ErrUnavailable) {
			t.Fatalf("lost Run: %v", err)
		}
	}
	if _, err := client.Start(t.Context(), "test", nil); !errors.Is(err, space.ErrUnavailable) {
		t.Fatalf("replayed on disconnected client: %v", err)
	}
}

func TestInitConfigAndFailure(t *testing.T) {
	for _, valid := range []bool{true, false} {
		left, right := net.Pipe()
		r := newRPC(runtime.WatchConnection(right))
		done := make(chan error, 1)
		go func() { done <- r.Handshake(t.Context()) }()
		cfg := &space.Config{Capacity: 3, SpaceType: "vm", Ephemeral: true, Slot: "test", SlotConfig: []byte{0, 255}, Providers: map[string][]byte{"test": {128, 0}}}
		if !valid {
			cfg.Capacity = 0
		}
		client, err := space.NewDaemon(t.Context(), left, cfg)
		handshakeErr := <-done
		if valid {
			if err != nil || handshakeErr != nil {
				t.Fatalf("handshake: %v %v", err, handshakeErr)
			}
			got := r.config
			if got.Capacity != 3 || got.SpaceType != "vm" || !got.Ephemeral || got.Slot != "test" || !bytes.Equal(got.SlotConfig, cfg.SlotConfig) || !bytes.Equal(got.Providers["test"], cfg.Providers["test"]) {
				t.Fatalf("config: %v", got)
			}
		} else if !errors.Is(err, space.ErrInvalidConfig) || !errors.Is(handshakeErr, space.ErrInvalidConfig) {
			t.Fatalf("invalid handshake: %v %v", err, handshakeErr)
		}
		if client != nil {
			client.Close()
		}
		r.Close()
		left.Close()
	}
}

func TestInvalidRunDoesNotBreakConnection(t *testing.T) {
	rpc := testRPC(t, func(ctx context.Context, _ string, _ []byte, _ <-chan provider.StopReason) (provider.Result, []byte, error) {
		<-ctx.Done()
		return provider.Canceled, nil, nil
	})
	for _, first := range []*space.RunCommand{{}, {Command: &space.RunCommand_Stop{Stop: &space.StopRequest{Reason: space.RunStopReason_CANCEL}}}} {
		stream, err := rpc.Run(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if err := stream.Send(first); err != nil {
			t.Fatal(err)
		}
		if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("bad first command: %v", err)
		}
	}
	stream, err := rpc.Run(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&space.RunCommand{Command: &space.RunCommand_Start{Start: &space.StartRequest{Provider: "test"}}}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&space.RunCommand{Command: &space.RunCommand_Stop{Stop: &space.StopRequest{}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad stop reason: %v", err)
	}
	if _, err := rpc.Inspect(t.Context(), &space.InspectRequest{}); err != nil {
		t.Fatal(err)
	}
	if _, err := rpc.Init(t.Context(), &space.InitRequest{Version: space.Version, Config: &space.Config{Capacity: 1}}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("reinitialized monitor: %v", err)
	}
}

func TestStopReasonsAndForceAcknowledgement(t *testing.T) {
	for name, reason := range map[string]provider.StopReason{
		"cancel": provider.StopCancel, "shutdown": provider.StopShutdown, "force": provider.StopForce,
	} {
		t.Run(name, func(t *testing.T) {
			requested := make(chan provider.StopReason, 1)
			release := make(chan struct{})
			client, _ := testDaemon(t, func(ctx context.Context, _ string, _ []byte, stops <-chan provider.StopReason) (provider.Result, []byte, error) {
				select {
				case reason := <-stops:
					requested <- reason
				case <-ctx.Done():
					return provider.Canceled, nil, ctx.Err()
				}
				select {
				case <-release:
				case <-ctx.Done():
					return provider.Canceled, nil, ctx.Err()
				}
				return provider.Canceled, []byte{0, 255}, nil
			})
			run, err := client.Start(t.Context(), "test", nil)
			if err != nil {
				t.Fatal(err)
			}
			stopped := make(chan error, 1)
			go func() { stopped <- run.Stop(t.Context(), reason) }()
			if got := <-requested; got != reason {
				t.Fatalf("stop reason: %v", got)
			}
			if reason == provider.StopForce {
				select {
				case <-stopped:
					t.Fatal("Force acknowledged before cleanup")
				default:
				}
			} else if err := <-stopped; err != nil {
				t.Fatal(err)
			}
			close(release)
			if reason == provider.StopForce {
				if err := <-stopped; err != nil {
					t.Fatal(err)
				}
			}
			if err := <-run.Done(); err != nil {
				t.Fatal(err)
			}
			result, data := run.Result()
			if result != provider.Canceled || !bytes.Equal(data, []byte{0, 255}) {
				t.Fatalf("result: %v %v", result, data)
			}
		})
	}
}

func TestStopRacingWithFinishedPreservesResult(t *testing.T) {
	client, _ := testDaemon(t, func(context.Context, string, []byte, <-chan provider.StopReason) (provider.Result, []byte, error) {
		return provider.Succeeded, []byte("completed"), nil
	})
	for range 32 {
		run, err := client.Start(t.Context(), "test", nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := run.Stop(t.Context(), provider.StopCancel); err != nil {
			t.Fatal(err)
		}
		if err := <-run.Done(); err != nil {
			t.Fatal(err)
		}
		result, data := run.Result()
		if result != provider.Succeeded || string(data) != "completed" {
			t.Fatalf("lost result: %v %q", result, data)
		}
	}
}

func TestExclusiveFinishEvents(t *testing.T) {
	for _, failure := range []error{nil, provider.ErrUnknown} {
		client := testRPC(t, func(context.Context, string, []byte, <-chan provider.StopReason) (provider.Result, []byte, error) {
			return provider.Failed, []byte("completion-data"), failure
		})
		stream, err := client.Run(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if err := stream.Send(&space.RunCommand{Command: &space.RunCommand_Start{Start: &space.StartRequest{Provider: "test"}}}); err != nil {
			t.Fatal(err)
		}
		event, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if failure != nil {
			if event.GetFinishError() == nil || !errors.Is(event.GetFinishError().Error.Err(), failure) || event.GetFinishResult() != nil {
				t.Fatalf("FinishError: %v", event)
			}
		} else if event.GetFinishResult() == nil || event.GetFinishResult().Result != uint32(provider.Failed) || string(event.GetFinishResult().Data) != "completion-data" || event.GetFinishError() != nil {
			t.Fatalf("FinishResult: %v", event)
		}
		if event, err := stream.Recv(); !errors.Is(err, io.EOF) {
			t.Fatalf("expected EOF after terminal event: %v %v", event, err)
		}
	}
}
