// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package space

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"dimidiumlabs.io/hrc/internal/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func testClient(t *testing.T, server *Server) (*Client, <-chan error) {
	t.Helper()
	left, right := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- server.Serve(t.Context(), right) }()
	client, err := NewClient(t.Context(), left, &Config{Capacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	return client, done
}

func TestConcurrentRunsPreserveOpaquePayload(t *testing.T) {
	client, _ := testClient(t, &Server{
		Configure: func(*Config) error { return nil },
		State:     func() State { return State{Capacity: 2} },
		Execute: func(_ context.Context, name string, payload []byte, _ <-chan provider.StopReason) (provider.Result, []byte, error) {
			if name != "test" {
				return provider.Failed, nil, provider.ErrUnsupportedJob
			}
			return provider.Succeeded, payload, nil
		},
	})
	var workers sync.WaitGroup
	for i := range 32 {
		workers.Go(func() {
			payload := []byte{byte(i), 0, 255, 128, '{', '}'}
			run, err := client.Start(t.Context(), "test", payload)
			if err != nil {
				t.Error(err)
				return
			}
			if err := <-run.Done(); err != nil {
				t.Error(err)
				return
			}
			result, data := run.Result()
			if result != provider.Succeeded || !bytes.Equal(data, payload) {
				t.Errorf("result: %v, %v", result, data)
			}
		})
	}
	workers.Wait()
}

func TestStreamCancellationIsIsolated(t *testing.T) {
	started := make(chan string, 2)
	stopped := make(chan string, 2)
	release := make(chan struct{})
	client, _ := testClient(t, &Server{
		Configure: func(*Config) error { return nil },
		State:     func() State { return State{Capacity: 2} },
		Execute: func(ctx context.Context, name string, _ []byte, _ <-chan provider.StopReason) (provider.Result, []byte, error) {
			started <- name
			select {
			case <-ctx.Done():
				stopped <- name
				return provider.Canceled, nil, nil
			case <-release:
				return provider.Succeeded, []byte(name), nil
			}
		},
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
	first.(*run).cancel() // Loss of just this RPC stream.
	if err := <-first.Done(); !errors.Is(err, ErrUnavailable) {
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
	client, done := testClient(t, &Server{
		Configure: func(*Config) error { return nil },
		Execute: func(ctx context.Context, _ string, _ []byte, _ <-chan provider.StopReason) (provider.Result, []byte, error) {
			started <- struct{}{}
			<-ctx.Done()
			canceled <- struct{}{}
			<-cleanup
			return provider.Canceled, nil, nil
		},
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
		if err := <-run.Done(); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("lost Run: %v", err)
		}
	}
	if _, err := client.Start(t.Context(), "test", nil); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("replayed on disconnected client: %v", err)
	}
}

func TestInitConfigAndFailure(t *testing.T) {
	for _, failure := range []error{nil, provider.ErrInvalidConfig} {
		left, right := net.Pipe()
		received := make(chan *Config, 1)
		done := make(chan error, 1)
		server := &Server{Configure: func(config *Config) error { received <- config; return failure }}
		go func() { done <- server.Serve(t.Context(), right) }()
		cfg := &Config{Capacity: 3, Ephemeral: true, Slot: "test", SlotConfig: []byte{0, 255}, Providers: map[string][]byte{"test": {128, 0}}}
		client, err := NewClient(t.Context(), left, cfg)
		if !errors.Is(err, failure) {
			t.Fatalf("Init error: %v", err)
		}
		got := <-received
		if got.Capacity != 3 || !got.Ephemeral || got.Slot != "test" || !bytes.Equal(got.SlotConfig, cfg.SlotConfig) || !bytes.Equal(got.Providers["test"], cfg.Providers["test"]) {
			t.Fatalf("config: %v", got)
		}
		if client != nil {
			client.Close()
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}

func TestInvalidRunDoesNotBreakConnection(t *testing.T) {
	client, _ := testClient(t, &Server{
		Configure: func(*Config) error { return nil },
		State:     func() State { return State{Capacity: 2} },
		Execute: func(ctx context.Context, _ string, _ []byte, _ <-chan provider.StopReason) (provider.Result, []byte, error) {
			<-ctx.Done()
			return provider.Canceled, nil, nil
		},
	})
	for _, first := range []*RunCommand{{}, {Command: &RunCommand_Stop{Stop: &StopRequest{Reason: RunStopReason_CANCEL}}}} {
		stream, err := client.rpc.Run(t.Context())
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
	stream, err := client.rpc.Run(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&RunCommand{Command: &RunCommand_Start{Start: &StartRequest{Provider: "test"}}}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&RunCommand{Command: &RunCommand_Stop{Stop: &StopRequest{}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad stop reason: %v", err)
	}
	if _, err := client.State(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := client.rpc.Init(t.Context(), &InitRequest{Version: Version, Config: &Config{Capacity: 1}}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("reinitialized scheduler: %v", err)
	}
}

func TestDisconnectBeforeInit(t *testing.T) {
	left, right := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- (&Server{}).Serve(t.Context(), right) }()
	left.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server waiting after disconnect")
	}
}

func TestStopReasonsAndForceAcknowledgement(t *testing.T) {
	for _, reason := range []provider.StopReason{provider.StopCancel, provider.StopShutdown, provider.StopForce} {
		t.Run(string(rune('0'+reason)), func(t *testing.T) {
			requested := make(chan provider.StopReason, 1)
			release := make(chan struct{})
			client, _ := testClient(t, &Server{
				Configure: func(*Config) error { return nil },
				Execute: func(ctx context.Context, _ string, _ []byte, stops <-chan provider.StopReason) (provider.Result, []byte, error) {
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
				},
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

func TestRPCsRequireInit(t *testing.T) {
	left, right := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- (&Server{}).Serve(t.Context(), right) }()
	connection, err := dial(left)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { connection.Close(); left.Close(); <-done }()
	rpc := NewSchedulerClient(connection)
	if _, err := rpc.Inspect(t.Context(), &InspectRequest{}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Inspect before Init: %v", err)
	}
	stream, err := rpc.Run(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Run before Init: %v", err)
	}
}

func TestStopRacingWithFinishedPreservesResult(t *testing.T) {
	client, _ := testClient(t, &Server{
		Configure: func(*Config) error { return nil },
		Execute: func(context.Context, string, []byte, <-chan provider.StopReason) (provider.Result, []byte, error) {
			return provider.Succeeded, []byte("completed"), nil
		},
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
		client, _ := testClient(t, &Server{
			Configure: func(*Config) error { return nil },
			Execute: func(context.Context, string, []byte, <-chan provider.StopReason) (provider.Result, []byte, error) {
				return provider.Failed, []byte("completion-data"), failure
			},
		})
		stream, err := client.rpc.Run(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if err := stream.Send(&RunCommand{Command: &RunCommand_Start{Start: &StartRequest{Provider: "test"}}}); err != nil {
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

		run, err := client.Start(t.Context(), "test", nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := <-run.Done(); !errors.Is(err, failure) {
			t.Fatalf("Run error: %v", err)
		}
		result, data := run.Result()
		if failure != nil {
			if result != provider.Unknown || len(data) != 0 {
				t.Fatalf("error mixed with result: %v %q", result, data)
			}
		} else if result != provider.Failed || string(data) != "completion-data" {
			t.Fatalf("lost result: %v %q", result, data)
		}
		client.Close()
	}
}
