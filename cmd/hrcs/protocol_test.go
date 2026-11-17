// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"dimidiumlabs.io/hrc/internal/provider"
	"dimidiumlabs.io/hrc/internal/space"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func dial(conn net.Conn) (*grpc.ClientConn, error) {
	var once sync.Once
	return grpc.NewClient("passthrough:///test-scheduler",
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

func testRPC(t *testing.T, server *Scheduler) space.SchedulerClient {
	t.Helper()
	left, right := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- server.Serve(t.Context(), right) }()
	connection, err := dial(left)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.Close(); left.Close(); <-done })
	rpc := space.NewSchedulerClient(connection)
	ready, err := rpc.Init(t.Context(), &space.InitRequest{Version: space.Version, Config: &space.Config{Capacity: 2}})
	if err != nil {
		t.Fatal(err)
	}
	if err := ready.Error.Err(); err != nil {
		t.Fatal(err)
	}
	return rpc
}

func testDaemon(t *testing.T, server *Scheduler) (*space.Daemon, <-chan error) {
	t.Helper()
	left, right := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- server.Serve(t.Context(), right) }()
	client, err := space.NewDaemon(t.Context(), left, &space.Config{Capacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	return client, done
}

type daemonDelegateFunc func(<-chan struct{})

var _ space.DaemonDelegate = daemonDelegateFunc(nil)

func (f daemonDelegateFunc) Disconnected(done <-chan struct{}) { f(done) }

func TestDisconnectNotifiesDelegateOutsideTeardown(t *testing.T) {
	var client space.Daemon
	notified := make(chan (<-chan struct{}), 1)
	client.Delegate = daemonDelegateFunc(func(done <-chan struct{}) {
		client.Close() // Must not recurse into an unfinished abort's sync.Once.
		notified <- done
	})
	left, right := net.Pipe()
	server := &Scheduler{Configure: func(*space.Config) error { return nil }}
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Serve(t.Context(), right) }()
	if err := client.Connect(t.Context(), left, &space.Config{Capacity: 1}); err != nil {
		t.Fatal(err)
	}
	client.SetStatus(space.Ready, nil)
	t.Cleanup(func() { client.Close() })
	generation := client.Done()
	left.Close()
	select {
	case got := <-notified:
		if got != generation || client.Status() != space.Broken {
			t.Fatal("delegate received the wrong generation or status")
		}
	case <-time.After(time.Second):
		t.Fatal("delegate was not notified or deadlocked in space.Daemon.Close")
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
	client.Close()
	select {
	case <-notified:
		t.Fatal("delegate notified more than once")
	default:
	}
}

func TestDaemonReconnectPreservesGenerationBoundaries(t *testing.T) {
	var client space.Daemon
	if client.Status() != space.Idle || client.Err() != nil {
		t.Fatal("zero Daemon is not idle")
	}
	t.Cleanup(func() { client.Close() })
	connect := func(server *Scheduler, capacity uint64) <-chan error {
		t.Helper()
		left, right := net.Pipe()
		done := make(chan error, 1)
		go func() { done <- server.Serve(t.Context(), right) }()
		if err := client.Connect(t.Context(), left, &space.Config{Capacity: capacity}); err != nil {
			t.Fatal(err)
		}
		if client.Status() != space.Starting {
			t.Fatal("handshake published Ready before the owner's startup completed")
		}
		client.SetStatus(space.Ready, nil)
		if client.Status() != space.Ready || client.Err() != nil {
			t.Fatalf("connected status: %v %v", client.Status(), client.Err())
		}
		return done
	}
	started := make(chan struct{})
	firstDone := connect(&Scheduler{
		Configure: func(*space.Config) error { return nil },
		State:     func() space.State { return space.State{Capacity: 1} },
		Execute: func(ctx context.Context, _ string, _ []byte, _ <-chan provider.StopReason) (provider.Result, []byte, error) {
			close(started)
			<-ctx.Done()
			return provider.Canceled, nil, nil
		},
	}, 1)
	lost, err := client.Start(t.Context(), "test", nil)
	if err != nil {
		t.Fatal(err)
	}
	<-started
	oldDone := client.Done()
	secondDone := connect(&Scheduler{
		Configure: func(*space.Config) error { return nil },
		State:     func() space.State { return space.State{Capacity: 2} },
		Execute: func(_ context.Context, _ string, data []byte, _ <-chan provider.StopReason) (provider.Result, []byte, error) {
			return provider.Succeeded, data, nil
		},
	}, 2)
	if oldDone == client.Done() {
		t.Fatal("reconnection reused the old generation")
	}
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-lost.Done(); !errors.Is(err, space.ErrUnavailable) {
		t.Fatalf("old Run was replayed or its failure lost: %v", err)
	}
	if state, err := client.State(t.Context()); err != nil || state.Capacity != 2 || client.Status() != space.Ready {
		t.Fatalf("new generation: %+v %v, status=%v", state, err, client.Status())
	}
	next, err := client.Start(t.Context(), "test", []byte("new generation"))
	if err != nil {
		t.Fatal(err)
	}
	if err := <-next.Done(); err != nil {
		t.Fatal(err)
	}
	if result, data := next.Result(); result != provider.Succeeded || string(data) != "new generation" {
		t.Fatalf("new Run: %v %q", result, data)
	}
	client.SetStatus(space.Destroying, nil)
	client.Close()
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	if client.Status() != space.Destroying || client.Err() != nil {
		t.Fatal("connection shutdown overwrote the explicit lifecycle status")
	}
	client.SetStatus(space.Idle, nil)
	if state, err := client.State(t.Context()); err != nil || state != (space.State{}) {
		t.Fatalf("idle client state: %+v %v", state, err)
	}
}

type blockingCloseConn struct {
	net.Conn
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *blockingCloseConn) Close() error {
	c.once.Do(func() {
		close(c.started)
		<-c.release
		c.Conn.Close()
	})
	return nil
}

func TestDaemonGettersDoNotWaitForTransportCleanup(t *testing.T) {
	left, right := net.Pipe()
	transport := &blockingCloseConn{Conn: left, started: make(chan struct{}), release: make(chan struct{})}
	serverDone := make(chan error, 1)
	server := &Scheduler{Configure: func(*space.Config) error { return nil }}
	go func() { serverDone <- server.Serve(t.Context(), right) }()
	client, err := space.NewDaemon(t.Context(), transport, &space.Config{Capacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan struct{})
	go func() { client.Close(); close(closed) }()
	<-transport.started
	defer func() { close(transport.release); <-closed; <-serverDone }()
	inspected := make(chan struct{})
	go func() {
		if client.Status() != space.Broken || !errors.Is(client.Err(), space.ErrUnavailable) {
			t.Error("connection failure not published before cleanup")
		}
		client.SetStatus(space.Ready, nil)
		if client.Status() != space.Broken || !errors.Is(client.Err(), space.ErrUnavailable) {
			t.Error("late Ready overwrote connection failure")
		}
		if state, err := client.State(t.Context()); state != (space.State{}) || !errors.Is(err, space.ErrUnavailable) {
			t.Errorf("failed client state: %+v %v", state, err)
		}
		<-client.Done()
		close(inspected)
	}()
	select {
	case <-inspected:
	case <-time.After(time.Second):
		t.Fatal("inspection waited for transport cleanup")
	}
}

func TestIdleConnectionStaysAvailable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, done := testDaemon(t, &Scheduler{
			Configure: func(*space.Config) error { return nil },
			State:     func() space.State { return space.State{Capacity: 2} },
			Execute: func(_ context.Context, _ string, payload []byte, _ <-chan provider.StopReason) (provider.Result, []byte, error) {
				return provider.Succeeded, payload, nil
			},
		})
		// No RPCs during this interval. Advance past gRPC's default idle
		// timeout without spending half an hour in the test runner.
		synctest.Wait()
		time.Sleep(time.Hour)
		synctest.Wait()
		select {
		case <-client.Done():
			t.Fatalf("healthy idle connection closed: %v", client.Err())
		default:
		}
		if state, err := client.State(t.Context()); err != nil || state.Capacity != 2 {
			t.Fatalf("inspect after idle: %+v %v", state, err)
		}
		run, err := client.Start(t.Context(), "test", []byte("after idle"))
		if err != nil {
			t.Fatal(err)
		}
		if err := <-run.Done(); err != nil {
			t.Fatal(err)
		}
		if result, data := run.Result(); result != provider.Succeeded || string(data) != "after idle" {
			t.Fatalf("run after idle: %v %q", result, data)
		}
		client.Close()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
}

func TestConcurrentRunsPreserveOpaquePayload(t *testing.T) {
	client, _ := testDaemon(t, &Scheduler{
		Configure: func(*space.Config) error { return nil },
		State:     func() space.State { return space.State{Capacity: 2} },
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
	client, _ := testDaemon(t, &Scheduler{
		Configure: func(*space.Config) error { return nil },
		State:     func() space.State { return space.State{Capacity: 2} },
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
	client, done := testDaemon(t, &Scheduler{
		Configure: func(*space.Config) error { return nil },
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
		if err := <-run.Done(); !errors.Is(err, space.ErrUnavailable) {
			t.Fatalf("lost Run: %v", err)
		}
	}
	if _, err := client.Start(t.Context(), "test", nil); !errors.Is(err, space.ErrUnavailable) {
		t.Fatalf("replayed on disconnected client: %v", err)
	}
}

func TestInitConfigAndFailure(t *testing.T) {
	for _, failure := range []error{nil, provider.ErrInvalidConfig} {
		left, right := net.Pipe()
		received := make(chan *space.Config, 1)
		done := make(chan error, 1)
		server := &Scheduler{Configure: func(config *space.Config) error { received <- config; return failure }}
		go func() { done <- server.Serve(t.Context(), right) }()
		cfg := &space.Config{Capacity: 3, Ephemeral: true, Slot: "test", SlotConfig: []byte{0, 255}, Providers: map[string][]byte{"test": {128, 0}}}
		client, err := space.NewDaemon(t.Context(), left, cfg)
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
	rpc := testRPC(t, &Scheduler{
		Configure: func(*space.Config) error { return nil },
		State:     func() space.State { return space.State{Capacity: 2} },
		Execute: func(ctx context.Context, _ string, _ []byte, _ <-chan provider.StopReason) (provider.Result, []byte, error) {
			<-ctx.Done()
			return provider.Canceled, nil, nil
		},
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
		t.Fatalf("reinitialized scheduler: %v", err)
	}
}

func TestDisconnectBeforeInit(t *testing.T) {
	left, right := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- (&Scheduler{}).Serve(t.Context(), right) }()
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
			client, _ := testDaemon(t, &Scheduler{
				Configure: func(*space.Config) error { return nil },
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
	go func() { done <- (&Scheduler{}).Serve(t.Context(), right) }()
	connection, err := dial(left)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { connection.Close(); left.Close(); <-done }()
	rpc := space.NewSchedulerClient(connection)
	if _, err := rpc.Inspect(t.Context(), &space.InspectRequest{}); status.Code(err) != codes.FailedPrecondition {
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
	client, _ := testDaemon(t, &Scheduler{
		Configure: func(*space.Config) error { return nil },
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
		newServer := func() *Scheduler {
			return &Scheduler{
				Configure: func(*space.Config) error { return nil },
				Execute: func(context.Context, string, []byte, <-chan provider.StopReason) (provider.Result, []byte, error) {
					return provider.Failed, []byte("completion-data"), failure
				},
			}
		}
		rpc := testRPC(t, newServer())
		client, _ := testDaemon(t, newServer())
		stream, err := rpc.Run(t.Context())
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
