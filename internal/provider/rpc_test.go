// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package provider_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	p "dimidiumlabs.io/hrc/internal/provider"
	"dimidiumlabs.io/hrc/internal/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestMain(m *testing.M) {
	if len(os.Args) == 4 && os.Args[1] == "replacement-child" {
		workerReplacementChild()
		os.Exit(0)
	}
	if len(os.Args) > 1 && os.Args[1] == "exit-child" {
		os.Exit(42)
	}
	if len(os.Args) == 4 && os.Args[1] == "connected-exit-child" {
		conn, err := runtime.InheritedConnection(os.Args[2:])
		if err != nil {
			os.Exit(30)
		}
		conn.Close()
		os.Exit(42)
	}
	if len(os.Args) == 4 && os.Args[1] == "queue-child" {
		conn, err := runtime.InheritedConnection(os.Args[2:])
		if err != nil {
			os.Exit(30)
		}
		server := &p.QueueClient{New: func(start *p.QueueStart) (p.Queue, error) {
			if string(start.Config) == "invalid-config" {
				return nil, errors.Join(p.ErrInvalidConfig, errors.New("invalid test configuration"))
			}
			return rpcQueue{mode: string(start.Config)}, nil
		}}
		err = server.Connect(context.Background(), conn)
		if err != nil {
			os.Exit(31)
		}
		os.Exit(0)
	}
	if len(os.Args) == 4 && os.Args[1] == "worker-child" {
		conn, err := runtime.InheritedConnection(os.Args[2:])
		if err != nil {
			os.Exit(32)
		}
		server := &p.WorkerClient{Start: func(_ context.Context, start *p.TaskStart) (p.Run, error) {
			if string(start.Config) == "invalid" {
				return nil, p.ErrInvalidConfig
			}
			if string(start.Config) == "crash" {
				os.Exit(43)
			}
			r := &rpcRun{done: make(chan error, 1), finished: make(chan struct{}), data: start.Payload, mode: string(start.Config)}
			if string(start.Config) != "wait" && string(start.Config) != "race" {
				r.finish(p.Succeeded)
			}
			return r, nil
		}}
		server.Connect(context.Background(), conn)
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// Open a second application stream without replacing the accepted connection.
// Rejection must not cancel the original worker channel.
func workerReplacementChild() {
	connection, err := runtime.InheritedConnection(os.Args[2:])
	if err != nil {
		os.Exit(30)
	}
	var connected sync.Once
	conn, err := grpc.NewClient("passthrough:///parent",
		grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry(), grpc.WithIdleTimeout(0),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			var accepted net.Conn
			connected.Do(func() { accepted = connection })
			if accepted == nil {
				return nil, net.ErrClosed
			}
			return accepted, nil
		}))
	if err != nil {
		os.Exit(31)
	}
	client := p.NewWorkerServiceClient(conn)
	original, err := client.Execute(context.Background())
	if err != nil || original.Send(&p.TaskEvent{Ready: true}) != nil {
		os.Exit(32)
	}
	// Streams can arrive in either order. A parent command proves that the
	// original Ready has been accepted before attempting its replacement.
	command, err := original.Recv()
	if err != nil {
		os.Exit(36)
	}
	replacement, err := client.Execute(context.Background())
	if err != nil || replacement.Send(&p.TaskEvent{Ready: true}) != nil {
		os.Exit(33)
	}
	if _, err := replacement.Recv(); status.Code(err) != codes.FailedPrecondition {
		os.Exit(34)
	}
	for {
		request := command.GetControl()
		if request == nil {
			os.Exit(35)
		}
		reply := &p.WorkerReply{Id: request.Id, State: p.WorkerState_READY}
		if request.GetShutdown() != nil {
			reply.State = p.WorkerState_STOPPING
			reply.Exited = true
		}
		if original.Send(&p.TaskEvent{Reply: reply}) != nil {
			break
		}
		command, err = original.Recv()
		if err != nil {
			break
		}
	}
	conn.Close()
	connection.Close()
}

type rpcQueue struct{ mode string }

func (q rpcQueue) Serve(ctx context.Context, d p.Demand) <-chan p.Handle {
	jobs := make(chan p.Handle)
	go func() {
		defer close(jobs)
		count := 1
		if q.mode == "block-finish" || q.mode == "block-close" {
			count = 2
		}
		for i := range count {
			r := d.Reserve()
			if r.Wait(ctx) != nil {
				r.Close()
				return
			}
			mode := q.mode
			if i > 0 {
				mode = ""
			}
			h := &rpcHandle{reservation: r, events: make(chan p.Event, 2), mode: mode}
			select {
			case jobs <- h:
			case <-ctx.Done():
				h.Close(ctx)
			}
		}
	}()
	return jobs
}

type rpcHandle struct {
	reservation p.Reservation
	events      chan p.Event
	once        sync.Once
	mode        string
}

func (h *rpcHandle) Job() p.Job {
	key := "job"
	if h.mode == "block-finish" || h.mode == "block-close" {
		key = h.mode
	}
	return &p.OpaqueJob{Key: key, Provider: "test", Payload: []byte{0, 255, 128, 1}}
}
func (h *rpcHandle) Events() <-chan p.Event {
	if h.mode == "crash" {
		os.Exit(23)
	}
	return h.events
}
func (h *rpcHandle) Finish(ctx context.Context, result p.Result, data []byte) error {
	if h.mode == "block-finish" && result == p.Abandoned {
		return h.blockOperation(ctx)
	}
	if result == p.Succeeded && !bytes.Equal(data, []byte{255, 0}) {
		return p.ErrInvalidJob
	}
	return nil
}
func (h *rpcHandle) blockOperation(ctx context.Context) error {
	var grace time.Duration
	if deadline, ok := ctx.Deadline(); ok {
		grace = time.Until(deadline)
	}
	h.events <- p.Event{Type: p.NetworkError, Grace: grace}
	<-ctx.Done()
	h.events <- p.Event{Type: p.NetworkError, Err: ctx.Err()}
	return ctx.Err()
}
func (h *rpcHandle) Close(ctx context.Context) error {
	var err error
	h.once.Do(func() {
		if h.mode == "block-close" {
			err = h.blockOperation(ctx)
		}
		close(h.events)
		h.reservation.Close()
	})
	return err
}

type rpcDemand struct {
	mu      sync.Mutex
	used    int
	changed chan struct{}
}

func (d *rpcDemand) State() (int, <-chan struct{}) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return 1 - d.used, d.changed
}
func (d *rpcDemand) Reserve() p.Reservation {
	d.mu.Lock()
	d.used++
	close(d.changed)
	d.changed = make(chan struct{})
	d.mu.Unlock()
	return &rpcReservation{d: d}
}

type rpcReservation struct {
	d    *rpcDemand
	once sync.Once
}

func (r *rpcReservation) Wait(ctx context.Context) error { return ctx.Err() }
func (r *rpcReservation) Close() error {
	r.once.Do(func() {
		r.d.mu.Lock()
		r.d.used--
		close(r.d.changed)
		r.d.changed = make(chan struct{})
		r.d.mu.Unlock()
	})
	return nil
}

func queueForTest(t *testing.T, mode string) (<-chan p.Handle, *rpcDemand, context.CancelFunc) {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	d := &rpcDemand{changed: make(chan struct{})}
	q := &p.ProcessQueue{Command: []string{self, "queue-child"}, Config: []byte(mode)}
	return q.Serve(ctx, d), d, cancel
}
func receiveHandle(t *testing.T, jobs <-chan p.Handle) p.Handle {
	t.Helper()
	select {
	case h, ok := <-jobs:
		if !ok {
			t.Fatal("queue ended without a job")
		}
		return h
	case <-time.After(10 * time.Second):
		t.Fatal("queue did not deliver")
		return nil
	}
}
func assertReleased(t *testing.T, d *rpcDemand) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		d.mu.Lock()
		used := d.used
		d.mu.Unlock()
		if used == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("reservation leaked: %d", used)
		}
		time.Sleep(time.Millisecond)
	}
}
func TestQueueIPCDrainAndOpaqueCompletion(t *testing.T) {
	jobs, d, cancel := queueForTest(t, "")
	h := receiveHandle(t, jobs)
	defer h.Close(context.Background())
	payload, err := h.Job().Marshal()
	if err != nil || !bytes.Equal(payload, []byte{0, 255, 128, 1}) {
		t.Fatalf("payload decoded or changed: %v %v", payload, err)
	}
	// Acquisition may end while a delivered Handle still owns its lease.
	cancel()
	select {
	case _, ok := <-jobs:
		if ok {
			t.Fatal("unexpected second job")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("acquisition did not stop")
	}
	if err := h.Finish(t.Context(), p.Succeeded, []byte{255, 0}); err != nil {
		t.Fatal(err)
	}
	if err := h.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	assertReleased(t, d)
}
func TestQueueOperationCancellationIsolated(t *testing.T) {
	for _, operation := range []string{"finish", "close"} {
		for _, cancellation := range []string{"deadline", "cancel", "already-canceled"} {
			// Close is single-use even when its context is already canceled;
			// exercise pre-canceled requests through the retryable Finish path.
			if operation == "close" && cancellation == "already-canceled" {
				continue
			}
			t.Run(operation+"/"+cancellation, func(t *testing.T) {
				jobs, d, _ := queueForTest(t, "block-"+operation)
				first := receiveHandle(t, jobs)
				second := receiveHandle(t, jobs)
				// Delivery goroutines need not preserve the order of offers.
				if first.Job().ID() != "block-"+operation {
					first, second = second, first
				}
				t.Cleanup(func() {
					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					defer cancel()
					first.Close(ctx)
					second.Close(ctx)
				})
				ctx, cancel := context.WithCancel(t.Context())
				if cancellation == "deadline" {
					cancel()
					ctx, cancel = context.WithTimeout(t.Context(), time.Second)
				}
				defer cancel()
				if cancellation == "already-canceled" {
					cancel()
				}
				done := make(chan error, 1)
				go func() {
					if operation == "close" {
						done <- first.Close(ctx)
					} else {
						done <- first.Finish(ctx, p.Abandoned, nil)
					}
				}()
				if cancellation != "already-canceled" {
					select {
					case event := <-first.Events():
						if event.Type != p.NetworkError || event.Err != nil {
							t.Fatalf("operation did not start: %+v", event)
						}
						if cancellation == "deadline" && event.Grace <= 0 {
							t.Fatal("provider did not receive operation deadline")
						}
					case <-time.After(10 * time.Second):
						t.Fatal("operation did not start")
					}
					if cancellation == "cancel" {
						cancel()
					}
				}
				want := context.Canceled
				if cancellation == "deadline" {
					want = context.DeadlineExceeded
				}
				select {
				case err := <-done:
					if !errors.Is(err, want) {
						t.Fatalf("operation error = %v; want %v", err, want)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("operation ignored cancellation")
				}
				// Finish retains its handle, so its event confirms that the
				// provider's operation context (not just the caller) was canceled.
				if operation == "finish" && cancellation != "already-canceled" {
					select {
					case event := <-first.Events():
						if event.Type != p.NetworkError || (!errors.Is(event.Err, context.Canceled) && !errors.Is(event.Err, context.DeadlineExceeded)) {
							t.Fatalf("provider operation was not canceled: %+v", event)
						}
					case <-time.After(10 * time.Second):
						t.Fatal("provider operation ignored cancellation")
					}
				}
				if err := second.Finish(t.Context(), p.Succeeded, []byte{255, 0}); err != nil {
					t.Fatalf("unrelated handle lost its session: %v", err)
				}
				select {
				case event := <-second.Events():
					t.Fatalf("unrelated handle received an event: %+v", event)
				default:
				}
				if err := second.Close(t.Context()); err != nil {
					t.Fatalf("unrelated handle could not close: %v", err)
				}
				if cancellation != "already-canceled" && operation == "close" {
					assertReleased(t, d)
				}
			})
		}
	}
}

func TestQueueInvalidConfigurationReported(t *testing.T) {
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var pool runtime.Pool
	defer pool.Close()
	failures := make(chan error, 1)
	q := &p.ProcessQueue{Pool: &pool, Command: []string{self, "queue-child"}, Config: []byte("invalid-config"), Name: "broken", OnError: func(err error) { failures <- err }}
	jobs := q.Serve(ctx, &rpcDemand{changed: make(chan struct{})})
	select {
	case err := <-failures:
		if !errors.Is(err, p.ErrInvalidConfig) || !strings.Contains(err.Error(), "broken") || !strings.Contains(err.Error(), "invalid test configuration") {
			t.Fatalf("configuration error lost across RPC: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("queue did not report invalid configuration")
	}
	select {
	case _, ok := <-jobs:
		if ok {
			t.Fatal("invalid queue delivered a job")
		}
	case <-ctx.Done():
		t.Fatal("invalid queue did not stop")
	}
	if len(pool.Children()) != 0 {
		t.Fatal("invalid queue leaked its process")
	}
}

func TestQueueLossCancelsDeliveredHandle(t *testing.T) {
	jobs, d, _ := queueForTest(t, "crash")
	h := receiveHandle(t, jobs)
	defer h.Close(context.Background())
	select {
	case event := <-h.Events():
		if event.Type != p.LeaseLost || !errors.Is(event.Err, p.ErrLeaseLost) {
			t.Fatalf("lost lease: %+v", event)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("queue loss did not cancel job")
	}
	d.mu.Lock()
	used := d.used
	d.mu.Unlock()
	if used != 1 {
		t.Fatalf("queue loss released an execution reservation early: %d", used)
	}
	h.Close(t.Context())
	assertReleased(t, d)
}
func TestQueueUndeliveredCleanup(t *testing.T) {
	jobs, d, cancel := queueForTest(t, "")
	deadline := time.Now().Add(10 * time.Second)
	for {
		d.mu.Lock()
		used := d.used
		d.mu.Unlock()
		if used != 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("queue did not reserve")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	// Keep the delivery blocked until cancellation has released its reservation.
	// Receiving immediately would race the send against acquisition.Done().
	assertReleased(t, d)
	select {
	case _, ok := <-jobs:
		if ok {
			t.Fatal("canceled acquisition delivered a handle")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("undelivered cleanup stalled")
	}
}

type rpcRun struct {
	done     chan error
	finished chan struct{}
	once     sync.Once
	result   p.Result
	data     []byte
	mode     string
}

func (r *rpcRun) finish(result p.Result) {
	r.once.Do(func() { r.result = result; close(r.finished); r.done <- nil; close(r.done) })
}
func (r *rpcRun) Done() <-chan error { return r.done }
func (r *rpcRun) Result() (p.Result, []byte) {
	select {
	case <-r.finished:
		return r.result, r.data
	default:
		return p.Unknown, nil
	}
}
func (r *rpcRun) Stop(context.Context, p.StopReason) error {
	if r.mode == "race" {
		r.finish(p.Succeeded)
		return os.ErrProcessDone
	}
	r.finish(p.Canceled)
	return nil
}

type taskStream struct {
	ctx   context.Context
	start *p.TaskCommand
}

func (s *taskStream) Context() context.Context { return s.ctx }
func (s *taskStream) Recv() (*p.TaskCommand, error) {
	if s.start != nil {
		command := s.start
		s.start = nil
		return command, nil
	}
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}
func (s *taskStream) Send(*p.TaskEvent) error { return nil }

func TestWorkerRejectsSecondExecution(t *testing.T) {
	server := &p.WorkerClient{Start: func(context.Context, *p.TaskStart) (p.Run, error) {
		r := &rpcRun{done: make(chan error, 1), finished: make(chan struct{})}
		r.finish(p.Succeeded)
		return r, nil
	}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stream := &taskStream{ctx: ctx, start: &p.TaskCommand{Command: &p.TaskCommand_Start{Start: &p.TaskStart{}}}}
	if err := server.Execute(stream); err != nil {
		t.Fatal(err)
	}
	if err := server.Execute(stream); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("second execution was accepted: %v", err)
	}
}

func testWorker(t *testing.T, mode string) (*p.WorkerSession, context.Context) {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	child, err := p.LaunchWorker(ctx, runtime.Command{Path: self, Args: []string{mode}}, &runtime.Pool{})
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

func TestWorkerStatusAndIdleShutdown(t *testing.T) {
	for _, reason := range []p.StopReason{p.StopCancel, p.StopShutdown, p.StopForce} {
		t.Run(string(rune('0'+reason)), func(t *testing.T) {
			child, ctx := testWorker(t, "worker-child")
			if err := child.Shutdown(ctx, p.StopForce+1); !errors.Is(err, p.ErrInvalidStopReason) {
				t.Fatalf("invalid shutdown = %v", err)
			}
			// Requests share a stream but each caller must receive its own reply.
			var requests sync.WaitGroup
			for range 16 {
				requests.Go(func() {
					if state, err := child.Status(ctx); err != nil || state != p.WorkerState_READY {
						t.Errorf("status = %v, %v", state, err)
					}
				})
			}
			requests.Wait()
			if err := child.Shutdown(ctx, reason); err != nil {
				t.Fatal(err)
			}
			select {
			case <-child.Done():
			case <-ctx.Done():
				t.Fatal("idle shutdown did not exit")
			}
		})
	}
}

func TestWorkerShutdownDuringExecution(t *testing.T) {
	for _, reason := range []p.StopReason{p.StopCancel, p.StopShutdown} {
		t.Run(string(rune('0'+reason)), func(t *testing.T) {
			child, ctx := testWorker(t, "worker-child")
			if err := child.Send(&p.TaskCommand{Command: &p.TaskCommand_Start{Start: &p.TaskStart{Config: []byte("wait")}}}); err != nil {
				t.Fatal(err)
			}
			if state, err := child.Status(ctx); err != nil || state != p.WorkerState_READY {
				t.Fatalf("running status = %v, %v", state, err)
			}
			if err := child.Shutdown(ctx, reason); err != nil {
				t.Fatal(err)
			}
			event, err := child.Recv()
			if err != nil || event.GetOutcome() == nil || p.Result(event.GetOutcome().Result) != p.Canceled {
				t.Fatalf("shutdown outcome = %v, %v", event, err)
			}
			if state, err := child.Status(ctx); err != nil || state != p.WorkerState_STOPPING {
				t.Fatalf("stopping status = %v, %v", state, err)
			}
			select {
			case <-child.Done():
				t.Fatal("worker exited before parent retired the channel")
			default:
			}
		})
	}
}

func TestWorkerParentLoss(t *testing.T) {
	for _, running := range []bool{false, true} {
		t.Run(map[bool]string{false: "idle", true: "running"}[running], func(t *testing.T) {
			child, ctx := testWorker(t, "worker-child")
			if running {
				if err := child.Send(&p.TaskCommand{Command: &p.TaskCommand_Start{Start: &p.TaskStart{Config: []byte("wait")}}}); err != nil {
					t.Fatal(err)
				}
				if _, err := child.Status(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if err := child.Close(); err != nil {
				t.Fatal(err)
			}
			if err := child.Err(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWorkerRejectsReplacementStream(t *testing.T) {
	child, ctx := testWorker(t, "replacement-child")
	if state, err := child.Status(ctx); err != nil || state != p.WorkerState_READY {
		t.Fatalf("replacement canceled original channel: %v, %v", state, err)
	}
	if err := child.Shutdown(ctx, p.StopShutdown); err != nil {
		t.Fatal(err)
	}
}

func TestEarlyChildExitDoesNotWaitForStartupTimeout(t *testing.T) {
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"exit-child", "connected-exit-child"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			child, err := p.LaunchWorker(ctx, runtime.Command{Path: self, Args: []string{mode}}, &runtime.Pool{})
			if child != nil {
				child.Close()
				t.Fatal("exited child was accepted")
			}
			if err == nil || ctx.Err() != nil {
				t.Fatalf("early exit was not observed: %v", err)
			}
		})
	}
}

func TestEarlyQueueExitDoesNotWaitForStartupTimeout(t *testing.T) {
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"exit-child", "connected-exit-child"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			q := &p.ProcessQueue{Command: []string{self, mode}}
			demand := &rpcDemand{changed: make(chan struct{})}
			select {
			case _, ok := <-q.Serve(ctx, demand):
				if ok {
					t.Fatal("exited queue delivered a job")
				}
				if ctx.Err() != nil {
					t.Fatal("queue exit waited for startup timeout")
				}
			case <-ctx.Done():
				t.Fatal("queue exit was not observed")
			}
		})
	}
}

func TestWorkerIPCOutcomeCancellationAndFailure(t *testing.T) {
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"success", "wait", "invalid", "force", "race", "crash"} {
		t.Run(mode, func(t *testing.T) {
			config := mode
			if mode == "force" {
				config = "wait"
			}
			r, err := p.StartWorker(t.Context(), p.ProcessConfig{Command: []string{self, "worker-child"}, Config: []byte(config)}, []byte{0, 255, 128}, runtime.Command{}, &runtime.Pool{})
			if err != nil {
				t.Fatal(err)
			}
			if mode == "wait" || mode == "race" {
				if err := r.Stop(t.Context(), p.StopCancel); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "force" {
				if err := r.Stop(t.Context(), p.StopForce); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case err = <-r.Done():
			case <-time.After(10 * time.Second):
				t.Fatal("worker did not stop")
			}
			result, data := r.Result()
			if mode == "invalid" {
				if !errors.Is(err, p.ErrInvalidConfig) {
					t.Fatalf("lost typed error: %v", err)
				}
				return
			}
			if mode == "force" {
				return
			}
			if mode == "crash" {
				if !errors.Is(err, p.ErrUnavailable) {
					t.Fatalf("worker crash lost: %v", err)
				}
				return
			}
			want := p.Succeeded
			if mode == "wait" {
				want = p.Canceled
			}
			if err != nil || result != want || !bytes.Equal(data, []byte{0, 255, 128}) {
				t.Fatalf("outcome: %v %v %v", result, data, err)
			}
		})
	}
}
