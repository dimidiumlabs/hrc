// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package space

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"dimidiumlabs.io/hrc/internal/provider"
	"google.golang.org/grpc"
)

// Daemon owns lifecycle status and successive scheduler connections. Its zero
// value is idle. Each Run stays with its original connection and is never replayed.
// The owner must serialize Connect, Close and lifecycle SetStatus calls.
type Daemon struct {
	state atomic.Pointer[daemonState]

	// Delegate is configured before Connect and notified after transport loss.
	Delegate DaemonDelegate

	// Capacity is the initial greeting's capacity when constructed by NewDaemon.
	// State reports the capacity of the current scheduler after reconnection.
	Capacity int
}

var _ io.Closer = (*Daemon)(nil)

// DaemonDelegate owns cleanup of the environment after a connection is lost.
// Disconnected runs outside connection teardown and may call Daemon.Close.
// The generation's Done channel lets the delegate reject stale notifications.
type DaemonDelegate interface {
	Disconnected(done <-chan struct{})
}

type daemonState struct {
	status     Status
	failure    error
	connection *daemonConnection
	capacity   int
}

type daemonConnection struct {
	owner     *Daemon
	conn      *grpc.ClientConn
	rpc       SchedulerClient
	transport net.Conn
	done      chan struct{}
	once      sync.Once
}

func (c *Daemon) snapshot() daemonState {
	if state := c.state.Load(); state != nil {
		return *state
	}
	return daemonState{}
}

// SetStatus publishes an explicit lifecycle transition and its cause together.
// It neither inspects nor closes the current connection. A late Ready cannot
// overwrite Broken; recovery must first prepare a new generation via Connect.
func (c *Daemon) SetStatus(status Status, failure error) {
	for {
		previous := c.state.Load()
		if status == Ready && previous != nil && previous.status == Broken {
			return
		}
		state := daemonState{status: status, failure: failure}
		if previous != nil {
			state.connection, state.capacity = previous.connection, previous.capacity
		}
		if c.state.CompareAndSwap(previous, &state) {
			return
		}
	}
}

func (c *Daemon) Status() Status { return c.snapshot().status }
func (c *Daemon) Err() error     { return c.snapshot().failure }
func (c *Daemon) Done() <-chan struct{} {
	if connection := c.snapshot().connection; connection != nil {
		return connection.done
	}
	return nil
}

func NewDaemon(ctx context.Context, conn net.Conn, config *Config) (*Daemon, error) {
	c := &Daemon{}
	if err := c.Connect(ctx, conn, config); err != nil {
		return nil, err
	}
	c.SetStatus(Ready, nil)
	if err := c.Err(); err != nil {
		c.Close()
		return nil, err
	}
	c.Capacity = c.snapshot().capacity
	return c, nil
}

// Connect replaces the scheduler connection and performs Init. On success it
// remains Starting; the owner publishes Ready after completing its own startup.
// The previous connection is closed before installing a new one.
func (c *Daemon) Connect(ctx context.Context, conn net.Conn, config *Config) error {
	c.Close()
	c.SetStatus(Starting, nil)
	watched := &watchedConn{Conn: conn, done: make(chan struct{})}
	connection, err := dial(watched)
	if err != nil {
		conn.Close()
		c.SetStatus(Broken, fmt.Errorf("%w: %w", ErrUnavailable, err))
		return err
	}
	generation := &daemonConnection{owner: c, conn: connection, rpc: NewSchedulerClient(connection), transport: watched, done: make(chan struct{})}
	initial := &daemonState{status: Starting, connection: generation}
	c.state.Store(initial)
	delegate := c.Delegate
	go func() {
		select {
		case <-watched.done:
			generation.abort(net.ErrClosed)
			if delegate != nil {
				delegate.Disconnected(generation.done)
			}
		case <-generation.done:
		}
	}()
	ready, err := generation.rpc.Init(ctx, &InitRequest{Version: Version, Config: config})
	if err == nil {
		err = ready.Error.Err()
	}
	if err == nil && (ready.Version != Version || ready.Capacity == 0 || ready.Capacity > uint64(^uint(0)>>1)) {
		err = errors.New("invalid scheduler greeting")
	}
	if err != nil {
		generation.abort(err)
		return err
	}
	state := &daemonState{status: Starting, connection: generation, capacity: int(ready.Capacity)}
	if !c.state.CompareAndSwap(initial, state) {
		// Loss during Init must not be overwritten by its late response.
		generation.abort(net.ErrClosed)
		if failure := c.Err(); failure != nil {
			return failure
		}
		return ErrUnavailable
	}
	return nil
}

func (c *Daemon) Start(ctx context.Context, name string, payload []byte) (provider.Run, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	state := c.snapshot()
	if state.status != Ready {
		if state.failure != nil {
			return nil, state.failure
		}
		return nil, ErrUnavailable
	}
	// The caller context limits submission only, not the lifetime of the Run.
	lifetime, cancel := context.WithCancel(context.Background())
	stopSubmission := context.AfterFunc(ctx, cancel)
	defer stopSubmission()
	stream, err := state.connection.rpc.Run(lifetime)
	if err != nil {
		cancel()
		return nil, err
	}
	r := &run{stream: stream, cancel: cancel, writes: make(chan struct{}, 1), done: make(chan error, 1), finished: make(chan struct{})}
	r.writes <- struct{}{}
	go r.read()
	if err := r.send(ctx, &RunCommand{Command: &RunCommand_Start{Start: &StartRequest{Provider: name, Payload: payload}}}); err != nil {
		cancel()
		return nil, err
	}
	return r, nil
}

func (g *daemonConnection) abort(err error) {
	g.once.Do(func() {
		failure := fmt.Errorf("%w: %w", ErrUnavailable, err)
		for {
			previous := g.owner.state.Load()
			if previous == nil || previous.connection != g || (previous.status != Starting && previous.status != Ready) {
				break
			}
			state := *previous
			state.status, state.failure = Broken, failure
			if g.owner.state.CompareAndSwap(previous, &state) {
				break
			}
		}
		close(g.done)
		g.conn.Close()
		g.transport.Close()
	})
}

func (c *Daemon) Close() error {
	if connection := c.snapshot().connection; connection != nil {
		connection.abort(net.ErrClosed)
	}
	return nil
}

func (c *Daemon) State(ctx context.Context) (State, error) {
	state := c.snapshot()
	switch state.status {
	case Broken:
		return State{}, state.failure
	case Ready:
		snapshot, err := state.connection.rpc.Inspect(ctx, &InspectRequest{})
		if err != nil {
			return State{}, err
		}
		return snapshot.State(), nil
	default:
		return State{}, nil
	}
}

var _ provider.Run = (*run)(nil)

type run struct {
	stream   grpc.BidiStreamingClient[RunCommand, RunEvent]
	cancel   context.CancelFunc
	writes   chan struct{}
	done     chan error
	finished chan struct{}
	result   provider.Result
	data     []byte
	err      error
}

func (r *run) read() {
	defer r.cancel()
	r.result = provider.Unknown
	event, err := r.stream.Recv()
	if err == nil {
		switch {
		case event.GetFinishError().GetError() != nil:
			r.err = event.GetFinishError().Error.Err()
		case event.GetFinishResult() != nil && event.GetFinishResult().Result <= 65535:
			result := event.GetFinishResult()
			r.result, r.data = provider.Result(result.Result), result.Data
		default:
			err = errors.New("invalid Run event")
		}
	}
	if err != nil {
		r.err = fmt.Errorf("%w: %w", ErrUnavailable, err)
	}

	close(r.finished)
	r.done <- r.err
	close(r.done)
}
func (r *run) Done() <-chan error { return r.done }
func (r *run) Result() (provider.Result, []byte) {
	select {
	case <-r.finished:
		return r.result, r.data
	default:
		return provider.Unknown, nil
	}
}

func (r *run) send(ctx context.Context, command *RunCommand) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.finished:
		return nil
	case <-r.writes:
	}
	defer func() { r.writes <- struct{}{} }()
	if err := ctx.Err(); err != nil {
		return err
	}
	stop := context.AfterFunc(ctx, r.cancel)
	defer stop()
	err := r.stream.Send(command)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, io.EOF) {
		// Send can see trailers before Recv consumes the final result. Preserve it.
		select {
		case <-r.finished:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err != nil {
		select {
		case <-r.finished:
			return nil
		default:
		}
		r.cancel()
	}
	return err
}
func (r *run) Stop(ctx context.Context, reason provider.StopReason) error {
	var kind RunStopReason
	switch reason {
	case provider.StopCancel:
		kind = RunStopReason_CANCEL
	case provider.StopShutdown:
		kind = RunStopReason_SHUTDOWN
	case provider.StopForce:
		kind = RunStopReason_FORCE
	default:
		return provider.ErrInvalidStopReason
	}
	if err := r.send(ctx, &RunCommand{Command: &RunCommand_Stop{Stop: &StopRequest{Reason: kind}}}); err != nil {
		return err
	}
	if reason != provider.StopForce {
		return nil
	}
	select {
	case <-r.finished:
		if errors.Is(r.err, ErrUnavailable) {
			return r.err
		}
		return nil
	case <-ctx.Done():
		r.cancel()
		return fmt.Errorf("%w: %w", ErrUnavailable, ctx.Err())
	}
}
