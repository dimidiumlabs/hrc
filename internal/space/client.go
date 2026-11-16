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
	"time"

	"dimidiumlabs.io/hrc/internal/provider"
	"google.golang.org/grpc"
)

// Client owns one scheduler connection. Each Run has its own RPC stream;
// there is no application-level dispatch table and no replay after disconnect.
type Client struct {
	conn      *grpc.ClientConn
	rpc       SchedulerClient
	transport net.Conn
	mu        sync.Mutex
	done      chan struct{}
	lost      <-chan struct{}
	err       error
	once      sync.Once
	Capacity  int
}

func NewClient(ctx context.Context, conn net.Conn, config *Config) (*Client, error) {
	watched := &watchedConn{Conn: conn, done: make(chan struct{})}
	connection, err := dial(watched)
	if err != nil {
		conn.Close()
		return nil, err
	}
	c := &Client{conn: connection, rpc: NewSchedulerClient(connection), transport: watched, done: make(chan struct{}), lost: watched.done}
	go func() {
		select {
		case <-watched.done:
			c.abort(net.ErrClosed)
		case <-c.done:
		}
	}()
	ready, err := c.rpc.Init(ctx, &InitRequest{Version: Version, Config: config})
	if err == nil {
		err = ready.Error.Err()
	}
	if err == nil && (ready.Version != Version || ready.Capacity == 0 || ready.Capacity > uint64(^uint(0)>>1)) {
		err = errors.New("invalid scheduler greeting")
	}
	if err != nil {
		c.Close()
		return nil, err
	}
	c.Capacity = int(ready.Capacity)
	return c, nil
}

func (c *Client) Start(ctx context.Context, name string, payload []byte) (provider.Run, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := c.Err(); err != nil {
		return nil, err
	}
	// The caller context limits submission only, not the lifetime of the Run.
	lifetime, cancel := context.WithCancel(context.Background())
	stopSubmission := context.AfterFunc(ctx, cancel)
	defer stopSubmission()
	stream, err := c.rpc.Run(lifetime)
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

func (c *Client) abort(err error) {
	c.once.Do(func() {
		c.mu.Lock()
		c.err = fmt.Errorf("%w: %w", ErrUnavailable, err)
		close(c.done)
		c.mu.Unlock()
		c.conn.Close()
		c.transport.Close()
	})
}
func (c *Client) checkConnection() {
	select {
	case <-c.lost:
		c.abort(net.ErrClosed)
	default:
	}
}
func (c *Client) Done() <-chan struct{} { c.checkConnection(); return c.done }
func (c *Client) Err() error            { c.checkConnection(); c.mu.Lock(); defer c.mu.Unlock(); return c.err }
func (c *Client) Close() error          { c.abort(net.ErrClosed); return nil }
func (c *Client) State(ctx context.Context) (State, error) {
	if err := c.Err(); err != nil {
		return State{}, err
	}
	snapshot, err := c.rpc.Inspect(ctx, &InspectRequest{})
	if err != nil {
		return State{}, err
	}
	return snapshot.State(), nil
}

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
