// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package scheduleripc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"dimidiumlabs.io/hrc/internal/provider"
	"dimidiumlabs.io/hrc/internal/space"
)

// Client is the space implementation's connection to one scheduler generation;
// loss of that connection fails its handles and never retries a submitted Run.
type Client struct {
	conn     *Conn
	mu       sync.Mutex
	starts   sync.Mutex
	next     uint64
	runs     map[uint64]*run
	queries  map[uint64]chan space.State
	done     chan struct{}
	err      error
	Capacity int
}

func NewClient(ctx context.Context, conn net.Conn, config []byte) (*Client, error) {
	c := &Client{conn: NewConn(conn), runs: make(map[uint64]*run), queries: make(map[uint64]chan space.State), done: make(chan struct{})}
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	if err := c.conn.Send(ctx, Message{Type: Init, Version: Version, Payload: config}); err != nil {
		conn.Close()
		return nil, err
	}
	ready, err := c.conn.Receive()
	if err == nil && ready.Error != "" {
		err = errors.New(ready.Error)
	}
	if err == nil && (ready.Type != Ready || ready.Version != Version || ready.Capacity <= 0) {
		err = errors.New("invalid scheduler greeting")
	}
	if err != nil {
		conn.Close()
		return nil, err
	}
	c.Capacity = ready.Capacity
	go c.read()
	return c, nil
}

func (c *Client) Start(ctx context.Context, name string, payload []byte) (provider.Run, error) {
	c.starts.Lock()
	defer c.starts.Unlock()
	c.mu.Lock()
	if c.err != nil {
		err := c.err
		c.mu.Unlock()
		return nil, err
	}
	c.next++
	r := &run{client: c, id: c.next, done: make(chan error, 1), finished: make(chan struct{})}
	c.runs[r.id] = r
	c.mu.Unlock()
	if err := c.conn.Send(ctx, Message{Type: Submit, ID: r.id, Provider: name, Payload: payload}); err != nil {
		c.abort(err)
		return nil, c.Err()
	}
	return r, nil
}

func (c *Client) read() {
	for {
		m, err := c.conn.Receive()
		if err != nil {
			c.abort(fmt.Errorf("scheduler disconnected: %w", err))
			return
		}
		c.mu.Lock()
		if m.Type == Snapshot {
			if reply := c.queries[m.ID]; reply != nil {
				reply <- m.State
				delete(c.queries, m.ID)
			}
			c.mu.Unlock()
			continue
		}
		r := c.runs[m.ID]
		if m.Type != Finished || r == nil {
			c.mu.Unlock()
			c.abort(errors.New("unexpected scheduler event"))
			return
		}
		delete(c.runs, m.ID)
		r.finish(m.Result, m.Data, m.Failure())
		c.mu.Unlock()
	}
}

func (c *Client) abort(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return
	}
	err = fmt.Errorf("%w: %w", space.ErrUnavailable, err)
	c.err = err
	c.conn.Close()
	for _, r := range c.runs {
		r.finish(provider.Unknown, nil, err)
	}
	c.runs = nil
	close(c.done)
}

func (c *Client) Done() <-chan struct{} { return c.done }

func (c *Client) State(ctx context.Context) (space.State, error) {
	c.mu.Lock()
	if c.err != nil {
		err := c.err
		c.mu.Unlock()
		return space.State{}, err
	}
	c.next++
	id := c.next
	reply := make(chan space.State, 1)
	c.queries[id] = reply
	c.mu.Unlock()
	defer func() { c.mu.Lock(); delete(c.queries, id); c.mu.Unlock() }()
	if err := c.conn.Send(ctx, Message{Type: Inspect, ID: id}); err != nil {
		c.abort(err)
		return space.State{}, c.Err()
	}
	select {
	case state := <-reply:
		return state, nil
	case <-ctx.Done():
		return space.State{}, ctx.Err()
	case <-c.done:
		return space.State{}, c.Err()
	}
}
func (c *Client) Err() error { c.mu.Lock(); defer c.mu.Unlock(); return c.err }
func (c *Client) Close() error {
	select {
	case <-c.done:
		return nil
	default:
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = c.conn.Send(ctx, Message{Type: Stop})
	c.abort(net.ErrClosed)
	return nil
}

type run struct {
	client   *Client
	id       uint64
	done     chan error
	finished chan struct{}
	result   provider.Result
	data     []byte
	err      error
	once     sync.Once
}

func (r *run) finish(result provider.Result, data []byte, err error) {
	r.once.Do(func() { r.result, r.data, r.err = result, data, err; close(r.finished); r.done <- err; close(r.done) })
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
func (r *run) command(ctx context.Context, kind Type) error {
	select {
	case <-r.finished:
		return nil
	default:
	}
	if err := r.client.conn.Send(ctx, Message{Type: kind, ID: r.id}); err != nil {
		r.client.abort(err)
		return err
	}
	return nil
}
func (r *run) Stop(ctx context.Context, reason provider.StopReason) error {
	var kind Type
	switch reason {
	case provider.StopCancel:
		kind = Cancel
	case provider.StopShutdown:
		kind = ShutdownRun
	case provider.StopForce:
		kind = Kill
	default:
		return provider.ErrInvalidStopReason
	}
	if err := r.command(ctx, kind); err != nil {
		return err
	}
	if reason != provider.StopForce {
		return nil
	}
	select {
	case <-r.finished:
		if errors.Is(r.err, space.ErrUnavailable) {
			return r.err
		}
		return nil
	case <-ctx.Done():
		r.client.abort(ctx.Err())
		return r.client.Err()
	}
}
