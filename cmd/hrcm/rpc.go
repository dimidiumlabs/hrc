// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"context"
	"errors"
	"net"
	"sync"

	hrcconfig "dimidiumlabs.io/hrc/internal/config"
	"dimidiumlabs.io/hrc/internal/provider"
	"dimidiumlabs.io/hrc/internal/runtime"
	"dimidiumlabs.io/hrc/internal/space"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// rpc owns the transport and Init handshake, not the execution environment.
type rpc struct {
	conn      runtime.Connection
	server    *grpc.Server
	done      chan struct{}
	serveErr  error
	closeOnce sync.Once

	mu          sync.Mutex
	attempted   bool
	config      *space.Config
	initErr     error
	initialized chan struct{}
	ready       chan struct{}
	target      rpcMonitor
}

// rpcMonitor is the execution side attached only after a successful handshake.
type rpcMonitor interface {
	state() space.State
	run(context.Context, string, []byte, <-chan provider.StopReason) (provider.Result, []byte, error)
}

var _ space.MonitorServer = (*rpc)(nil)

func newRPC(conn runtime.Connection) *rpc {
	r := &rpc{
		conn: conn, done: make(chan struct{}),
		initialized: make(chan struct{}), ready: make(chan struct{}),
		server: grpc.NewServer(grpc.MaxRecvMsgSize(hrcconfig.GRPCMaxMessageSize), grpc.MaxSendMsgSize(hrcconfig.GRPCMaxMessageSize), grpc.WaitForHandlers(true)),
	}
	space.RegisterMonitorServer(r.server, r)
	go func() {
		r.serveErr = r.server.Serve(conn.Listener())
		close(r.done)
	}()
	return r
}

// Handshake waits for Init before any Monitor or storage is created.
// Init acknowledges the configuration; environment preparation follows it.
func (r *rpc) Handshake(ctx context.Context) error {
	startup, cancel := context.WithTimeout(ctx, hrcconfig.ProcessStartTimeout)
	defer cancel()
	select {
	case <-r.initialized:
		return r.initErr
	case <-startup.Done():
		return startup.Err()
	case <-r.conn.Done():
		return net.ErrClosed
	case <-r.done:
		return r.serveErr
	}
}

func (r *rpc) Close() {
	r.closeOnce.Do(func() {
		r.server.Stop()
		r.conn.Close()
		<-r.done
	})
}

func (r *rpc) serve(ctx context.Context, target rpcMonitor) error {
	r.mu.Lock()
	r.target = target
	close(r.ready)
	r.mu.Unlock()
	select {
	case <-ctx.Done():
	case <-r.conn.Done():
	case <-r.done:
	}
	// Stop cancels RPC contexts and waits for every Run to stop before cleanup.
	r.Close()
	if errors.Is(r.serveErr, grpc.ErrServerStopped) {
		return nil
	}
	return r.serveErr
}

func (r *rpc) Init(_ context.Context, request *space.InitRequest) (*space.InitResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.attempted {
		return nil, status.Error(codes.FailedPrecondition, "monitor already initialized")
	}
	r.attempted = true
	cfg := request.GetConfig()
	if request.GetVersion() != space.Version || cfg == nil || cfg.Capacity == 0 || cfg.Capacity > uint64(^uint(0)>>1) {
		r.initErr = space.ErrInvalidConfig
	} else {
		r.config = cfg
	}
	close(r.initialized)
	return &space.InitResponse{Version: space.Version, Capacity: cfg.GetCapacity(), Error: space.EncodeError(r.initErr)}, nil
}

func (r *rpc) monitor(ctx context.Context) (rpcMonitor, error) {
	r.mu.Lock()
	initialized := r.config != nil
	r.mu.Unlock()
	if !initialized {
		return nil, status.Error(codes.FailedPrecondition, "monitor is not initialized")
	}
	// Init can be acknowledged before the monitor finishes preparation.
	select {
	case <-r.ready:
		return r.target, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (r *rpc) Inspect(ctx context.Context, _ *space.InspectRequest) (*space.Snapshot, error) {
	target, err := r.monitor(ctx)
	if err != nil {
		return nil, err
	}
	state := target.state()
	return &space.Snapshot{Capacity: state.Capacity, Slots: state.Slots, BusySlots: state.BusySlots, Running: state.Running, Queued: state.Queued}, nil
}

func (s *rpc) Run(stream grpc.BidiStreamingServer[space.RunCommand, space.RunEvent]) error {
	target, err := s.monitor(stream.Context())
	if err != nil {
		return err
	}
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	start := first.GetStart()
	if start == nil {
		return status.Error(codes.InvalidArgument, "Run must start with a Job")
	}
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	stops := make(chan provider.StopReason, 1)
	receiveErr := make(chan error, 1)
	go func() {
		for {
			command, err := stream.Recv()
			if err != nil {
				receiveErr <- err
				cancel()
				return
			}
			stop := command.GetStop()
			var reason provider.StopReason
			switch stop.GetReason() {
			case space.RunStopReason_CANCEL:
				reason = provider.StopCancel
			case space.RunStopReason_SHUTDOWN:
				reason = provider.StopShutdown
			case space.RunStopReason_FORCE:
				reason = provider.StopForce
			default:
				receiveErr <- status.Error(codes.InvalidArgument, "expected a valid Stop command")
				cancel()
				return
			}
			select {
			case stops <- reason:
			case <-ctx.Done():
				return
			}
		}
	}()
	result, data, err := target.run(ctx, start.Provider, start.Payload, stops)
	select {
	case readErr := <-receiveErr:
		return readErr
	default:
	}
	if err != nil {
		return stream.Send(&space.RunEvent{Event: &space.RunEvent_FinishError{FinishError: &space.FinishError{Error: space.EncodeError(err)}}})
	}
	return stream.Send(&space.RunEvent{Event: &space.RunEvent_FinishResult{FinishResult: &space.FinishResult{Result: uint32(result), Data: data}}})
}
