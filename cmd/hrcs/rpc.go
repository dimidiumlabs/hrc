// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"context"
	"errors"
	"net"
	"sync"

	"dimidiumlabs.io/hrc/internal/provider"
	"dimidiumlabs.io/hrc/internal/space"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Scheduler adapts execution to RPC. Execute owns a Run until it and its cleanup
// finish, and must stop it when ctx is canceled. Serve waits for all handlers.
type Scheduler struct {
	Configure func(*space.Config) error
	Execute   func(ctx context.Context, name string, payload []byte, stops <-chan provider.StopReason) (provider.Result, []byte, error)
	State     func() space.State

	mu          sync.RWMutex
	initialized bool
	attempted   bool
}

var _ space.SchedulerServer = (*Scheduler)(nil)

func (s *Scheduler) Serve(ctx context.Context, conn net.Conn) error {
	server := grpc.NewServer(grpc.MaxRecvMsgSize(space.MaxMessageSize), grpc.MaxSendMsgSize(space.MaxMessageSize), grpc.WaitForHandlers(true))
	space.RegisterSchedulerServer(server, s)
	watched := &watchedConn{Conn: conn, done: make(chan struct{})}
	listener := &singleListener{conn: watched, done: make(chan struct{})}
	defer watched.Close()
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	select {
	case <-ctx.Done():
	case <-watched.done:
	case err := <-done:
		server.Stop()
		return err
	}
	// Stop cancels RPC contexts, then waits for Execute to stop/reap every Run.
	server.Stop()
	err := <-done
	if errors.Is(err, grpc.ErrServerStopped) {
		return nil
	}
	return err
}

func (s *Scheduler) Init(_ context.Context, request *space.InitRequest) (*space.InitResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.attempted {
		return nil, status.Error(codes.FailedPrecondition, "scheduler already initialized")
	}
	s.attempted = true
	cfg := request.GetConfig()
	if request.GetVersion() != space.Version || cfg == nil || cfg.Capacity == 0 || cfg.Capacity > uint64(^uint(0)>>1) {
		return &space.InitResponse{Version: space.Version, Error: space.EncodeError(space.ErrInvalidConfig)}, nil
	}
	if err := s.Configure(cfg); err != nil {
		return &space.InitResponse{Version: space.Version, Error: space.EncodeError(err)}, nil
	}
	s.initialized = true
	return &space.InitResponse{Version: space.Version, Capacity: cfg.Capacity}, nil
}

func (s *Scheduler) ready() bool { s.mu.RLock(); defer s.mu.RUnlock(); return s.initialized }

func (s *Scheduler) Inspect(context.Context, *space.InspectRequest) (*space.Snapshot, error) {
	if !s.ready() {
		return nil, status.Error(codes.FailedPrecondition, "scheduler is not initialized")
	}
	state := s.State()
	return &space.Snapshot{Capacity: state.Capacity, Slots: state.Slots, BusySlots: state.BusySlots, Running: state.Running, Queued: state.Queued}, nil
}

func (s *Scheduler) Run(stream grpc.BidiStreamingServer[space.RunCommand, space.RunEvent]) error {
	if !s.ready() {
		return status.Error(codes.FailedPrecondition, "scheduler is not initialized")
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
	result, data, err := s.Execute(ctx, start.Provider, start.Payload, stops)
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
