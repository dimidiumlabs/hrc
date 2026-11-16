// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package space

import (
	"context"
	"errors"
	"net"
	"sync"

	"dimidiumlabs.io/hrc/internal/provider"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server adapts the scheduler to RPC. Execute owns a Run until it and its cleanup
// finish, and must stop it when ctx is canceled. Serve waits for all handlers.
type Server struct {
	Configure func(*Config) error
	Execute   func(ctx context.Context, name string, payload []byte, stops <-chan provider.StopReason) (provider.Result, []byte, error)
	State     func() State

	mu          sync.RWMutex
	initialized bool
	attempted   bool
}

func (s *Server) Serve(ctx context.Context, conn net.Conn) error {
	server := grpc.NewServer(grpc.MaxRecvMsgSize(MaxMessageSize), grpc.MaxSendMsgSize(MaxMessageSize), grpc.WaitForHandlers(true))
	RegisterSchedulerServer(server, s)
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

func (s *Server) Init(_ context.Context, request *InitRequest) (*InitResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.attempted {
		return nil, status.Error(codes.FailedPrecondition, "scheduler already initialized")
	}
	s.attempted = true
	cfg := request.GetConfig()
	if request.GetVersion() != Version || cfg == nil || cfg.Capacity == 0 || cfg.Capacity > uint64(^uint(0)>>1) {
		return &InitResponse{Version: Version, Error: EncodeError(ErrInvalidConfig)}, nil
	}
	if err := s.Configure(cfg); err != nil {
		return &InitResponse{Version: Version, Error: EncodeError(err)}, nil
	}
	s.initialized = true
	return &InitResponse{Version: Version, Capacity: cfg.Capacity}, nil
}

func (s *Server) ready() bool { s.mu.RLock(); defer s.mu.RUnlock(); return s.initialized }

func (s *Server) Inspect(context.Context, *InspectRequest) (*Snapshot, error) {
	if !s.ready() {
		return nil, status.Error(codes.FailedPrecondition, "scheduler is not initialized")
	}
	state := s.State()
	return &Snapshot{Capacity: state.Capacity, Slots: state.Slots, BusySlots: state.BusySlots, Running: state.Running, Queued: state.Queued}, nil
}

func (s *Server) Run(stream grpc.BidiStreamingServer[RunCommand, RunEvent]) error {
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
			case RunStopReason_CANCEL:
				reason = provider.StopCancel
			case RunStopReason_SHUTDOWN:
				reason = provider.StopShutdown
			case RunStopReason_FORCE:
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
		return stream.Send(&RunEvent{Event: &RunEvent_FinishError{FinishError: &FinishError{Error: EncodeError(err)}}})
	}
	return stream.Send(&RunEvent{Event: &RunEvent_FinishResult{FinishResult: &FinishResult{Result: uint32(result), Data: data}}})
}
