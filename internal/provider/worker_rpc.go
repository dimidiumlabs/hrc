// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package provider

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"

	hrcconfig "dimidiumlabs.io/hrc/internal/config"
	"dimidiumlabs.io/hrc/internal/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// WorkerChannel is the worker's side of its single process channel.
type WorkerChannel interface {
	Context() context.Context
	Send(*TaskEvent) error
	Recv() (*TaskCommand, error)
}

type workerClientChannel struct {
	grpc.BidiStreamingClient[TaskEvent, TaskCommand]
	writes            sync.Mutex
	started, stopping bool
}

func (s *workerClientChannel) Send(event *TaskEvent) error {
	s.writes.Lock()
	defer s.writes.Unlock()
	return s.BidiStreamingClient.Send(event)
}
func (s *workerClientChannel) Recv() (*TaskCommand, error) {
	for {
		command, err := s.BidiStreamingClient.Recv()
		if err != nil {
			return nil, err
		}
		request := command.GetControl()
		if request == nil {
			if s.stopping && !s.started {
				return nil, io.EOF
			}
			if command.GetStart() != nil {
				s.started = true
			}
			return command, nil
		}
		reply := &WorkerReply{Id: request.Id, State: WorkerState_READY}
		switch method := request.Method.(type) {
		case *WorkerRequest_Status:
			if !method.Status {
				return nil, status.Error(codes.InvalidArgument, "invalid status request")
			}
		case *WorkerRequest_Shutdown:
			if method.Shutdown.Reason > uint32(StopForce) {
				return nil, ErrInvalidStopReason
			}
			s.stopping = true
			reply.Exited = !s.started
		default:
			return nil, status.Error(codes.InvalidArgument, "invalid worker control")
		}
		if s.stopping {
			reply.State = WorkerState_STOPPING
		}
		if err := s.Send(&TaskEvent{Reply: reply}); err != nil {
			return nil, err
		}
		if request.GetShutdown() != nil && s.started {
			return &TaskCommand{Command: &TaskCommand_Stop{Stop: request.GetShutdown()}}, nil
		}
	}
}

// WorkerClient connects to its parent and permits exactly one execution.
type WorkerClient struct {
	Start   func(context.Context, *TaskStart) (Run, error)
	mu      sync.Mutex
	started bool
}

func (s *WorkerClient) Connect(ctx context.Context, socket net.Conn) error {
	var connected sync.Once
	conn, err := grpc.NewClient("passthrough:///worker",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDisableRetry(), grpc.WithIdleTimeout(0),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			var accepted net.Conn
			connected.Do(func() { accepted = socket })
			if accepted == nil {
				return nil, net.ErrClosed
			}
			return accepted, nil
		}), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(hrcconfig.GRPCMaxMessageSize), grpc.MaxCallSendMsgSize(hrcconfig.GRPCMaxMessageSize)))
	if err != nil {
		socket.Close()
		return err
	}
	defer conn.Close()
	defer socket.Close()
	raw, err := NewWorkerServiceClient(conn).Execute(ctx)
	if err != nil {
		return err
	}
	stream := &workerClientChannel{BidiStreamingClient: raw}
	if err := stream.Send(&TaskEvent{Ready: true}); err != nil {
		return err
	}
	err = s.Execute(stream)
	if err == nil {
		// Parent consumes Outcome before retiring the channel.
		<-raw.Context().Done()
	}
	if raw.Context().Err() != nil || errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

func (s *WorkerClient) Execute(stream WorkerChannel) error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return status.Error(codes.FailedPrecondition, "worker already used")
	}
	s.started = true
	s.mu.Unlock()
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if first.GetStart() == nil {
		return status.Error(codes.InvalidArgument, "expected task start")
	}
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	commands := make(chan StopReason, 1)
	go func() {
		for {
			command, err := stream.Recv()
			if err != nil {
				cancel()
				return
			}
			stop := command.GetStop()
			if stop == nil || stop.Reason > uint32(StopForce) {
				cancel()
				return
			}
			select {
			case commands <- StopReason(stop.Reason):
			case <-ctx.Done():
				return
			}
		}
	}()
	local, err := s.Start(ctx, first.GetStart())
	if err != nil || local == nil {
		if local != nil {
			err = errors.Join(err, local.Stop(context.Background(), StopForce))
		}
		if err == nil {
			err = ErrInvalidJob
		}
		return stream.Send(&TaskEvent{Outcome: &Outcome{Error: EncodeFailure(err)}})
	}
	defer local.Stop(context.Background(), StopForce)
	for {
		select {
		case err := <-local.Done():
			result, data := local.Result()
			return stream.Send(&TaskEvent{Outcome: &Outcome{Result: uint32(result), Data: data, Error: EncodeFailure(err)}})
		case reason := <-commands:
			stop, cancel := context.WithTimeout(context.Background(), hrcconfig.ControlTimeout)
			err := local.Stop(stop, reason)
			cancel()
			if err != nil {
				// A stop can race normal completion. Preserve the provider's
				// completion data rather than reporting completion twice.
				select {
				case completionErr := <-local.Done():
					err = completionErr
				default:
					stopErr := local.Stop(context.Background(), StopForce)
					err = errors.Join(err, stopErr, <-local.Done())
				}
				result, data := local.Result()
				return stream.Send(&TaskEvent{Outcome: &Outcome{Result: uint32(result), Data: data, Error: EncodeFailure(err)}})
			}
		case <-ctx.Done():
			local.Stop(context.Background(), StopForce)
			<-local.Done()
			return ctx.Err()
		}
	}
}

// WorkerSession owns one worker process and its one RPC. Status and Shutdown
// are correlated requests on that channel, not a separate lifecycle service.
type WorkerSession struct {
	ctx       context.Context
	cancel    context.CancelFunc
	server    *grpc.Server
	conn      net.Conn
	child     *runtime.Child
	closeOnce sync.Once
	closeErr  error

	mu      sync.Mutex
	writes  sync.Mutex
	ready   chan struct{}
	stream  grpc.BidiStreamingServer[TaskEvent, TaskCommand]
	events  chan *TaskEvent
	next    uint64
	pending map[uint64]chan *WorkerReply
}

func LaunchWorker(ctx context.Context, command runtime.Command, pool *runtime.Pool) (*WorkerSession, error) {
	if command.Path == "" {
		return nil, errors.New("worker command is required")
	}
	if command.Stdout == nil {
		command.Stdout = os.Stdout
	}
	if command.Stderr == nil {
		command.Stderr = os.Stderr
	}
	startup, stop := context.WithTimeout(ctx, hrcconfig.ProcessStartTimeout)
	defer stop()
	lifetime, cancel := context.WithCancel(context.Background())
	s := &WorkerSession{ctx: lifetime, cancel: cancel, ready: make(chan struct{}), events: make(chan *TaskEvent, 1),
		pending: make(map[uint64]chan *WorkerReply),
		server:  grpc.NewServer(grpc.MaxRecvMsgSize(hrcconfig.GRPCMaxMessageSize), grpc.MaxSendMsgSize(hrcconfig.GRPCMaxMessageSize))}
	RegisterWorkerServiceServer(s.server, s)
	context.AfterFunc(lifetime, s.server.Stop)
	var err error
	s.child, err = pool.Start(startup, runtime.ChildSpec{
		Command: command,
	})
	if err != nil {
		cancel()
		return nil, err
	}
	s.conn = s.child.Connection()
	go s.server.Serve(s.child.Connection().Listener())
	select {
	case <-s.ready:
		return s, nil
	case <-s.child.Done():
		s.Close()
		return nil, errors.Join(ErrUnavailable, s.child.Err())
	case <-startup.Done():
		s.Close()
		return nil, startup.Err()
	case <-s.ctx.Done():
		s.Close()
		return nil, errors.Join(s.ctx.Err(), s.child.Err())
	}
}
func (s *WorkerSession) Execute(stream grpc.BidiStreamingServer[TaskEvent, TaskCommand]) error {
	ready, err := stream.Recv()
	if err != nil {
		s.cancel()
		return err
	}
	if !ready.Ready || ready.Outcome != nil || ready.Reply != nil {
		s.cancel()
		return status.Error(codes.InvalidArgument, "expected worker ready")
	}
	s.mu.Lock()
	if s.stream != nil {
		s.mu.Unlock()
		return status.Error(codes.FailedPrecondition, "worker already connected")
	}
	s.stream = stream
	close(s.ready)
	s.mu.Unlock()
	defer s.cancel()
	for {
		event, err := stream.Recv()
		if err != nil {
			return err
		}
		if event.Reply != nil {
			s.mu.Lock()
			reply := s.pending[event.Reply.Id]
			s.mu.Unlock()
			if reply != nil {
				select {
				case reply <- event.Reply:
				default:
				}
			}
			continue
		}
		select {
		case s.events <- event:
		case <-s.ctx.Done():
			return s.ctx.Err()
		}
	}
}
func (s *WorkerSession) Send(command *TaskCommand) error {
	s.writes.Lock()
	defer s.writes.Unlock()
	if err := s.ctx.Err(); err != nil {
		return err
	}
	return s.stream.Send(command)
}
func (s *WorkerSession) Recv() (*TaskEvent, error) {
	// A peer may close immediately after its terminal event. Preserve that event.
	select {
	case event := <-s.events:
		return event, nil
	default:
	}
	select {
	case event := <-s.events:
		return event, nil
	case <-s.ctx.Done():
		select {
		case event := <-s.events:
			return event, nil
		default:
			return nil, s.ctx.Err()
		}
	}
}
func (s *WorkerSession) request(ctx context.Context, request *WorkerRequest) (*WorkerReply, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	reply := make(chan *WorkerReply, 1)
	s.mu.Lock()
	s.next++
	request.Id = s.next
	s.pending[request.Id] = reply
	s.mu.Unlock()
	defer func() { s.mu.Lock(); delete(s.pending, request.Id); s.mu.Unlock() }()
	stop := context.AfterFunc(ctx, s.cancel)
	err := s.Send(&TaskCommand{Command: &TaskCommand_Control{Control: request}})
	stop()
	if err != nil {
		return nil, err
	}
	select {
	case response := <-reply:
		return response, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.ctx.Done():
		select {
		case response := <-reply:
			return response, nil
		default:
			return nil, s.ctx.Err()
		}
	}
}
func (s *WorkerSession) Status(ctx context.Context) (WorkerState, error) {
	reply, err := s.request(ctx, &WorkerRequest{Method: &WorkerRequest_Status{Status: true}})
	if err != nil {
		return WorkerState_READY, err
	}
	return reply.State, nil
}
func (s *WorkerSession) Shutdown(ctx context.Context, reason StopReason) error {
	if reason > StopForce {
		return ErrInvalidStopReason
	}
	reply, err := s.request(ctx, &WorkerRequest{Method: &WorkerRequest_Shutdown{Shutdown: &TaskStop{Reason: uint32(reason)}}})
	if err == nil && (reply.Exited || reason == StopForce) {
		s.cancel()
	}
	return err
}
func (s *WorkerSession) Done() <-chan struct{} { return s.child.Done() }
func (s *WorkerSession) Err() error            { return s.child.Err() }
func (s *WorkerSession) Close() error {
	s.closeOnce.Do(func() {
		s.cancel()
		s.server.Stop()
		_, s.closeErr = s.child.Close(hrcconfig.ProcessExitTimeout)
	})
	return s.closeErr
}

// StartWorker launches one worker in the caller's execution environment.
func StartWorker(ctx context.Context, config ProcessConfig, payload []byte, command runtime.Command, pool *runtime.Pool) (Run, error) {
	if len(config.Command) == 0 {
		return nil, errors.New("worker command is required")
	}
	command.Path, command.Args = config.Command[0], config.Command[1:]
	child, err := LaunchWorker(ctx, command, pool)
	if err != nil {
		return nil, err
	}
	r := &workerRun{child: child, cancel: child.cancel, done: make(chan error, 1), finished: make(chan struct{}), writes: make(chan struct{}, 1)}
	r.writes <- struct{}{}
	stopped := context.AfterFunc(ctx, child.cancel)
	err = child.Send(&TaskCommand{Command: &TaskCommand_Start{Start: &TaskStart{Config: config.Config, Payload: payload}}})
	stopped()
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		child.Close()
		return nil, err
	}
	go r.read()
	return r, nil
}

type workerRun struct {
	child    *WorkerSession
	cancel   context.CancelFunc
	writes   chan struct{}
	done     chan error
	finished chan struct{}
	result   Result
	data     []byte
	err      error
}

func (r *workerRun) read() {
	event, err := r.child.Recv()
	if err == nil {
		if event.Outcome == nil || event.Outcome.Result > 65535 {
			err = ErrInvalidJob
		} else {
			r.result = Result(event.Outcome.Result)
			r.data = event.Outcome.Data
			r.err = event.Outcome.Error.Err()
		}
	}
	if err != nil {
		r.err = errors.Join(ErrUnavailable, err)
	}
	r.cancel()
	r.err = errors.Join(r.err, r.child.Close(), r.child.Err())
	close(r.finished)
	r.done <- r.err
	close(r.done)
}
func (r *workerRun) Done() <-chan error { return r.done }
func (r *workerRun) Result() (Result, []byte) {
	select {
	case <-r.finished:
		return r.result, r.data
	default:
		return Unknown, nil
	}
}
func (r *workerRun) Stop(ctx context.Context, reason StopReason) error {
	if reason > StopForce {
		return ErrInvalidStopReason
	}
	if reason == StopForce {
		r.cancel()
		select {
		case <-r.finished:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	select {
	case <-r.finished:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-r.writes:
	}
	defer func() { r.writes <- struct{}{} }()
	stop := context.AfterFunc(ctx, r.cancel)
	defer stop()
	err := r.child.Send(&TaskCommand{Command: &TaskCommand_Stop{Stop: &TaskStop{Reason: uint32(reason)}}})
	if errors.Is(err, io.EOF) {
		select {
		case <-r.finished:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}
