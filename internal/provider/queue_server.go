// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package provider

import (
	"context"
	"sync"
	"time"

	"net"

	hrcconfig "dimidiumlabs.io/hrc/internal/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// QueueClient adapts a provider's acquisition implementation to one parent.
// New is provider-owned; the adapter never interprets its configuration or jobs.
type QueueClient struct {
	New     func(*QueueStart) (Queue, error)
	mu      sync.Mutex
	started bool
}

type QueueChannel interface {
	Context() context.Context
	Send(*QueueOutput) error
	Recv() (*QueueInput, error)
}

func (s *QueueClient) Connect(ctx context.Context, socket net.Conn) error {
	var connected sync.Once
	conn, err := grpc.NewClient("passthrough:///queue",
		grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry(), grpc.WithIdleTimeout(0),
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
	stream, err := NewQueueServiceClient(conn).Session(ctx)
	if err != nil {
		return err
	}
	if err := stream.Send(&QueueOutput{Event: &QueueOutput_Ready{Ready: true}}); err != nil {
		return err
	}
	err = s.Session(stream)
	if stream.Context().Err() != nil {
		return nil
	}
	return err
}

func (s *QueueClient) Session(stream QueueChannel) error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return status.Error(codes.FailedPrecondition, "queue already initialized")
	}
	s.started = true
	s.mu.Unlock()
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	start := first.GetStart()
	if start == nil {
		return status.Error(codes.InvalidArgument, "expected queue configuration")
	}
	queue, err := s.New(start)
	if sendErr := stream.Send(&QueueOutput{Event: &QueueOutput_Initialized{Initialized: &QueueInitialization{Error: EncodeFailure(err)}}}); sendErr != nil {
		return sendErr
	}
	if err != nil {
		// Keep the initialization error available until the parent retires the
		// connection; exiting immediately could discard the buffered reply.
		<-stream.Context().Done()
		return err
	}
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	acquisition, stop := context.WithCancel(ctx)
	defer stop()
	var writes sync.Mutex
	send := func(event *QueueOutput) error {
		writes.Lock()
		defer writes.Unlock()
		err := stream.Send(event)
		if err != nil {
			cancel()
		}
		return err
	}
	demand := &remoteDemand{ctx: acquisition, send: send, changed: make(chan struct{}), reservations: make(map[uint64]*remoteReservation)}
	type owned struct {
		handle   Handle
		accepted chan struct{}
	}
	handles := make(map[uint64]*owned)
	operations := make(map[uint64]context.CancelFunc)
	var mu sync.Mutex
	var tasks sync.WaitGroup
	// Receiver remains active after StopAcquisition, until all handles are closed.
	received := make(chan error, 1)
	go func() {
		for {
			command, err := stream.Recv()
			if err != nil {
				received <- err
				cancel()
				return
			}
			switch value := command.Command.(type) {
			case *QueueInput_State:
				demand.update(int(value.State.Available))
			case *QueueInput_Reserved:
				demand.grant(value.Reserved)
			case *QueueInput_Accepted:
				mu.Lock()
				h := handles[value.Accepted]
				if h != nil {
					select {
					case <-h.accepted:
					default:
						close(h.accepted)
					}
				}
				mu.Unlock()
			case *QueueInput_StopAcquisition:
				stop()
			case *QueueInput_CancelOperation:
				mu.Lock()
				cancelOperation := operations[value.CancelOperation]
				mu.Unlock()
				if cancelOperation != nil {
					cancelOperation()
				}
			case *QueueInput_Operation:
				op := value.Operation
				if op == nil || op.Request == 0 {
					received <- status.Error(codes.InvalidArgument, "invalid queue operation")
					cancel()
					return
				}
				var operation context.Context
				var cancelOperation context.CancelFunc
				if op.DeadlineUnixNs != 0 {
					operation, cancelOperation = context.WithDeadline(ctx, time.Unix(0, op.DeadlineUnixNs))
				} else {
					operation, cancelOperation = context.WithCancel(ctx)
				}
				mu.Lock()
				if _, duplicate := operations[op.Request]; duplicate {
					mu.Unlock()
					cancelOperation()
					received <- status.Error(codes.InvalidArgument, "duplicate queue operation")
					cancel()
					return
				}
				h := handles[op.Handle]
				// Register before reading another command so an immediate cancel
				// cannot overtake creation of the operation's context.
				operations[op.Request] = cancelOperation
				mu.Unlock()
				tasks.Go(func() {
					defer cancelOperation()
					var err error
					if h == nil {
						err = ErrClosed
					} else if op.Close {
						err = h.handle.Close(operation)
						mu.Lock()
						delete(handles, op.Handle)
						mu.Unlock()
					} else if op.Outcome == nil || op.Outcome.Result > 65535 {
						err = ErrInvalidJob
					} else {
						err = h.handle.Finish(operation, Result(op.Outcome.Result), op.Outcome.Data)
					}
					mu.Lock()
					delete(operations, op.Request)
					mu.Unlock()
					send(&QueueOutput{Event: &QueueOutput_Reply{Reply: &OperationReply{Request: op.Request, Error: EncodeFailure(err)}}})
				})
			default:
				received <- status.Error(codes.InvalidArgument, "invalid queue command")
				cancel()
				return
			}
		}
	}()
	var next uint64
	jobs := queue.Serve(acquisition, demand)
	for handle := range jobs {
		next++
		id := next
		description := handle.Job()
		payload, err := description.Marshal()
		if err != nil {
			cleanupHandle(handle)
			continue
		}
		h := &owned{handle: handle, accepted: make(chan struct{})}
		mu.Lock()
		handles[id] = h
		mu.Unlock()
		if err := send(&QueueOutput{Event: &QueueOutput_Job{Job: &JobOffer{Handle: id, Job: &Description{Id: description.ID(), Provider: description.Type(), Payload: payload}}}}); err != nil {
			continue
		}
		// All queue handles are registered before events are forwarded. Parent ACK
		// marks successful submission, not successful execution preparation.
		tasks.Go(func() {
			select {
			case <-h.accepted:
			case <-ctx.Done():
				return
			}
			for {
				select {
				case event, ok := <-handle.Events():
					if !ok {
						send(&QueueOutput{Event: &QueueOutput_Notification{Notification: &HandleEvent{Handle: id, Type: uint32(NetworkError), Error: EncodeFailure(ErrClosed)}}})
						return
					}
					if send(&QueueOutput{Event: &QueueOutput_Notification{Notification: &HandleEvent{Handle: id, Type: uint32(event.Type), GraceNs: int64(event.Grace), Error: EncodeFailure(event.Err)}}}) != nil {
						return
					}
				case <-ctx.Done():
					return
				}
			}
		})
	}
	send(&QueueOutput{Event: &QueueOutput_AcquisitionStopped{AcquisitionStopped: true}})
	<-ctx.Done()
	stop()
	// Wait until the receiver cannot add more operations before joining tasks.
	err = <-received
	tasks.Wait()
	mu.Lock()
	remaining := make([]Handle, 0, len(handles))
	for _, h := range handles {
		remaining = append(remaining, h.handle)
	}
	mu.Unlock()
	for _, h := range remaining {
		cleanupHandle(h)
	}
	demand.close()
	return err
}
func cleanupHandle(h Handle) {
	ctx, cancel := context.WithTimeout(context.Background(), hrcconfig.JobCleanupTimeout)
	defer cancel()
	h.Finish(ctx, Canceled, nil)
	h.Close(ctx)
}

type remoteDemand struct {
	ctx          context.Context
	send         func(*QueueOutput) error
	mu           sync.Mutex
	next         uint64
	available    int
	changed      chan struct{}
	reservations map[uint64]*remoteReservation
}

func (d *remoteDemand) Reserve() Reservation {
	d.mu.Lock()
	d.next++
	r := &remoteReservation{owner: d, id: d.next, ready: make(chan struct{})}
	d.reservations[r.id] = r
	d.mu.Unlock()
	if err := d.send(&QueueOutput{Event: &QueueOutput_Reserve{Reserve: r.id}}); err != nil {
		r.grant(err)
	}
	return r
}
func (d *remoteDemand) State() (int, <-chan struct{}) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.available, d.changed
}
func (d *remoteDemand) update(available int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.available = available
	close(d.changed)
	d.changed = make(chan struct{})
}
func (d *remoteDemand) grant(reply *ReservationReply) {
	d.mu.Lock()
	r := d.reservations[reply.Id]
	d.mu.Unlock()
	if r != nil {
		r.grant(reply.Error.Err())
	}
}
func (d *remoteDemand) close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, r := range d.reservations {
		r.grant(ErrClosed)
	}
}

type remoteReservation struct {
	owner           *remoteDemand
	id              uint64
	ready           chan struct{}
	once, closeOnce sync.Once
	err, closeErr   error
}

func (r *remoteReservation) grant(err error) { r.once.Do(func() { r.err = err; close(r.ready) }) }
func (r *remoteReservation) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.owner.ctx.Done():
		return r.owner.ctx.Err()
	case <-r.ready:
		return r.err
	}
}
func (r *remoteReservation) Close() error {
	r.closeOnce.Do(func() {
		r.owner.mu.Lock()
		delete(r.owner.reservations, r.id)
		r.owner.mu.Unlock()
		r.closeErr = r.owner.send(&QueueOutput{Event: &QueueOutput_Release{Release: r.id}})
	})
	return r.closeErr
}
