// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package provider

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	hrcconfig "dimidiumlabs.io/hrc/internal/config"
	"dimidiumlabs.io/hrc/internal/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ProcessQueue owns one queue process per Serve session. Cancellation stops
// acquisition only; the process remains until all delivered handles are closed.
type ProcessQueue struct {
	Pool                    *runtime.Pool
	User                    *runtime.User
	OnError                 func(error)
	Command                 []string
	Config                  []byte
	Instance, Version, Name string
}

// queueProcess owns acquisition and all leases for a single queue generation.
// Its existing StopAcquisition/Operation messages are sufficient for lifecycle.
type queueProcess struct {
	ctx    context.Context
	cancel context.CancelFunc
	server *grpc.Server
	child  *runtime.Child
	ready  chan struct{}
	mu     sync.Mutex
	stream grpc.BidiStreamingServer[QueueOutput, QueueInput]
	once   sync.Once
}

func launchQueue(ctx context.Context, command []string, pool *runtime.Pool, user *runtime.User) (*queueProcess, error) {
	if len(command) == 0 {
		return nil, errors.New("queue command is required")
	}
	startup, stop := context.WithTimeout(ctx, hrcconfig.ProcessStartTimeout)
	defer stop()
	lifetime, cancel := context.WithCancel(context.Background())
	p := &queueProcess{ctx: lifetime, cancel: cancel, ready: make(chan struct{}),
		server: grpc.NewServer(grpc.MaxRecvMsgSize(hrcconfig.GRPCMaxMessageSize), grpc.MaxSendMsgSize(hrcconfig.GRPCMaxMessageSize))}
	RegisterQueueServiceServer(p.server, p)
	context.AfterFunc(lifetime, p.server.Stop)
	var err error
	spec := runtime.Command{Path: command[0], Args: command[1:], Stdout: os.Stdout, Stderr: os.Stderr, User: user}
	if user != nil {
		spec.Dir = user.HomeDir
		spec.Env = user.Environ()
	}
	p.child, err = pool.Start(startup, runtime.ChildSpec{Command: spec})
	if err != nil {
		cancel()
		return nil, err
	}
	go p.server.Serve(p.child.Connection().Listener())
	select {
	case <-p.ready:
		return p, nil
	case <-p.child.Done():
		p.Close()
		return nil, errors.Join(ErrUnavailable, p.child.Err())
	case <-startup.Done():
		p.Close()
		return nil, startup.Err()
	case <-p.ctx.Done():
		p.Close()
		return nil, ErrUnavailable
	}
}
func (p *queueProcess) Session(stream grpc.BidiStreamingServer[QueueOutput, QueueInput]) error {
	ready, err := stream.Recv()
	if err != nil {
		p.cancel()
		return err
	}
	if !ready.GetReady() {
		p.cancel()
		return status.Error(codes.InvalidArgument, "expected queue ready")
	}
	p.mu.Lock()
	if p.stream != nil {
		p.mu.Unlock()
		return status.Error(codes.FailedPrecondition, "queue already connected")
	}
	p.stream = stream
	close(p.ready)
	p.mu.Unlock()
	defer p.cancel()
	select {
	case <-stream.Context().Done():
		return stream.Context().Err()
	case <-p.ctx.Done():
		return p.ctx.Err()
	}
}
func (p *queueProcess) Close() {
	p.once.Do(func() {
		p.cancel()
		p.server.Stop()
		p.child.Close(hrcconfig.ProcessExitTimeout)
	})
}

func (q *ProcessQueue) Serve(ctx context.Context, demand Demand) <-chan Handle {
	jobs := make(chan Handle)
	pool := q.Pool
	if pool == nil {
		pool = &runtime.Pool{}
	}
	go func() {
		startup, stop := context.WithTimeout(ctx, hrcconfig.ProcessStartTimeout)
		defer stop()
		child, err := launchQueue(startup, q.Command, pool, q.User)
		if err != nil {
			slog.Error("Start provider queue", "err", err)
			close(jobs)
			return
		}
		stream := child.stream
		cancel := child.cancel
		lifetime := child.ctx
		stopStartup := context.AfterFunc(startup, cancel)
		c := &queueConnection{stream: stream, cancel: cancel, done: make(chan struct{}), handles: make(map[uint64]*remoteHandle), pending: make(map[uint64]chan error), reservations: make(map[uint64]Reservation)}
		if err := c.send(&QueueInput{Command: &QueueInput_Start{Start: &QueueStart{Config: q.Config, Instance: q.Instance, Version: q.Version, Name: q.Name}}}); err != nil {
			stopStartup()
			cancel()
			child.Close()
			close(jobs)
			return
		}
		initialized, err := stream.Recv()
		if err == nil {
			if initialized.GetInitialized() == nil {
				err = fmt.Errorf("%w: expected queue initialization", ErrUnavailable)
			} else {
				err = initialized.GetInitialized().Error.Err()
			}
		}
		stopStartup()
		if err != nil {
			if errors.Is(err, ErrInvalidConfig) {
				if q.OnError != nil {
					q.OnError(fmt.Errorf("forge %q: %w", q.Name, err))
				}
			} else if ctx.Err() == nil {
				slog.Error("Initialize provider queue", "queue", q.Name, "err", err)
			}
			child.Close()
			close(jobs)
			return
		}
		stop()
		var deliveries sync.WaitGroup
		acquisition, stop := context.WithCancel(ctx)
		defer stop()
		acquisitionDone := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				// A wedged child must not hold daemon shutdown forever.
				shutdown, cancel := context.WithTimeout(context.Background(), hrcconfig.QueueStopTimeout)
				abort := context.AfterFunc(shutdown, c.cancel)
				defer func() { abort(); cancel() }()
				c.send(&QueueInput{Command: &QueueInput_StopAcquisition{StopAcquisition: true}})
				select {
				case <-acquisitionDone:
				case <-c.done:
				case <-shutdown.Done():
				}
			case <-c.done:
			}
		}()
		go func() {
			for {
				available, changed := demand.State()
				if c.send(&QueueInput{Command: &QueueInput_State{State: &DemandState{Available: uint64(max(available, 0))}}}) != nil {
					return
				}
				select {
				case <-changed:
				case <-c.done:
					return
				}
			}
		}()
		acquisitionStopped := false
		for {
			event, err := stream.Recv()
			if err != nil {
				if lifetime.Err() == nil {
					slog.Error("Provider queue disconnected", "queue", q.Name, "err", err)
				}
				c.fail(fmt.Errorf("%w: queue connection lost: %v", ErrLeaseLost, err))
				break
			}
			switch value := event.Event.(type) {
			case *QueueOutput_Reserve:
				c.mu.Lock()
				_, duplicate := c.reservations[value.Reserve]
				c.mu.Unlock()
				if value.Reserve == 0 || duplicate {
					cancel()
					continue
				}
				reservation := demand.Reserve()
				c.mu.Lock()
				c.reservations[value.Reserve] = reservation
				c.mu.Unlock()
				deliveries.Go(func() {
					err := reservation.Wait(acquisition)
					c.send(&QueueInput{Command: &QueueInput_Reserved{Reserved: &ReservationReply{Id: value.Reserve, Error: EncodeFailure(err)}}})
				})
			case *QueueOutput_Release:
				c.mu.Lock()
				reservation := c.reservations[value.Release]
				delete(c.reservations, value.Release)
				c.mu.Unlock()
				if reservation != nil {
					reservation.Close()
				}
			case *QueueOutput_Job:
				offer := value.Job
				if offer == nil || offer.Job == nil || offer.Handle == 0 || offer.Job.Provider == "" {
					cancel()
					continue
				}
				c.mu.Lock()
				_, duplicate := c.handles[offer.Handle]
				c.mu.Unlock()
				if duplicate {
					cancel()
					continue
				}
				h := &remoteHandle{owner: c, id: offer.Handle, job: &OpaqueJob{Key: offer.Job.Id, Provider: offer.Job.Provider, Payload: offer.Job.Payload}, events: make(chan Event), closed: make(chan struct{}), changed: make(chan struct{}, 1)}
				c.mu.Lock()
				c.handles[h.id] = h
				c.mu.Unlock()
				go h.forwardEvents()
				deliveries.Go(func() {
					select {
					case jobs <- h:
						if err := c.send(&QueueInput{Command: &QueueInput_Accepted{Accepted: h.id}}); err != nil {
							cancel()
						}
					case <-acquisition.Done():
						cleanup, cancel := context.WithTimeout(context.Background(), hrcconfig.JobCleanupTimeout)
						defer cancel()
						h.Finish(cleanup, Canceled, nil)
						h.Close(cleanup)
					case <-c.done:
						h.Close(context.Background())
					}
				})
			case *QueueOutput_Notification:
				n := value.Notification
				if n == nil || n.Type > uint32(NetworkError) || n.GraceNs < 0 {
					cancel()
					continue
				}
				c.mu.Lock()
				h := c.handles[n.Handle]
				c.mu.Unlock()
				if h != nil {
					h.notify(Event{Type: EventType(n.Type), Grace: time.Duration(n.GraceNs), Err: n.Error.Err()})
				}
			case *QueueOutput_Reply:
				c.mu.Lock()
				reply := c.pending[value.Reply.Request]
				delete(c.pending, value.Reply.Request)
				c.mu.Unlock()
				if reply != nil {
					reply <- value.Reply.Error.Err()
				}
			case *QueueOutput_AcquisitionStopped:
				if !acquisitionStopped {
					acquisitionStopped = true
					close(acquisitionDone)
					go func() { deliveries.Wait(); close(jobs) }()
				}
				c.mu.Lock()
				c.stopped = true
				empty := len(c.handles) == 0
				c.mu.Unlock()
				if empty {
					cancel()
				}
			default:
				cancel()
			}
		}
		stop()
		if !acquisitionStopped {
			deliveries.Wait()
			close(jobs)
		}
		child.Close()
	}()
	return jobs
}

type queueConnection struct {
	stream       grpc.BidiStreamingServer[QueueOutput, QueueInput]
	cancel       context.CancelFunc
	writes       sync.Mutex
	mu           sync.Mutex
	next         uint64
	stopped      bool
	handles      map[uint64]*remoteHandle
	pending      map[uint64]chan error
	reservations map[uint64]Reservation
	done         chan struct{}
	once         sync.Once
}

func (c *queueConnection) send(command *QueueInput) error {
	c.writes.Lock()
	defer c.writes.Unlock()
	return c.stream.Send(command)
}
func (c *queueConnection) fail(err error) {
	c.once.Do(func() {
		c.cancel()
		close(c.done)
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, reply := range c.pending {
			reply <- err
		}
		c.pending = make(map[uint64]chan error)
		// A lost queue cannot release capacity still occupied by executions.
		// Reservation-to-handle association is provider-owned, so conservatively
		// retain this session's reservations until its last handle is closed.
		if len(c.handles) == 0 {
			c.releaseReservations()
		}
		for _, h := range c.handles {
			h.notify(Event{Type: LeaseLost, Err: err})
		}
	})
}

// The caller holds c.mu. Used only after supervision is lost.
func (c *queueConnection) releaseReservations() {
	for _, r := range c.reservations {
		r.Close()
	}
	c.reservations = make(map[uint64]Reservation)
}

func (c *queueConnection) operation(ctx context.Context, op *HandleOperation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok {
		op.DeadlineUnixNs = deadline.UnixNano()
	}
	reply := make(chan error, 1)
	c.mu.Lock()
	c.next++
	op.Request = c.next
	c.pending[op.Request] = reply
	c.mu.Unlock()
	defer func() { c.mu.Lock(); delete(c.pending, op.Request); c.mu.Unlock() }()
	select {
	case <-c.done:
		return ErrClosed
	default:
	}
	// Sending can block behind transport backpressure. Keep the caller's wait
	// bounded without closing a session that also owns unrelated leases.
	sent := make(chan struct{})
	go func() {
		defer close(sent)
		if err := c.send(&QueueInput{Command: &QueueInput_Operation{Operation: op}}); err != nil {
			c.cancel() // A transport failure, unlike an operation timeout, is fatal.
		}
	}()
	select {
	case err := <-reply:
		return err
	case <-ctx.Done():
		go func() {
			// Cancellation must follow its operation on the stream, even if the
			// caller timed out before the original send completed.
			<-sent
			if err := c.send(&QueueInput{Command: &QueueInput_CancelOperation{CancelOperation: op.Request}}); err != nil {
				c.cancel()
			}
		}()
		return ctx.Err()
	case <-c.done:
		return ErrClosed
	}
}

type remoteHandle struct {
	owner         *queueConnection
	id            uint64
	job           Job
	events        chan Event
	closed        chan struct{}
	once          sync.Once
	closeErr      error
	eventMu       sync.Mutex
	notifications []Event
	changed       chan struct{}
}

func (h *remoteHandle) notify(event Event) {
	h.eventMu.Lock()
	h.notifications = append(h.notifications, event)
	h.eventMu.Unlock()
	select {
	case h.changed <- struct{}{}:
	default:
	}
}
func (h *remoteHandle) forwardEvents() {
	defer close(h.events)
	for {
		h.eventMu.Lock()
		var event Event
		available := len(h.notifications) > 0
		if available {
			event = h.notifications[0]
			h.notifications = h.notifications[1:]
		}
		h.eventMu.Unlock()
		if available {
			select {
			case h.events <- event:
			case <-h.closed:
				return
			}
			continue
		}
		select {
		case <-h.changed:
		case <-h.closed:
			return
		}
	}
}
func (h *remoteHandle) Job() Job             { return h.job }
func (h *remoteHandle) Events() <-chan Event { return h.events }
func (h *remoteHandle) Finish(ctx context.Context, result Result, data []byte) error {
	return h.owner.operation(ctx, &HandleOperation{Handle: h.id, Outcome: &Outcome{Result: uint32(result), Data: data}})
}
func (h *remoteHandle) Close(ctx context.Context) error {
	h.once.Do(func() {
		h.closeErr = h.owner.operation(ctx, &HandleOperation{Handle: h.id, Close: true})
		close(h.closed)
		h.owner.mu.Lock()
		delete(h.owner.handles, h.id)
		if len(h.owner.handles) == 0 {
			select {
			case <-h.owner.done:
				h.owner.releaseReservations()
			default:
			}
		}
		empty := h.owner.stopped && len(h.owner.handles) == 0
		h.owner.mu.Unlock()
		if empty {
			h.owner.cancel()
		}
	})
	return h.closeErr
}
