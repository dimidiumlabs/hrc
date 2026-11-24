// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

// Package provider separates job acquisition and supervision from execution.
package provider

import (
	"context"
	"errors"
	"time"
)

var (
	ErrInvalidConfig     = errors.New("invalid provider configuration")
	ErrInvalidJob        = errors.New("invalid job")
	ErrUnsupportedJob    = errors.New("unsupported job type")
	ErrClosed            = errors.New("provider handle closed")
	ErrUnavailable       = errors.New("provider unavailable")
	ErrLeaseLost         = errors.New("job lease lost")
	ErrUnknown           = errors.New("unknown provider error")
	ErrInvalidStopReason = errors.New("invalid run stop reason")
)

type Result uint16

const (
	Unknown Result = iota
	Succeeded
	SucceededWithIssues
	Failed
	Canceled
	Skipped
	Abandoned
)

func (r Result) String() string {
	switch r {
	case Succeeded:
		return "succeeded"
	case SucceededWithIssues:
		return "succeededWithIssues"
	case Failed:
		return "failed"
	case Canceled:
		return "canceled"
	case Skipped:
		return "skipped"
	case Abandoned:
		return "abandoned"
	default:
		return "unknown"
	}
}

func (r Result) Valid() bool {
	switch r {
	case Succeeded, SucceededWithIssues, Failed, Canceled, Skipped, Abandoned:
		return true
	default:
		return false
	}
}

type Reservation interface {
	Wait(context.Context) error
	Close() error
}

// Demand describes how many Jobs may be acquired, independently of slots and
// the source's queue protocol. Reserve accounts for pending acquisition before
// Wait blocks; Handle.Close releases it after the Job has been processed.
type Demand interface {
	Reserve() Reservation
	State() (available int, changed <-chan struct{})
}

// Job is a serializable description, independent of acquisition and execution.
// Marshal is repeatable and has no lifecycle side effects. Unmarshal validates
// and replaces the description; only the concrete provider interprets its data.
type Job interface {
	ID() string
	Type() string

	Marshal() ([]byte, error)
	Unmarshal([]byte) error
}

type EventType uint16

const (
	Cancellation EventType = iota
	Shutdown
	LeaseLost
	NetworkError
)

// Event stop execution. A positive Grace overrides the daemon's default;
// provider-specific timeout encodings and adjustments are resolved beforehand.
type Event struct {
	Type  EventType
	Grace time.Duration
	Err   error
}

// Handle owns an acquired Job's queue session in hrcd. Finish reports the
// result and provider-owned data after execution stops. Close releases
// supervision and the acquisition reservation, never the slot. Close is idempotent.
type Handle interface {
	Job() Job
	Events() <-chan Event
	Finish(context.Context, Result, []byte) error
	Close(context.Context) error
}

// Queue delivers acquired jobs within the supplied demand. Receiving from the
// unbuffered stream transfers Handle ownership to the caller. Canceling the
// context stops acquisition and closes the stream after cleaning up undelivered
// Handles. Delivered Handles remain valid and must be finished and closed by
// the caller. The stream may also close without cancellation when the queue
// has no more jobs or cannot continue. This does not invalidate delivered
// Handles. The caller owns queue retries and may invoke Serve again after the
// stream closes; failures of delivered Jobs are reported through Handle.Events.
type Queue interface {
	Serve(context.Context, Demand) <-chan Handle
}

type StopReason uint16

const (
	StopCancel StopReason = iota
	StopShutdown
	StopForce
)

// Run represents one execution, while its owner and environment can outlive it.
// StopCancel and StopShutdown request provider-specific graceful termination.
// StopForce stops and reaps this Run, bounded by context, preserving the slot.
// Done delivers exactly one completion error (possibly nil), then closes.
// The owner must consume that value; multiple receivers would compete for it.
// Result never blocks: it returns Unknown, nil before completion and the stable
// outcome afterwards. Stop is repeatable and does not consume Done.
// Loss of the environment is an error, not proof that execution stopped.
type Run interface {
	Done() <-chan error
	Result() (Result, []byte)

	Stop(context.Context, StopReason) error
}
