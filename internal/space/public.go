// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

// Package space describes an environment and access to its scheduler.
package space

import (
	"context"
	"errors"

	"dimidiumlabs.io/hrc/internal/provider"
)

// ErrUnavailable is a terminal loss of this scheduler generation. Its Runs
// must not be retried automatically.
var (
	ErrUnavailable   = errors.New("space unavailable")
	ErrClosed        = errors.New("space closed")
	ErrInvalidConfig = errors.New("invalid space configuration")
	ErrUnknown       = errors.New("unknown space error")
)

type Resources struct {
	CPU    float64
	Memory uint64
}

type Status uint16

const (
	Idle Status = iota
	Starting
	Ready
	Destroying
	Destroyed
	Failed
)

// State is a point-in-time snapshot, not a reservation of capacity.
type State struct {
	Capacity, Slots uint64

	// BusySlots includes capacity reserved for slots still being created.
	BusySlots uint64

	// Running includes Runs being stopped. Queued counts requests waiting for a slot.
	Running, Queued uint64
}

// Space owns the environment and scheduler process. Run ensures readiness
// and submits a provider Job. The returned Run can still be waiting for a slot.
// Destroy stops all execution. Connections and IPC are implementation details.
type Space interface {
	Type() string
	Name() string

	// Cwd is the space's local directory on the controller host.
	Cwd() string
	Status() Status
	State(context.Context) (State, error)

	Run(context.Context, provider.Job) (provider.Run, error)
	Destroy() error
}
