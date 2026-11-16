// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

// Package slot describes a reusable environment for executing provider Runs.
package slot

import (
	"context"
	"errors"

	"dimidiumlabs.io/hrc/internal/provider"
)

type Status uint16

const (
	Idle Status = iota
	Starting
	Running
	Stopping
	Destroying
	Destroyed
)

var (
	ErrBusy                = errors.New("slot is busy")
	ErrDestroyed           = errors.New("slot destroyed")
	ErrUnknownRun          = errors.New("run does not belong to slot")
	ErrProviderUnavailable = errors.New("provider unavailable in slot")
	ErrInvalidRun          = errors.New("provider returned no Run")
	ErrPrepare             = errors.New("slot preparation failed")
	ErrUnknown             = errors.New("unknown slot error")
)

type Slot interface {
	Type() string
	Name() string

	Cwd() string
	Status() Status

	// Run executes a provider Job in this slot.
	// Its context limits preparation, not the lifetime of the resulting Run.
	Run(context.Context, provider.Job) (provider.Run, error)
	// Stop targets this exact Run, waits for termination, and preserves the slot.
	Stop(context.Context, provider.Run) error

	// Destroy stops execution and removes the environment and its contents.
	Destroy(context.Context) error
}
