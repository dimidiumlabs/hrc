// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

// Messages describe one monitor generation. Provider payloads remain opaque.
package space

import (
	"context"
	"errors"
	"fmt"

	"dimidiumlabs.io/hrc/internal/provider"
	"dimidiumlabs.io/hrc/internal/slot"
)

const Version = 1

// Only stable public errors cross IPC; provider errors retain their message.
var wireErrors = map[string]error{
	"context.canceled": context.Canceled,
	"context.deadline": context.DeadlineExceeded,

	"slot.busy":                slot.ErrBusy,
	"slot.destroyed":           slot.ErrDestroyed,
	"slot.invalidRun":          slot.ErrInvalidRun,
	"slot.prepare":             slot.ErrPrepare,
	"slot.providerUnavailable": slot.ErrProviderUnavailable,
	"slot.unknown":             slot.ErrUnknown,
	"slot.unknownRun":          slot.ErrUnknownRun,

	"space.closed":        ErrClosed,
	"space.invalidConfig": ErrInvalidConfig,
	"space.unavailable":   ErrUnavailable,
	"space.unknown":       ErrUnknown,

	"provider.closed":            provider.ErrClosed,
	"provider.invalidConfig":     provider.ErrInvalidConfig,
	"provider.invalidJob":        provider.ErrInvalidJob,
	"provider.invalidStopReason": provider.ErrInvalidStopReason,
	"provider.leaseLost":         provider.ErrLeaseLost,
	"provider.unavailable":       provider.ErrUnavailable,
	"provider.unknown":           provider.ErrUnknown,
	"provider.unsupportedJob":    provider.ErrUnsupportedJob,
}

func EncodeError(err error) *WireError {
	if err == nil {
		return nil
	}
	m := &WireError{Message: err.Error()}
	for name, kind := range wireErrors {
		if errors.Is(err, kind) {
			m.Kind = name
			break
		}
	}
	return m
}

func (m *WireError) Err() error {
	if m == nil {
		return nil
	}
	if kind := wireErrors[m.Kind]; kind != nil {
		return fmt.Errorf("%s: %w", m.Message, kind)
	}
	return errors.New(m.Message)
}

func (m *Snapshot) State() State {
	return State{Capacity: m.Capacity, Slots: m.Slots, BusySlots: m.BusySlots, Running: m.Running, Queued: m.Queued}
}
