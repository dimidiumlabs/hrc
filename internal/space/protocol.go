// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

// Messages describe one scheduler generation. Provider payloads remain opaque.
package space

import (
	"context"
	"errors"
	"fmt"

	"dimidiumlabs.io/hrc/internal/provider"
	"dimidiumlabs.io/hrc/internal/slot"
)

const Version = 1
const MaxMessageSize = 64 << 20

// Only stable public errors cross IPC; provider errors retain their message.
var wireErrors = map[string]error{
	"provider.invalidStopReason": provider.ErrInvalidStopReason,
	"provider.invalidConfig":     provider.ErrInvalidConfig, "provider.invalidJob": provider.ErrInvalidJob,
	"provider.unsupportedJob": provider.ErrUnsupportedJob, "provider.closed": provider.ErrClosed,
	"provider.unavailable": provider.ErrUnavailable, "provider.leaseLost": provider.ErrLeaseLost,
	"provider.unknown": provider.ErrUnknown,
	"slot.busy":        slot.ErrBusy, "slot.destroyed": slot.ErrDestroyed,
	"slot.unknownRun": slot.ErrUnknownRun, "slot.providerUnavailable": slot.ErrProviderUnavailable,
	"slot.invalidRun": slot.ErrInvalidRun, "slot.prepare": slot.ErrPrepare, "slot.unknown": slot.ErrUnknown,
	"space.unavailable": ErrUnavailable, "space.closed": ErrClosed,
	"space.invalidConfig": ErrInvalidConfig, "space.unknown": ErrUnknown,
	"context.canceled": context.Canceled, "context.deadline": context.DeadlineExceeded,
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
