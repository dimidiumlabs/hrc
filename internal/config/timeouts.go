// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package config

import "time"

// Process lifecycle limits are shared by the daemon, monitors and providers.
// Equal durations have separate names when they govern different operations.
const (
	// ProcessStartTimeout bounds launch, connection and protocol initialization.
	ProcessStartTimeout = 30 * time.Second
	// ProcessExitTimeout allows a provider child to exit after protocol retirement.
	ProcessExitTimeout = 5 * time.Second
	// ProcessWaitDelay bounds draining inherited stdout/stderr after process exit.
	ProcessWaitDelay = 5 * time.Second
	// ProcessGroupCleanupTimeout bounds termination of a slot's descendants.
	ProcessGroupCleanupTimeout = 5 * time.Second
	ProcessGroupPollInterval   = 10 * time.Millisecond
	// MonitorExitTimeout allows a monitor to stop and clean up its slots.
	MonitorExitTimeout = 30 * time.Second
)

const (
	// ControlTimeout bounds delivery or handling of an execution stop command.
	ControlTimeout = 30 * time.Second
	// QueueStopTimeout bounds acknowledgement that acquisition has stopped.
	QueueStopTimeout = 30 * time.Second
	// JobCancelGrace is used when the provider supplies no cancellation grace.
	JobCancelGrace   = time.Minute
	JobFinishTimeout = time.Minute
	JobCloseTimeout  = 30 * time.Second
	// JobCleanupTimeout bounds fallback cleanup of acquired handles.
	JobCleanupTimeout = time.Minute
	// QueueRetryMin and QueueRetryMax bound acquisition restart backoff.
	QueueRetryMin = 2 * time.Second
	QueueRetryMax = time.Minute
)

const (
	// SecretCommandTimeout is the default, overridable by a secret's timeout field.
	SecretCommandTimeout   = 30 * time.Second
	SecretCommandWaitDelay = time.Second
)
