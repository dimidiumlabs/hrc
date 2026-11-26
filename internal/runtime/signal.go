// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package runtime

import (
	"os"
	"syscall"
)

// TerminationSignals includes interactive interrupts and service-manager stops.
// On Windows, Go also reports console close and shutdown events as SIGTERM;
// both Ctrl+C and Ctrl+Break are reported as os.Interrupt.
func TerminationSignals() []os.Signal {
	return []os.Signal{os.Interrupt, syscall.SIGTERM}
}
