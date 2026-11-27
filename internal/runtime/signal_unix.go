// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

//go:build unix

package runtime

import (
	"os"
	"syscall"
)

// UserSignal1 returns the first user-defined signal, or nil when unsupported.
func UserSignal1() os.Signal { return syscall.SIGUSR1 }
