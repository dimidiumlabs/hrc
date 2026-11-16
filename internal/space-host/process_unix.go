// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

//go:build !windows

package spacehost

import (
	"os/exec"
	"syscall"
)

// hrcd translates service/terminal shutdown into Run commands. A terminal
// interrupt must not bypass that grace period by also terminating hrcs.
func configureProcess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}
