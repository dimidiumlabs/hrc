// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package runtime

import (
	"os"
	"os/exec"
	"testing"

	"golang.org/x/sys/windows"
)

// TerminateProcess can return access denied after the OS has terminated the
// child but before exec.Cmd.Wait has published that outcome to the reaper.
func TestChildKillAfterTerminationBeforeReap(t *testing.T) {
	cmd := exec.Command(os.Args[0], "hrc-control-child", "exit")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cmd.Process.Kill()
		cmd.Wait()
	})
	var state uint32
	var waitErr error
	if err := cmd.Process.WithHandle(func(handle uintptr) {
		state, waitErr = windows.WaitForSingleObject(windows.Handle(handle), 5000)
	}); err != nil {
		t.Fatal(err)
	}
	if waitErr != nil || state != windows.WAIT_OBJECT_0 {
		t.Fatalf("wait for termination: state=%d error=%v", state, waitErr)
	}
	child := &Child{cmd: cmd, started: make(chan struct{})}
	close(child.started)
	if err := child.Kill(); err != nil {
		t.Fatalf("Kill reported an already terminated process as a cleanup failure: %v", err)
	}
}
