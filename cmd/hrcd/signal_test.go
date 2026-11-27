// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"os"
	"testing"
	"time"

	"dimidiumlabs.io/hrc/internal/runtime"
)

func TestDrainSignal(t *testing.T) {
	drainSignal := runtime.UserSignal1()
	if drainSignal == nil {
		stop := notifyDrain(func() { t.Error("unsupported signal requested drain") })
		stop()
		return
	}
	requested := make(chan struct{}, 1)
	stop := notifyDrain(func() { requested <- struct{}{} })
	defer stop()
	process, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	defer process.Release()
	if err := process.Signal(drainSignal); err != nil {
		t.Fatal(err)
	}
	select {
	case <-requested:
	case <-time.After(5 * time.Second):
		t.Fatal("user signal did not request drain")
	}
}
