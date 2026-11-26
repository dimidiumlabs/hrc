// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"

	"dimidiumlabs.io/hrc/internal/provider"
	"dimidiumlabs.io/hrc/internal/resource"
)

func TestFinalizerBoundsWaitingForRunCleanup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		_, _, handle, calls := fakeController(t)
		finalizer := jobFinalization{handle: handle, local: &cleanupRun{Run: handle.local, wedged: true}}
		if err := finalizer.Shutdown(t.Context(), resource.Down); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("run cleanup exceeded its operation budget", err)
		}
		if finalizer.stopped || len(*calls) != 0 {
			t.Fatal("unconfirmed execution cleanup released the handle")
		}
	})
}

func TestFinalizerPreservesSuccessfulStepsAndRetainsFailedOnes(t *testing.T) {
	_, _, base, _ := fakeController(t)
	handle := &finalizationHandle{fakeJob: base, finishFailures: 1, closeFailures: 1}
	finalizer := jobFinalization{handle: handle, result: provider.Succeeded}
	if err := finalizer.Shutdown(t.Context(), resource.Down); !errors.Is(err, provider.ErrUnavailable) || handle.closeCalls != 0 {
		t.Fatal("failed Finish must retain a usable handle", err)
	}
	if err := finalizer.Shutdown(t.Context(), resource.Down); !errors.Is(err, provider.ErrUnavailable) || !finalizer.finished {
		t.Fatal("failed Close must preserve successful Finish", err)
	}
	if err := finalizer.Shutdown(t.Context(), resource.Term); err != nil || handle.finishCalls != 2 || handle.closeCalls != 2 {
		t.Fatalf("successful finalization steps repeated: finish=%d close=%d error=%v", handle.finishCalls, handle.closeCalls, err)
	}
}
