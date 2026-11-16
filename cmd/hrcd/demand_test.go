// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"context"
	hrcconfig "dimidiumlabs.io/hrc/internal/config"
	"errors"
	"math"
	"os"
	"testing"
	"time"
)

func TestDemandBoundsAcquisition(t *testing.T) {
	demand := newDemand(1)
	defer demand.Close()
	first, second := demand.Reserve(), demand.Reserve()
	defer first.Close()
	defer second.Close()
	if free, _ := demand.State(); free != 0 {
		t.Fatalf("pending demand advertised as free: %d", free)
	}
	if err := first.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- second.Wait(t.Context()) }()
	select {
	case <-done:
		t.Fatal("acquired beyond demand")
	case <-time.After(10 * time.Millisecond):
	}
	_, changed := demand.State()
	first.Close()
	select {
	case <-changed:
	default:
		t.Fatal("release did not notify queues")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	second.Close()
	if free, _ := demand.State(); free != 1 {
		t.Fatalf("demand leaked: %d", free)
	}
}

func TestDemandCancellationAndClose(t *testing.T) {
	for _, closing := range []bool{false, true} {
		demand := newDemand(1)
		first := demand.Reserve()
		if err := first.Wait(t.Context()); err != nil {
			t.Fatal(err)
		}
		pending := demand.Reserve()
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- pending.Wait(ctx) }()
		want := context.Canceled
		if closing {
			want = os.ErrClosed
			demand.Close()
		} else {
			cancel()
		}
		if err := <-done; !errors.Is(err, want) {
			t.Fatalf("waiting acquisition: %v", err)
		}
		pending.Close()
		first.Close()
		cancel()
		demand.Close()
	}
}

func TestSpaceBudgets(t *testing.T) {
	specs := []hrcconfig.Space{
		{ID: "a", Resources: hrcconfig.Resources{CPU: 4, Memory: 8 << 30}},
		{ID: "b", Resources: hrcconfig.Resources{CPU: 2, Memory: 4 << 30}},
	}
	pools := []hrcconfig.Slot{{ID: "a-pool", Space: "a", Quota: hrcconfig.Resources{CPU: 2, Memory: 2 << 30}}, {ID: "b-pool", Space: "b", Quota: hrcconfig.Resources{CPU: 1, Memory: 4 << 30}}}
	host := hrcconfig.Resources{CPU: 8, Memory: 16 << 30}
	limits, err := spaceLimits(specs, pools, host)
	if err != nil || len(limits) != 2 || limits[0] != 2 || limits[1] != 1 {
		t.Fatalf("per-space limits: %v, %v", limits, err)
	}
	specs[1].Resources.CPU = 5
	if _, err := spaceLimits(specs, pools, host); err == nil {
		t.Fatal("overlapping space budgets accepted")
	}
}

func TestHostSchedulerWithoutQuota(t *testing.T) {
	limits, err := spaceLimits([]hrcconfig.Space{{ID: "host"}}, []hrcconfig.Slot{{ID: "pool", Space: "host"}}, hrcconfig.Resources{CPU: 8, Memory: 16 << 30})
	if err != nil || len(limits) != 1 || limits[0] != math.MaxInt32 {
		t.Fatalf("limits=%v err=%v", limits, err)
	}
}
