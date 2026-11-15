// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"math"
	"testing"
)

func TestHostSchedulerWithoutQuota(t *testing.T) {
	p, err := newHostScheduler(t.TempDir(), Resources{}, Resources{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	})
	if free, _ := p.State(); free != math.MaxInt32 {
		t.Fatalf("unlimited pool capacity = %d", free)
	}
}
