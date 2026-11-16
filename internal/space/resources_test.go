// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package space

import (
	"math"
	"strings"
	"testing"
)

func TestSlotCount(t *testing.T) {
	const gib = 1 << 30
	host := Resources{CPU: 32, Memory: 128 * gib}
	for _, tt := range []struct {
		name  string
		quota Resources
		want  int
		err   string
	}{
		{name: "unlimited", want: math.MaxInt32},
		{name: "balanced", quota: Resources{CPU: 2, Memory: 8 * gib}, want: 16},
		{name: "memory bound", quota: Resources{CPU: 1, Memory: 16 * gib}, want: 8},
		{name: "cpu bound", quota: Resources{CPU: 4, Memory: gib}, want: 8},
		{name: "cpu only", quota: Resources{CPU: 3}, want: 10},
		{name: "memory only", quota: Resources{Memory: 16 * gib}, want: 8},
		{name: "fractional cpu", quota: Resources{CPU: 0.1}, want: 320},
		{name: "quota exceeds space", quota: Resources{CPU: 64}, err: "exceeds space resources"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SlotCount(host, tt.quota)
			if tt.err == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tt.err) {
				t.Fatalf("error = %v, want containing %q", err, tt.err)
			}
			if got != tt.want {
				t.Errorf("slotCount = %d, want %d", got, tt.want)
			}
		})
	}
}
