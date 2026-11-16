// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package space

import (
	"fmt"
	"math"
)

// SlotCount returns how many slots fit within a space budget.
// Without a quota the count is unlimited.
func SlotCount(budget, quota Resources) (int, error) {
	total := math.MaxInt32

	if quota.CPU > 0 {
		// The epsilon keeps quotas such as 0.1 from losing a slot to rounding.
		n := math.Floor(budget.CPU/quota.CPU + 1e-9)
		total = min(total, int(max(n, 0)))
	}

	if quota.Memory > 0 {
		n := budget.Memory / quota.Memory
		total = min(total, int(min(n, math.MaxInt32)))
	}

	if total == 0 {
		return 0, fmt.Errorf("quota (cpu %g, memory %d bytes) exceeds space resources (cpu %g, memory %d bytes)",
			quota.CPU, quota.Memory, budget.CPU, budget.Memory)
	}

	return total, nil
}
