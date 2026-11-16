// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package config

import (
	"fmt"
	"math"
)

type Resources struct {
	CPU    float64  `json:"cpu"`
	Memory ByteSize `json:"memory"`
}

func (r *Resources) Validate() error {
	if r.CPU < 0 || math.IsInf(r.CPU, 0) || math.IsNaN(r.CPU) {
		return fmt.Errorf("cpu must be a non-negative number")
	}
	return nil
}

// Provider describes a named queue and its placement.
type Provider struct {
	ID   ID     `json:"id"`
	Type string `json:"type"`
	Slot ID     `json:"slot"`
}

// Slot describes execution capacity and lifetime within a space.
type Slot struct {
	ID        ID        `json:"id"`
	Space     ID        `json:"space"`
	Type      string    `json:"type"`
	Quota     Resources `json:"quota"`
	Ephemeral bool      `json:"ephemeral"`
}

// Space describes an environment and its resource budget.
type Space struct {
	ID        ID        `json:"id"`
	Type      string    `json:"type"`
	Resources Resources `json:"resources"`
}
