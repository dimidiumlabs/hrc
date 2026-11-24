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

// ProviderType selects a provider implementation. Its zero value is unspecified;
// configuration files use the corresponding string names, not numeric values.
type ProviderType uint8

const (
	ProviderUnknown ProviderType = iota
	ProviderGitHub
	ProviderForgejo
	ProviderProcess
)

func (t ProviderType) String() string {
	switch t {
	case ProviderUnknown:
		return ""
	case ProviderGitHub:
		return "github"
	case ProviderForgejo:
		return "forgejo"
	case ProviderProcess:
		return "process"
	default:
		return fmt.Sprintf("ProviderType(%d)", uint8(t))
	}
}

func (t ProviderType) MarshalText() ([]byte, error) {
	switch t {
	case ProviderGitHub, ProviderForgejo, ProviderProcess:
		return []byte(t.String()), nil
	default:
		return nil, fmt.Errorf("unknown configuration type %q", t)
	}
}

func (t *ProviderType) UnmarshalText(data []byte) error {
	switch string(data) {
	case "github":
		*t = ProviderGitHub
	case "forgejo":
		*t = ProviderForgejo
	case "process":
		*t = ProviderProcess
	default:
		return fmt.Errorf("unknown configuration type %q", data)
	}
	return nil
}

// Provider describes a named queue and its placement.
type Provider struct {
	ID   ID           `json:"id"`
	Type ProviderType `json:"type"`
	Slot ID           `json:"slot"`
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
