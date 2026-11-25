// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package slotpgroup

import (
	"dimidiumlabs.io/hrc/internal/config"
	"fmt"
)

// Config describes a pool of process slots rooted in the space.
type Config struct {
	config.Slot
}

func (c *Config) Validate() error {
	if c.Type == "" {
		c.Type = "pgroup"
	}
	if c.Type != "pgroup" {
		return fmt.Errorf("unknown slot type %q", c.Type)
	}
	if err := c.ID.Validate(); err != nil {
		return fmt.Errorf("slot pool: %w", err)
	}
	if err := c.Space.Validate(); err != nil {
		return fmt.Errorf("slot pool space: %w", err)
	}
	return c.Quota.Validate()
}
