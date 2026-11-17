// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package spacehost

import (
	"fmt"

	"dimidiumlabs.io/hrc/internal/config"
)

// Config describes a host space.
type Config struct {
	config.Space

	Dir string `json:"dir"`
}

func (c *Config) Validate() error {
	if c.Type == "" {
		c.Type = "host"
	}
	if c.Type != "host" {
		return fmt.Errorf("unknown space type %q", c.Type)
	}
	if err := c.ID.Validate(); err != nil {
		return fmt.Errorf("space: %w", err)
	}
	if err := c.Resources.Validate(); err != nil {
		return err
	}
	return nil
}
