// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"fmt"

	hrcconfig "dimidiumlabs.io/hrc/internal/config"
)

type Directories struct {
	Logs    string `json:"logs"`
	Spaces  string `json:"spaces"`
	Runtime string `json:"runtime"`
}

type Config struct {
	InstanceID  hrcconfig.ID  `json:"instanceId"`
	Forges      []ForgeConfig `json:"forges"`
	Spaces      []SpaceConfig `json:"spaces"`
	Slots       []SlotConfig  `json:"slots"`
	Directories Directories   `json:"directories"`
}

func (c *Config) defaults() {
	if c.Directories.Runtime == "" {
		c.Directories.Runtime = "./runner"
	}
	if c.Directories.Spaces == "" {
		c.Directories.Spaces = "./hrc/build"
	}
	if c.Directories.Logs == "" {
		c.Directories.Logs = "./hrc/log"
	}
}

func (c *Config) Validate() error {
	c.defaults()

	if err := c.InstanceID.Validate(); err != nil {
		return fmt.Errorf("instanceId: %w", err)
	}

	fids := make(map[hrcconfig.ID]bool, len(c.Forges))
	for i := range c.Forges {
		variant := &c.Forges[i]
		if err := variant.Validate(); err != nil {
			return fmt.Errorf("forge %d: %w", i, err)
		}
		f := variant.Base()
		if err := f.ID.Validate(); err != nil {
			return fmt.Errorf("forge %d: %w", i, err)
		}
		if err := f.Slot.Validate(); err != nil {
			return fmt.Errorf("forge %q slot: %w", f.ID, err)
		}
		if fids[f.ID] {
			return fmt.Errorf("duplicate forge id %q", f.ID)
		}
		fids[f.ID] = true
	}

	sids := make(map[hrcconfig.ID]bool)
	for i := range c.Spaces {
		variant := &c.Spaces[i]
		if err := variant.Validate(); err != nil {
			return err
		}
		s := variant.Base()

		if sids[s.ID] {
			return fmt.Errorf("duplicate space %q", s.ID)
		}
		sids[s.ID] = true
	}

	pools := make(map[hrcconfig.ID]bool)
	occupied := make(map[hrcconfig.ID]hrcconfig.ID)
	for i := range c.Slots {
		variant := &c.Slots[i]
		if err := variant.Validate(); err != nil {
			return fmt.Errorf("slot pool %d: %w", i, err)
		}
		pool := variant.Base()
		if pools[pool.ID] {
			return fmt.Errorf("duplicate slot pool %q", pool.ID)
		}
		if !sids[pool.Space] {
			return fmt.Errorf("slot pool %q refers to unknown space %q", pool.ID, pool.Space)
		}
		if other, ok := occupied[pool.Space]; ok {
			return fmt.Errorf("space %q has multiple slot pools: %q and %q", pool.Space, other, pool.ID)
		}
		pools[pool.ID] = true
		occupied[pool.Space] = pool.ID
	}
	for _, variant := range c.Forges {
		forge := variant.Base()
		if !pools[forge.Slot] {
			return fmt.Errorf("forge %q refers to unknown slot pool %q", forge.ID, forge.Slot)
		}
	}

	return nil
}

func LoadConfig(pathname string) (*Config, error) {
	var cfg Config
	if err := hrcconfig.Load(pathname, &cfg); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}
	return &cfg, nil
}

func (c *Config) poolForSpace(id hrcconfig.ID) *SlotConfig {
	for i := range c.Slots {
		if c.Slots[i].Base().Space == id {
			return &c.Slots[i]
		}
	}
	return nil
}
