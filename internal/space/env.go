// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package space

import (
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"dimidiumlabs.io/hrc/internal/config"
)

// EnvSpace owns the space layout and storage lifetimes. Its root is hrcm's cwd.
type EnvSpace struct {
	root, spaceType, slotType string
}

func NewEnvSpace(root, spaceType, slotType string) *EnvSpace {
	return &EnvSpace{root: root, spaceType: spaceType, slotType: slotType}
}

func (f *EnvSpace) DirRoot() string     { return f.root }
func (f *EnvSpace) DirTemp() string     { return filepath.Join(f.root, "tmp") }
func (f *EnvSpace) DirCache() string    { return filepath.Join(f.root, "cache") }
func (f *EnvSpace) DirSlots() string    { return filepath.Join(f.root, "slots") }
func (f *EnvSpace) DirLogJobs() string  { return filepath.Join(f.root, "log", "jobs") }
func (f *EnvSpace) DirLogSlots() string { return filepath.Join(f.root, "log", "slots") }
func (f *EnvSpace) DirLogSpace() string { return filepath.Join(f.root, "log", "space") }

func (f *EnvSpace) ClearTmp() error {
	return os.RemoveAll(f.DirTemp())
}

func (f *EnvSpace) ClearCache() error {
	return os.RemoveAll(f.DirCache())
}

func (f *EnvSpace) ClearSlots() error {
	return os.RemoveAll(f.DirSlots())
}

func (f *EnvSpace) Clear(cache bool) error {
	if err := errors.Join(f.ClearTmp(), f.ClearSlots(), nil); err != nil {
		return err
	}
	if cache {
		return f.ClearCache()
	}
	return nil
}

func (f *EnvSpace) Prepare() error {
	if err := f.Clear(false); err != nil {
		return fmt.Errorf("clean space: %w", err)
	}

	dirs := []string{f.DirTemp(), f.DirCache(), f.DirSlots(), f.DirLogJobs(), f.DirSlots()}
	for _, name := range dirs {
		if err := os.MkdirAll(name, 0o700); err != nil {
			return fmt.Errorf("prepare space: %w", err)
		}
	}

	return nil
}

func (f *EnvSpace) Environ() map[string]string {
	return map[string]string{
		"HRC_VERSION":    config.Version,
		"HRC_SPACE_TYPE": f.spaceType,

		"HRC_SPACE_DIR_LOG":   f.DirLogSpace(),
		"HRC_SPACE_DIR_TMP":   f.DirTemp(),
		"HRC_SPACE_DIR_CACHE": f.DirCache(),
	}
}

func (f *EnvSpace) NewSlot() (string, error) {
	name := "slt-" + rand.Text()[:6]
	home := filepath.Join(f.DirSlots(), name)
	logs := filepath.Join(f.DirLogSlots(), name)

	for _, name := range []string{home, logs} {
		if err := os.MkdirAll(name, 0o700); err != nil {
			return "", fmt.Errorf("create slot: %w", err)
		}
	}

	return name, nil
}

func (f *EnvSpace) CleanSlot(name string) error {
	home := filepath.Join(f.DirSlots(), name)
	logs := filepath.Join(f.DirLogSlots(), name)

	for _, name := range []string{home, logs} {
		if err := os.RemoveAll(name); err != nil {
			return fmt.Errorf("clean slot: %w", err)
		}
	}

	return nil
}
