// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package slot

import (
	"fmt"
	"os"
	"path/filepath"
)

type EnvSlot struct {
	name, rootDir, logsDir, slotType string
}

func NewEnvSlot(name, rootDir, logsDir, slotType string) *EnvSlot {
	return &EnvSlot{name: name, rootDir: rootDir, logsDir: logsDir, slotType: slotType}
}

func (f *EnvSlot) Name() string     { return f.name }
func (f *EnvSlot) DirWork() string  { return filepath.Join(f.rootDir, "work") }
func (f *EnvSlot) DirTemp() string  { return filepath.Join(f.rootDir, "tmp") }
func (f *EnvSlot) DirCache() string { return filepath.Join(f.rootDir, "cache") }

func (f *EnvSlot) ClearTmp() error {
	return os.RemoveAll(f.DirTemp())
}

func (f *EnvSlot) ClearCache() error {
	return os.RemoveAll(f.DirCache())
}

func (f *EnvSlot) Clear(cache bool) error {
	if err := f.ClearTmp(); err != nil {
		return err
	}
	if cache {
		return f.ClearCache()
	}
	return nil
}

func (f *EnvSlot) Environ() map[string]string {
	return map[string]string{
		"HRC_SLOT_TYPE": f.slotType,

		"HRC_SLOT_DIR_LOG":   f.logsDir,
		"HRC_SLOT_DIR_TMP":   f.DirTemp(),
		"HRC_SLOT_DIR_CACHE": f.DirCache(),
	}
}

// Prepare resets job-scoped storage without touching the reusable workspace.
func (f *EnvSlot) Prepare() error {
	if err := f.Clear(false); err != nil {
		return fmt.Errorf("clean slot: %w", err)
	}
	dirs := []string{f.DirWork(), f.DirTemp(), f.DirCache()}
	for _, name := range dirs {
		if err := os.MkdirAll(name, 0o700); err != nil {
			return fmt.Errorf("prepare slot: %w", err)
		}
	}

	return nil
}
