// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package runtime

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	goruntime "runtime"
)

// LookPath resolves a component beside the real executable before searching PATH.
// Explicit paths retain exec.LookPath semantics. A present but unusable sibling
// is an error, not a reason to select a different installation from PATH.
// The returned path is absolute.
func LookPath(name string) (string, error) {
	return lookPath(name, os.Executable)
}

func lookPath(name string, executable func() (string, error)) (string, error) {
	if filepath.Base(name) != name || filepath.VolumeName(name) != "" {
		return absoluteLookPath(name)
	}
	self, err := executable()
	if err != nil {
		return "", err
	}
	self, err = filepath.Abs(self)
	if err != nil {
		return "", err
	}
	self, err = filepath.EvalSymlinks(self)
	if err != nil {
		return "", err
	}
	filename := name
	if goruntime.GOOS == "windows" && filepath.Ext(filename) == "" {
		filename += ".exe"
	}
	sibling := filepath.Join(filepath.Dir(self), filename)
	// Lstat distinguishes an absent sibling from a dangling symlink. The latter
	// is a broken installation and must not silently select another version.
	if _, err := os.Lstat(sibling); err == nil {
		return absoluteLookPath(sibling)
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	return absoluteLookPath(name)
}

func absoluteLookPath(name string) (string, error) {
	path, err := exec.LookPath(name)
	if err != nil {
		return "", err
	}
	return filepath.Abs(path)
}
