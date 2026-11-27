// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"encoding/json/v2"
	"os"
	"os/user"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"testing"

	"dimidiumlabs.io/hrc/internal/runtime"
)

func TestSpaceUsesConfiguredAccount(t *testing.T) {
	account, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	if account.Uid == "0" || (goruntime.GOOS == "windows" && !strings.HasPrefix(account.Uid, "S-1-5-21-")) {
		t.Skip("requires a regular non-root current account")
	}
	target, err := runtime.LookupUser(account.Username)
	if err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	name := "hrcm"
	if goruntime.GOOS == "windows" {
		name += ".exe"
	}
	// NewSpace only resolves the executable; it does not start it or create HOME.
	if err := os.WriteFile(filepath.Join(bin, name), nil, 0755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin)
	var cfg Config
	if err := json.Unmarshal([]byte(poolConfigJSON), &cfg); err != nil {
		t.Fatal(err)
	}
	cfg.Spaces[0].Base().User = account.Username
	pool := &runtime.Pool{}
	defer pool.Close()
	s, err := cfg.Spaces[0].NewSpace(cfg.Slots[0], 1, nil, pool)
	if err != nil {
		t.Fatal(err)
	}
	if s.Cwd() != target.HomeDir {
		t.Fatalf("space cwd=%q, configured account home=%q", s.Cwd(), target.HomeDir)
	}
}

func TestSpaceRejectsMissingAccount(t *testing.T) {
	var cfg Config
	if err := json.Unmarshal([]byte(poolConfigJSON), &cfg); err != nil {
		t.Fatal(err)
	}
	cfg.Spaces[0].Base().User = "hrc_missing_7f4e3d"
	pool := &runtime.Pool{}
	defer pool.Close()
	s, err := cfg.Spaces[0].NewSpace(cfg.Slots[0], 1, nil, pool)
	if err == nil || s != nil || !strings.Contains(err.Error(), `space "build"`) || !strings.Contains(err.Error(), cfg.Spaces[0].Base().User) {
		t.Fatalf("missing space account: space=%v error=%v", s, err)
	}
}
