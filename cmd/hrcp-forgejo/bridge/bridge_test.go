// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package bridge

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestNodeRuntime(t *testing.T) {
	root := t.TempDir()
	name := "node"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	for _, using := range []string{"node20", "node24"} {
		path := filepath.Join(root, "externals", using, "bin", name)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("bundled runtime"), 0700); err != nil {
			t.Fatal(err)
		}
		got, err := NodeRuntime(root, using)
		if err != nil || got != path {
			t.Fatalf("%s: %q, %v", using, got, err)
		}
	}
	// Even an installed host node must not substitute an absent requested version.
	t.Setenv("PATH", root)
	for _, using := range []string{"node12", "node16"} {
		if _, err := NodeRuntime(root, using); err == nil || !strings.Contains(err.Error(), using) {
			t.Fatalf("missing %s runtime: %v", using, err)
		}
	}
	for _, using := range []string{"", "node", "../node20", "node22"} {
		if _, err := NodeRuntime(root, using); err == nil {
			t.Fatalf("accepted %q", using)
		}
	}
	if _, err := NodeRuntime("", "node20"); err == nil {
		t.Fatal("accepted missing runtime directory")
	}
	if runtime.GOOS != "windows" {
		path := filepath.Join(root, "externals", "node20", "bin", name)
		if err := os.Chmod(path, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := NodeRuntime(root, "node20"); err == nil {
			t.Fatal("accepted non-executable runtime")
		}
	}
}
