// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package runtime

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	goruntime "runtime"
	"testing"
)

func componentFile(t *testing.T, dir, name string) string {
	t.Helper()
	if goruntime.GOOS == "windows" {
		name += ".exe"
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("test executable"), 0755); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLookPathSiblingBeforePATH(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "installation with spaces")
	if err := os.Mkdir(dir, 0755); err != nil {
		t.Fatal(err)
	}
	self := componentFile(t, dir, "controller")
	want, err := filepath.EvalSymlinks(componentFile(t, dir, "scheduler"))
	if err != nil {
		t.Fatal(err)
	}
	pathDir := t.TempDir()
	componentFile(t, pathDir, "scheduler")
	t.Setenv("PATH", pathDir)
	for _, searchPath := range []string{pathDir, ""} {
		t.Setenv("PATH", searchPath)
		for _, name := range []string{"scheduler", filepath.Base(want)} {
			got, err := lookPath(name, func() (string, error) { return self, nil })
			if err != nil || got != want {
				t.Fatalf("LookPath(%q) = %q, %v; want %q", name, got, err, want)
			}
		}
	}
}

func TestLookPathFallback(t *testing.T) {
	pathDir := t.TempDir()
	want := componentFile(t, pathDir, "hrc-test-component")
	t.Setenv("PATH", pathDir)
	got, err := LookPath("hrc-test-component")
	if err != nil || got != want {
		t.Fatalf("LookPath = %q, %v; want %q", got, err, want)
	}
	_, err = LookPath("hrc-test-missing-component")
	if !errors.Is(err, exec.ErrNotFound) {
		t.Fatalf("missing component: %v", err)
	}
}

func TestLookPathExplicitPath(t *testing.T) {
	dir := t.TempDir()
	want := componentFile(t, dir, "component")
	t.Chdir(filepath.Dir(dir))
	got, err := lookPath(filepath.Join(filepath.Base(dir), filepath.Base(want)), func() (string, error) {
		t.Fatal("explicit paths must not inspect the current executable")
		return "", os.ErrInvalid
	})
	if err != nil || !filepath.IsAbs(got) {
		t.Fatalf("LookPath = %q, %v; want absolute path to %q", got, err, want)
	}
	actual, err := os.Stat(got)
	expected, statErr := os.Stat(want)
	if err != nil || statErr != nil || !os.SameFile(actual, expected) {
		t.Fatalf("LookPath = %q; want the explicit file %q", got, want)
	}
}

func TestLookPathResolvesExecutableSymlink(t *testing.T) {
	realDir := t.TempDir()
	self := componentFile(t, realDir, "controller")
	want, err := filepath.EvalSymlinks(componentFile(t, realDir, "scheduler"))
	if err != nil {
		t.Fatal(err)
	}
	linkDir := t.TempDir()
	link := filepath.Join(linkDir, filepath.Base(self))
	if err := os.Symlink(self, link); err != nil {
		if goruntime.GOOS == "windows" {
			t.Skipf("symlinks unavailable: %v", err)
		}
		t.Fatal(err)
	}
	componentFile(t, linkDir, "scheduler")
	t.Setenv("PATH", "")
	t.Chdir(filepath.Dir(linkDir))
	got, err := lookPath(filepath.Base(want), func() (string, error) {
		return filepath.Join(filepath.Base(linkDir), filepath.Base(link)), nil
	})
	if err != nil || got != want {
		t.Fatalf("LookPath = %q, %v; want real sibling %q", got, err, want)
	}
}

func TestLookPathUnusableSiblingDoesNotFallback(t *testing.T) {
	for _, kind := range []string{"directory", "nonexecutable", "dangling-link"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			self := componentFile(t, dir, "controller")
			sibling := componentFile(t, dir, "scheduler")
			if err := os.Remove(sibling); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "directory":
				if err := os.Mkdir(sibling, 0755); err != nil {
					t.Fatal(err)
				}
			case "nonexecutable":
				if goruntime.GOOS == "windows" {
					t.Skip("Windows executable lookup does not use Unix mode bits")
				}
				if err := os.WriteFile(sibling, nil, 0644); err != nil {
					t.Fatal(err)
				}
			case "dangling-link":
				if err := os.Symlink(filepath.Join(dir, "missing"), sibling); err != nil {
					if goruntime.GOOS == "windows" {
						t.Skipf("symlinks unavailable: %v", err)
					}
					t.Fatal(err)
				}
			}
			pathDir := t.TempDir()
			componentFile(t, pathDir, "scheduler")
			t.Setenv("PATH", pathDir)
			if path, err := lookPath("scheduler", func() (string, error) { return self, nil }); err == nil {
				t.Fatalf("accepted unusable sibling: %q", path)
			}
		})
	}
}

func TestLookPathExecutableError(t *testing.T) {
	_, err := lookPath("scheduler", func() (string, error) { return "", os.ErrPermission })
	if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("executable error = %v", err)
	}
}
