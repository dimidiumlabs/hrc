// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

//go:build !windows

package runtime

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
)

func TestUserLaunch(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root to test switching to another account")
	}
	user, err := LookupUser("nobody")
	if err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp("/tmp", "hrc-user-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	if err := os.Chmod(root, 0755); err != nil {
		t.Fatal(err)
	}
	home := filepath.Join(root, "home")
	if err := os.Mkdir(home, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chown(home, int(user.uid), int(user.gid)); err != nil {
		t.Fatal(err)
	}
	secret := filepath.Join(root, "daemon-secret")
	if err := os.WriteFile(secret, []byte("private"), 0600); err != nil {
		t.Fatal(err)
	}
	pool := &Pool{}
	defer pool.Close()
	var stderr bytes.Buffer
	// The shell and its id subprocesses must have the target identity. Report
	// through the inherited control channel, not stdout; create state as that user.
	child, err := pool.Start(t.Context(), ChildSpec{Command: Command{
		Path: "/bin/sh", Args: []string{"-c", `exec 1>&"$3"
id -u
id -g
id -G
pwd
printf '%s\n' "$HOME" "$USER" "$LOGNAME"
test ! -r "$1" || exit 90
mkdir work && echo owned > work/file`, "identity", secret},
		Dir: home, User: user, Env: user.Environ(), Stderr: &stderr,
	}})
	if err != nil {
		t.Fatal(err)
	}
	report, err := io.ReadAll(child.Connection())
	if err != nil {
		t.Fatal(err)
	}
	if code, err := child.Wait(); err != nil || code != 0 {
		t.Fatalf("child: code=%d error=%v stderr=%s", code, err, &stderr)
	}
	lines := strings.Split(strings.TrimSpace(string(report)), "\n")
	if len(lines) != 7 || lines[0] != strconv.FormatUint(uint64(user.uid), 10) || lines[1] != strconv.FormatUint(uint64(user.gid), 10) || lines[3] != home || lines[4] != user.HomeDir || lines[5] != user.Name || lines[6] != user.Name {
		t.Fatalf("identity report: %q", report)
	}
	groups := strings.Fields(lines[2])
	want := make([]string, len(user.groups))
	for i, group := range user.groups {
		want[i] = strconv.FormatUint(uint64(group), 10)
	}
	slices.Sort(groups)
	slices.Sort(want)
	if !slices.Equal(groups, slices.Compact(want)) {
		t.Fatalf("groups: %v, want %v", groups, want)
	}
	info, err := os.Stat(filepath.Join(home, "work", "file"))
	if err != nil {
		t.Fatal(err)
	}
	if stat := info.Sys().(*syscall.Stat_t); stat.Uid != user.uid || stat.Gid != user.gid {
		t.Fatalf("child state ownership: uid=%d gid=%d", stat.Uid, stat.Gid)
	}
}
