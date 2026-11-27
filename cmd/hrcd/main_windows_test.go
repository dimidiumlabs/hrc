// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"context"
	"encoding/json/v2"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/windows"

	"dimidiumlabs.io/hrc/internal/provider"
	"dimidiumlabs.io/hrc/internal/runtime"
	"dimidiumlabs.io/hrc/internal/space"
)

func TestWindowsQueueKeepsServiceIdentity(t *testing.T) {
	c := ForgeConfig{}
	queue, err := c.NewQueue("test", "test", &runtime.Pool{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if queue.(*provider.ProcessQueue).User != nil {
		t.Fatal("Windows queue must retain the service account")
	}
}

// The opt-in fixture supplies a local hrc account, a real sibling hrcm.exe,
// launch privileges, and a parent directory writable by hrc but only readable
// by the daemon. It must not point at a production workspace.
func TestWindowsSpaceUser(t *testing.T) {
	parent := os.Getenv("HRC_TEST_WINDOWS_SPACE_ROOT")
	if parent == "" {
		t.Skip("requires isolated privileged Windows space fixture")
	}
	host, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}
	user, err := runtime.LookupUser(host + `\hrc`)
	if err != nil {
		t.Fatal(err)
	}
	var config Config
	if err := json.Unmarshal([]byte(`{
  "instanceId":"test",
  "spaces":[{"id":"build","type":"host"}],
  "slots":[{"id":"workers","space":"build","type":"pgroup","quota":{"cpu":1,"memory":"128MiB"},"ephemeral":true}]
 }`), &config); err != nil {
		t.Fatal(err)
	}
	if err := config.Validate(); err != nil {
		t.Fatal(err)
	}
	pool := &runtime.Pool{}
	defer pool.Close()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	workerConfig, err := json.Marshal(provider.ProcessConfig{Command: []string{self, "-test.run=^TestWindowsSpaceWorker$"}})
	if err != nil {
		t.Fatal(err)
	}
	s, err := config.Spaces[0].NewSpace(Directories{Spaces: parent}, config.Slots[0], 1, map[string][]byte{"test": workerConfig}, pool)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Destroy(); err != nil {
			t.Error(err)
		}
	}()
	if s.Cwd() != user.HomeDir {
		t.Fatalf("monitor cwd=%q, home=%q", s.Cwd(), user.HomeDir)
	}
	root := filepath.Join(parent, "build")
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatalf("workspace already exists: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	payload, err := json.Marshal(os.Getenv("HRC_TEST_WINDOWS_SECRET"))
	if err != nil {
		t.Fatal(err)
	}
	run, err := s.Run(ctx, &provider.OpaqueJob{Key: "test", Provider: "test", Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-run.Done():
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	result, data := run.Result()
	if result != provider.Succeeded {
		t.Fatalf("worker result=%v: %s", result, data)
	}
	var report windowsSpaceIdentity
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatal(err)
	}
	if report.Home != user.HomeDir || report.KnownHome != user.HomeDir {
		t.Fatalf("worker profile: %+v", report)
	}
	rel, err := filepath.Rel(root, report.Cwd)
	if err != nil || strings.HasPrefix(rel, "..") || filepath.Base(report.Cwd) != "work" {
		t.Fatalf("worker cwd=%q", report.Cwd)
	}
	if os.Getenv("HRC_TEST_WINDOWS_SECRET") != "" && !report.SecretDenied {
		t.Fatal("worker read daemon-only secret")
	}
	if s.Status() != space.Ready {
		t.Fatalf("monitor did not initialize: status=%v", s.Status())
	}
	if _, err := s.State(ctx); err != nil {
		t.Fatal(err)
	}
	sd, err := windows.GetNamedSecurityInfo(root, windows.SE_FILE_OBJECT, windows.OWNER_SECURITY_INFORMATION)
	if err != nil {
		t.Fatal(err)
	}
	owner, _, err := sd.Owner()
	if err != nil {
		t.Fatal(err)
	}
	want, _, _, err := windows.LookupSID("", user.Name)
	if err != nil {
		t.Fatal(err)
	}
	if owner.String() != want.String() || report.SID != want.String() || report.Owner != want.String() {
		t.Fatalf("workspace owner=%s, worker=%s, want %s", owner, report.SID, want)
	}
}

type windowsSpaceIdentity struct {
	SID, Owner, Home, KnownHome, Cwd string
	SecretDenied                     bool
}

type windowsSpaceRun struct {
	data []byte
	done chan error
}

func (r *windowsSpaceRun) Done() <-chan error                            { return r.done }
func (r *windowsSpaceRun) Result() (provider.Result, []byte)             { return provider.Succeeded, r.data }
func (*windowsSpaceRun) Stop(context.Context, provider.StopReason) error { return nil }

func TestWindowsSpaceWorker(t *testing.T) {
	if len(flag.Args()) != 2 {
		t.Skip("worker subprocess helper")
	}
	conn, err := runtime.InheritedConnection(flag.Args())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := provider.WorkerClient{Start: func(_ context.Context, task *provider.TaskStart) (provider.Run, error) {
		token, err := windows.GetCurrentProcessToken().GetTokenUser()
		if err != nil {
			return nil, err
		}
		report := windowsSpaceIdentity{SID: token.User.Sid.String(), Home: os.Getenv("USERPROFILE")}
		report.KnownHome, err = windows.KnownFolderPath(windows.FOLDERID_Profile, 0)
		if err != nil {
			return nil, err
		}
		report.Cwd, err = os.Getwd()
		if err != nil {
			return nil, err
		}
		sd, err := windows.GetNamedSecurityInfo(report.Cwd, windows.SE_FILE_OBJECT, windows.OWNER_SECURITY_INFORMATION)
		if err != nil {
			return nil, err
		}
		owner, _, err := sd.Owner()
		if err != nil {
			return nil, err
		}
		report.Owner = owner.String()
		var secret string
		if err := json.Unmarshal(task.Payload, &secret); err != nil {
			return nil, err
		}
		if secret != "" {
			_, err := os.ReadFile(secret)
			report.SecretDenied = errors.Is(err, os.ErrPermission)
		}
		data, err := json.Marshal(report)
		if err != nil {
			return nil, err
		}
		done := make(chan error, 1)
		done <- nil
		return &windowsSpaceRun{data: data, done: done}, nil
	}}
	if err := client.Connect(t.Context(), conn); err != nil {
		t.Fatal(err)
	}
}
