// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"context"
	"encoding/json/v2"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"code.forgejo.org/forgejo/runner/v13/hrcbridge"
	"dimidiumlabs.io/hrc/internal/provider"
)

func TestRejectMalformedAssignment(t *testing.T) {
	for _, data := range []string{`{`, `{}`, `{"task":"YQ=="}`, `{"unknown":true}`} {
		local, err := start(context.Background(), &provider.TaskStart{Payload: []byte(data)})
		if !errors.Is(err, provider.ErrInvalidJob) || local != nil {
			t.Fatalf("start(%q) = %v, %v", data, local, err)
		}
	}
}

func TestRejectInvalidSettingsAndUnavailableDiagnostics(t *testing.T) {
	payload, err := json.Marshal(bridge.Assignment{
		Connection: bridge.Connection{URL: "http://127.0.0.1:1", UUID: "test", Token: "test"},
		Task:       []byte("not a protobuf"),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, settings := range []string{`{`, `{"unknown":true}`, `{"Dir":1}`} {
		local, err := start(context.Background(), &provider.TaskStart{Payload: payload, Config: []byte(settings)})
		if !errors.Is(err, provider.ErrInvalidConfig) || local != nil {
			t.Fatalf("settings %s: %v, %v", settings, local, err)
		}
	}
	root := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(root, nil, 0600); err != nil {
		t.Fatal(err)
	}
	settings, err := json.Marshal(struct{ LogRoot string }{root})
	if err != nil {
		t.Fatal(err)
	}
	local, err := start(context.Background(), &provider.TaskStart{Payload: payload, Config: settings})
	if err == nil || local != nil || !strings.Contains(err.Error(), "diagnostics") {
		t.Fatalf("must not silently fall back to stderr: %v, %v", local, err)
	}
}

func TestMalformedTaskHasNoReportingAcknowledgment(t *testing.T) {
	data, err := json.Marshal(bridge.Assignment{
		Connection: bridge.Connection{URL: "http://127.0.0.1:1", UUID: "poc", Token: "test"},
		Task:       []byte("not a protobuf"),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	root := t.TempDir()
	settings, err := json.Marshal(struct{ Dir, LogRoot string }{t.TempDir(), root})
	if err != nil {
		t.Fatal(err)
	}
	local, err := start(ctx, &provider.TaskStart{Payload: data, Config: settings})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-local.Done():
		if err == nil {
			t.Fatal("expected decode error")
		}
	case <-ctx.Done():
		t.Fatal("execution did not terminate")
	}
	if result, data := local.Result(); result != provider.Unknown || len(data) != 0 {
		t.Fatalf("malformed task claimed reported outcome: %v, %q", result, data)
	}
	files, err := filepath.Glob(filepath.Join(root, "jobs", "forgejo-*", "worker.log"))
	if err != nil || len(files) != 1 {
		t.Fatalf("diagnostics files: %v, %v", files, err)
	}
	content, err := os.ReadFile(files[0])
	if err != nil || !strings.Contains(string(content), "decode task") || !strings.Contains(string(content), "Starting Forgejo task") {
		t.Fatalf("missing local execution diagnostics: %s, %v", content, err)
	}
	if info, err := os.Stat(files[0]); err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("private diagnostics: %v, %v", info, err)
	}
	for range 2 {
		if err := local.Stop(ctx, provider.StopForce); err != nil {
			t.Fatal(err)
		}
	}
	if err := local.Stop(ctx, provider.StopReason(99)); !errors.Is(err, provider.ErrInvalidStopReason) {
		t.Fatalf("stop: %v", err)
	}
}
