// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package slotpgroup

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"dimidiumlabs.io/hrc/internal/provider-github/worker"
)

func TestMain(m *testing.M) {
	if len(os.Args) == 4 && os.Args[1] == "spawnclient" {
		handle, err := strconv.ParseUint(os.Args[2], 10, 64)
		if err != nil {
			os.Exit(51)
		}
		input := os.NewFile(uintptr(handle), "worker-input")
		data, err := io.ReadAll(input)
		if err != nil {
			os.Exit(52)
		}
		cwd, err := os.Getwd()
		if err != nil {
			os.Exit(53)
		}
		json.NewEncoder(os.Stdout).Encode(map[string]string{
			"input": string(data), "cwd": cwd, "layer": os.Getenv("HRC_LAUNCH_LAYER"),
			"dotenv": os.Getenv("HRC_LAUNCH_DOTENV"), "tracking": os.Getenv("RUNNER_TRACKING_GROUP"),
		})
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// The adapter owns inherited handles, installation environment and slot identity.
// Worker message framing is tested separately in the worker package.
func TestStartWorkerPipesEnvironmentAndReuse(t *testing.T) {
	s, err := New(filepath.Join(t.TempDir(), "slot"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Destroy(context.Background())
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	assets := t.TempDir()
	if err := os.WriteFile(filepath.Join(assets, ".env"), []byte("HRC_LAUNCH_LAYER=dotenv\nHRC_LAUNCH_DOTENV=present\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HRC_LAUNCH_LAYER", "inherited")
	// A service may have no usable PATH. Resolve the Worker beside this binary.
	t.Setenv("PATH", "")
	var identity string
	for i := 0; i < 2; i++ {
		var output bytes.Buffer
		p, input, err := s.StartWorker(t.Context(), worker.Config{Path: filepath.Base(self), WorkDir: s.Cwd(), Dir: assets, Stdout: &output}, []string{"HRC_LAUNCH_LAYER=caller"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := input.Write([]byte("opaque payload")); err != nil {
			t.Fatal(err)
		}
		input.Close()
		code, err := p.Wait()
		p.Close(0)
		if err != nil || code != 0 {
			t.Fatalf("worker: %d %v", code, err)
		}
		var report map[string]string
		if err := json.Unmarshal(output.Bytes(), &report); err != nil {
			t.Fatal(err)
		}
		cwd, err := os.Stat(report["cwd"])
		expected, statErr := os.Stat(s.Cwd())
		if err != nil || statErr != nil || !os.SameFile(cwd, expected) {
			t.Fatal("worker started outside slot")
		}
		if report["input"] != "opaque payload" || report["layer"] != "caller" || report["dotenv"] != "present" || report["tracking"] == "" {
			t.Fatalf("launch settings lost: %v", report)
		}
		if i > 0 && report["tracking"] != identity {
			t.Fatal("slot identity changed between workers")
		}
		identity = report["tracking"]
	}
	if err := os.Remove(filepath.Join(assets, ".env")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(assets, ".env"), 0700); err != nil {
		t.Fatal(err)
	}
	p, _, err := s.StartWorker(t.Context(), worker.Config{Path: self, WorkDir: s.Cwd(), Dir: assets}, nil)
	if err == nil {
		defer p.Close(0)
		if _, err := p.Wait(); err == nil {
			t.Fatal("invalid environment file accepted")
		}
	}
}
