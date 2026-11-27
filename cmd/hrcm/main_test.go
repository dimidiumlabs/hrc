// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"dimidiumlabs.io/hrc/internal/runtime"
	"dimidiumlabs.io/hrc/internal/space"
)

func TestMain(m *testing.M) {
	if len(os.Args) == 5 && os.Args[1] == "hrcm-directory-child" {
		*directory = os.Args[2]
		if err := serve(context.Background(), os.Args[3:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestMonitorPreparesDirectoryAfterInit(t *testing.T) {
	home := t.TempDir()
	root := filepath.Join(home, "jobs", "build")
	var pool runtime.Pool
	child, err := pool.Start(t.Context(), runtime.ChildSpec{Command: runtime.Command{
		Path: os.Args[0], Args: []string{"hrcm-directory-child", root},
		Dir: home, Stderr: os.Stderr,
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pool.Close() })
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatalf("monitor prepared directory before Init: %v", err)
	}
	conn, err := dial(child.Connection())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := space.NewMonitorClient(conn)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	reply, err := client.Init(ctx, &space.InitRequest{Version: space.Version, Config: &space.Config{
		SpaceType: "host", Capacity: 1, Slot: "pgroup", SlotConfig: []byte("{}"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if reply.Error != nil {
		t.Fatal(reply.Error)
	}
	if _, err := client.Inspect(ctx, &space.InspectRequest{}); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{"cache", "tmp", "slots", filepath.Join("log", "jobs")} {
		if info, err := os.Stat(filepath.Join(root, dir)); err != nil || !info.IsDir() {
			t.Fatalf("space directory %s: %v", dir, err)
		}
	}
	exit, cleanup := child.Close(5 * time.Second)
	if exit != nil || cleanup != nil {
		t.Fatalf("close monitor: %v %v", exit, cleanup)
	}
}
