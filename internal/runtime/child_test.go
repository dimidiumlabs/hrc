// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package runtime

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func childTestMain() {
	if len(os.Args) != 5 || os.Args[1] != "hrc-socket-child" {
		return
	}
	switch os.Args[2] {
	case "exit":
		os.Exit(12)
	case "stall":
		os.WriteFile("started", []byte(os.Args[4]), 0600)
		time.Sleep(time.Hour)
		os.Exit(0)
	}
	conn, err := net.Dial("unix", os.Args[4])
	if err != nil {
		os.Exit(20)
	}
	if os.Args[2] == "outcome" {
		conn.Write([]byte("terminal outcome"))
	} else {
		conn.Write([]byte("ready"))
		io.Copy(conn, conn)
	}
	conn.Close()
	os.Exit(0)
}

func childSpec(t *testing.T, mode string) ChildSpec {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return ChildSpec{
		Command:    Command{Path: self, Args: []string{"hrc-socket-child", mode}, Dir: t.TempDir(), Stdout: os.Stdout, Stderr: os.Stderr},
		SocketArgs: func(socket string) []string { return []string{"-connect", socket} },
	}
}
func startSocketChild(t *testing.T, pool *Pool, spec ChildSpec) *Child {
	t.Helper()
	c, err := pool.Start(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		exit, err := c.Close(5 * time.Second)
		if err != nil || exit != nil {
			t.Errorf("close child: exit=%v cleanup=%v", exit, err)
		}
	})
	return c
}

// The same Child owns bare and connected processes. Concurrent joins and cleanup
// must never call exec.Cmd.Wait twice or close a slot's reusable Group.
func TestChildWithoutSocketConcurrentWaitAndClose(t *testing.T) {
	var pool Pool
	groups := []*Group{nil, newTestPGroup(t), newTestPGroup(t)}
	var children []*Child
	for _, group := range groups {
		c, err := pool.Start(t.Context(), ChildSpec{Command: Command{
			Group: group, Path: os.Args[0], Args: []string{"hrc-socket-child", "exit", "-connect", "unused"},
		}})
		if err != nil {
			t.Fatal(err)
		}
		children = append(children, c)
		if c.Connection() != nil || c.SocketPath() != "" {
			t.Fatal("bare child allocated a control socket")
		}
	}
	if len(pool.Children()) != len(children) {
		t.Fatal("pool lost ownership of bare children")
	}
	var joins sync.WaitGroup
	for _, c := range children {
		for range 4 {
			joins.Go(func() {
				if code, err := c.Wait(); code != 12 || err != nil {
					t.Errorf("Wait: %d %v", code, err)
				}
			})
			joins.Go(func() {
				exit, cleanup := c.Close(5 * time.Second)
				var status *ExitError
				if cleanup != nil || !errors.As(exit, &status) || status.Code != 12 {
					t.Errorf("Close: %v %v", exit, cleanup)
				}
			})
		}
	}
	joins.Wait()
	if len(pool.Children()) != 0 {
		t.Fatal("closed children retained in pool")
	}
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	for _, group := range groups[1:] {
		c, err := group.Start(t.Context(), Command{Path: os.Args[0], Args: []string{"hrc-socket-child", "exit", "-connect", "unused"}})
		if err != nil {
			t.Fatalf("child cleanup closed its group: %v", err)
		}
		c.Close(5 * time.Second)
	}
}

func TestChildRetainsOutcomeAfterExit(t *testing.T) {
	var pool Pool
	c := startSocketChild(t, &pool, childSpec(t, "outcome"))
	select {
	case <-c.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("child did not exit")
	}
	data, err := io.ReadAll(c.Connection())
	if err != nil || string(data) != "terminal outcome" {
		t.Fatalf("buffered outcome lost: %q %v", data, err)
	}
	if c.Err() != nil {
		t.Fatal(c.Err())
	}
}

func TestPoolStartupFailure(t *testing.T) {
	for _, mode := range []string{"exit", "missing", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			var pool Pool
			spec := childSpec(t, mode)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			if mode == "missing" {
				spec.Command.Path = filepath.Join(t.TempDir(), "missing")
			}
			if mode == "canceled" {
				cancel()
			}
			_, err := pool.Start(ctx, spec)
			if err == nil {
				t.Fatal("startup succeeded")
			}
			if mode == "exit" {
				var exit *ExitError
				if ctx.Err() != nil || !errors.As(err, &exit) || exit.ExitCode() != 12 {
					t.Fatalf("early exit: %v", err)
				}
			}
			if len(pool.Children()) != 0 {
				t.Fatal("failed startup leaked a child")
			}
			if err := pool.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPoolCloseDuringStartup(t *testing.T) {
	var pool Pool
	spec := childSpec(t, "stall")
	done := make(chan error, 1)
	go func() { _, err := pool.Start(t.Context(), spec); done <- err }()
	deadline := time.After(10 * time.Second)
	var socket []byte
	for {
		socket, _ = os.ReadFile(filepath.Join(spec.Command.Dir, "started"))
		if len(socket) > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("child did not start")
		case <-time.After(time.Millisecond):
		}
	}
	// A forced startup rollback may report the killed child's exit status.
	pool.Close()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("startup not canceled: %v", err)
	}
	if _, err := os.Stat(filepath.Dir(string(socket))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("socket leaked: %v", err)
	}
	if _, err := pool.Start(t.Context(), spec); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closed pool accepted child: %v", err)
	}
}

func TestChildCleanupRetry(t *testing.T) {
	var pool Pool
	c := startSocketChild(t, &pool, childSpec(t, "echo"))
	actual := c.socket
	c.socket = filepath.Join(t.TempDir(), "invalid\x00socket", "rpc.sock")
	if _, err := c.Close(5 * time.Second); err == nil {
		t.Fatal("invalid cleanup succeeded")
	}
	if len(pool.Children()) != 1 {
		t.Fatal("failed cleanup discarded ownership")
	}
	c.socket = actual
	if _, err := c.Close(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	if len(pool.Children()) != 0 {
		t.Fatal("successful cleanup retained child")
	}
	if _, err := os.Stat(actual); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("socket leaked: %v", err)
	}
}

func TestChildGroupAndStartupContextIndependent(t *testing.T) {
	var pool Pool
	group, err := NewGroup()
	if err != nil {
		t.Fatal(err)
	}
	defer group.Close()
	for range 2 {
		spec := childSpec(t, "echo")
		spec.Command.Group = group
		ctx, cancel := context.WithCancel(t.Context())
		c, err := pool.Start(ctx, spec)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		c.Connection().SetDeadline(time.Now().Add(10 * time.Second))
		ready := make([]byte, 5)
		if _, err := io.ReadFull(c.Connection(), ready); err != nil || string(ready) != "ready" {
			t.Fatalf("ready: %q %v", ready, err)
		}
		if _, err := c.Connection().Write([]byte("echo")); err != nil {
			t.Fatal(err)
		}
		echo := make([]byte, 4)
		if _, err := io.ReadFull(c.Connection(), echo); err != nil || string(echo) != "echo" {
			t.Fatalf("startup cancellation killed child: %q %v", echo, err)
		}
		if exit, cleanup := c.Close(5 * time.Second); exit != nil || cleanup != nil {
			t.Fatalf("child cleanup: %v %v", exit, cleanup)
		}
	}
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	// Pool cleanup must not prevent the slot's Group from accepting a new child.
	p, err := group.Start(t.Context(), Command{Path: os.Args[0], Args: []string{"hrc-socket-child", "exit", "-connect", "unused"}})
	if err != nil {
		t.Fatalf("pool closed its children's group: %v", err)
	}
	if code, err := p.Wait(); code != 12 || err != nil {
		t.Fatalf("exit: %d %v", code, err)
	}
	p.Close(0)
}
