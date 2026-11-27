// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package runtime

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestNamedPipePreservesStdio(t *testing.T) {
	var stdout, stderr bytes.Buffer
	var pool Pool
	child, err := pool.Start(t.Context(), ChildSpec{Command: Command{
		Transport: NamedPipeTransport,
		Path:      os.Args[0], Args: []string{"hrc-stdio-child"},
		Input: true, Stdout: &stdout, Stderr: &stderr,
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pool.Close() })
	if _, err := io.WriteString(child.Stdin(), "independent stdin"); err != nil {
		t.Fatal(err)
	}
	child.Stdin().Close()
	testConnectionEcho(t, child.Connection())
	exit, cleanup := child.Close(5 * time.Second)
	if exit != nil || cleanup != nil {
		t.Fatalf("close: %v %v", exit, cleanup)
	}
	if stdout.String() != "stdout: independent stdin" || stderr.String() != "stderr diagnostic\n" {
		t.Fatalf("stdio was not independent: stdout=%q stderr=%q", &stdout, &stderr)
	}
}

func TestNamedPipeRejectsWrongServer(t *testing.T) {
	control, err := prepareControl(Command{Transport: NamedPipeTransport})
	if err != nil {
		t.Fatal(err)
	}
	defer control.close()
	args := []string{control.name, strconv.Itoa(os.Getpid() + 1)}
	conn, err := InternalConnection(t.Context(), args)
	if conn != nil {
		conn.Close()
		t.Fatal("connected to the wrong daemon PID")
	}
	if err == nil || !strings.Contains(err.Error(), "PID mismatch") {
		t.Fatalf("expected daemon PID mismatch, got %v", err)
	}
}

func TestNamedPipeRejectsWrongClient(t *testing.T) {
	control, err := prepareControl(Command{Transport: NamedPipeTransport})
	if err != nil {
		t.Fatal(err)
	}
	defer control.close()
	// Connect from this test process, not the expected child.
	conn, err := InternalConnection(t.Context(), control.args())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	child := &Child{
		cmd:  &exec.Cmd{Process: &os.Process{Pid: os.Getpid() + 1}},
		done: make(chan struct{}),
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	accepted, err := control.take(ctx, child)
	if accepted != nil {
		accepted.Close()
		t.Fatal("accepted the wrong child PID")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wrong-client rejection did not cancel: %v", err)
	}
}

func TestNamedPipeStartupCancellation(t *testing.T) {
	var pool Pool
	spec := childSpec(t, "stall")
	spec.Command.Transport = NamedPipeTransport
	spec.StartupTimeout = 100 * time.Millisecond
	child, err := pool.Start(t.Context(), spec)
	if child != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("startup: child=%v err=%v", child, err)
	}
	if len(pool.Children()) != 0 {
		t.Fatal("failed startup left a tracked child")
	}
}
