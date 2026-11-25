// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package runtime

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"
)

func TestAnonymousChild(t *testing.T) {
	var pool Pool
	child, err := pool.Start(t.Context(), ChildSpec{
		Command: Command{Path: os.Args[0], Args: []string{"hrc-anonymous-child"}, Stderr: os.Stderr},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		exit, cleanup := child.Close(5 * time.Second)
		if exit != nil || cleanup != nil {
			t.Errorf("close: %v %v", exit, cleanup)
		}
	})
	testConnectionEcho(t, child.Connection())
}

func testConnectionEcho(t *testing.T, conn net.Conn) {
	t.Helper()
	// Synchronous anonymous Windows pipes do not support OS deadlines.
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil && !errors.Is(err, os.ErrNoDeadline) {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() {
		payload := []byte("opaque control payload Привет")
		if _, err := conn.Write(payload); err != nil {
			finished <- err
			return
		}
		received := make([]byte, len(payload))
		_, err := io.ReadFull(conn, received)
		if err == nil && !bytes.Equal(received, payload) {
			err = fmt.Errorf("received %q", received)
		}
		finished <- err
	}()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		conn.Close()
		t.Fatal("control channel did not echo")
	}
}

func TestAnonymousChildPipeFallback(t *testing.T) {
	conn, input, output, err := pipeConnection()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	defer input.Close()
	defer output.Close()
	child, err := StartChild(t.Context(), Command{
		Path: os.Args[0], Args: []string{"hrc-anonymous-child"},
		files: []*os.File{input, output}, Stderr: os.Stderr,
	})
	if err != nil {
		t.Fatal(err)
	}
	input.Close()
	output.Close()
	t.Cleanup(func() {
		exit, cleanup := child.Close(5 * time.Second)
		if exit != nil || cleanup != nil {
			t.Errorf("close: %v %v", exit, cleanup)
		}
	})
	testConnectionEcho(t, conn)
}

func TestPipeConnection(t *testing.T) {
	conn, input, output, err := pipeConnection()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		defer input.Close()
		defer output.Close()
		io.Copy(output, input)
	}()
	testConnectionEcho(t, conn)
	conn.Close()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("closing connection did not release peer")
	}
}

func TestAnonymousPreservesStdio(t *testing.T) {
	var stdout, stderr bytes.Buffer
	var pool Pool
	child, err := pool.Start(t.Context(), ChildSpec{
		Command: Command{
			Path: os.Args[0], Args: []string{"hrc-stdio-child"},
			Input: true, Stdout: &stdout, Stderr: &stderr,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { child.Close(5 * time.Second) })
	if _, err := io.WriteString(child.Stdin(), "ordinary input\n"); err != nil {
		t.Fatal(err)
	}
	if err := child.Stdin().Close(); err != nil {
		t.Fatal(err)
	}
	testConnectionEcho(t, child.Connection())
	if exit, cleanup := child.Close(5 * time.Second); exit != nil || cleanup != nil {
		t.Fatalf("close: %v %v", exit, cleanup)
	}
	if stdout.String() != "stdout: ordinary input\n" || stderr.String() != "stderr diagnostic\n" {
		t.Fatalf("stdio was changed: stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

func TestInheritedConnectionInvalidArguments(t *testing.T) {
	for _, args := range [][]string{
		nil, {"3"}, {"3", "4", "5"}, {"0", "4"}, {"3", "1"},
		{"-1", "4"}, {"abc", "4"}, {"3", ""}, {"18446744073709551615", "4"},
		{"18446744073709551616", "4"},
	} {
		if conn, err := InheritedConnection(args); !errors.Is(err, os.ErrInvalid) || conn != nil {
			t.Errorf("InheritedConnection(%q) = %v, %v", args, conn, err)
		}
	}
}
