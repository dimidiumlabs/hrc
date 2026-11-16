// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package scheduleripc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"testing"

	"dimidiumlabs.io/hrc/internal/provider"
	"dimidiumlabs.io/hrc/internal/space"
)

func TestConcurrentFramesPreserveOpaquePayload(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	writer, reader := NewConn(left), NewConn(right)
	payload := []byte{0, 255, 1, 0, '{', '}', 128}
	const count = 32
	var writers sync.WaitGroup
	for i := range count {
		writers.Go(func() {
			if err := writer.Send(t.Context(), Message{Type: Submit, ID: uint64(i + 1), Provider: "test", Payload: payload}); err != nil {
				t.Error(err)
			}
		})
	}
	seen := make(map[uint64]bool)
	for range count {
		m, err := reader.Receive()
		if err != nil {
			t.Fatal(err)
		}
		if m.Type != Submit || m.Provider != "test" || !bytes.Equal(m.Payload, payload) || seen[m.ID] {
			t.Fatalf("corrupt frame: %+v", m)
		}
		seen[m.ID] = true
	}
	writers.Wait()
}

func TestRejectsInvalidFrames(t *testing.T) {
	oversized := []byte{byte(Submit), 0, 0, 0, 0}
	binary.BigEndian.PutUint32(oversized[1:], MaxFrame+1)
	for _, data := range [][]byte{{255, 0, 0, 0, 0}, oversized, {byte(Submit), 0, 0, 0, 10, '{'}} {
		left, right := net.Pipe()
		go func() { left.Write(data); left.Close() }()
		if _, err := NewConn(right).Receive(); err == nil {
			t.Fatal("accepted invalid frame")
		}
		right.Close()
	}
}

func TestClientDoesNotRetryAfterDisconnect(t *testing.T) {
	left, right := net.Pipe()
	received := make(chan Message, 1)
	go func() {
		wire := NewConn(right)
		wire.Receive()
		wire.Send(t.Context(), Message{Type: Ready, Version: Version, Capacity: 1})
		m, _ := wire.Receive()
		received <- m
		right.Close()
	}()
	client, err := NewClient(t.Context(), left, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	run, err := client.Start(t.Context(), "test", []byte("payload"))
	if err != nil {
		t.Fatal(err)
	}
	if m := <-received; m.Type != Submit {
		t.Fatalf("submission: %+v", m)
	}
	if err := <-run.Done(); err == nil {
		t.Fatal("lost execution reported success")
	}
	if _, err := client.Start(t.Context(), "test", nil); err == nil {
		t.Fatal("started on disconnected scheduler")
	}
	if err := run.Stop(t.Context(), provider.StopForce); !errors.Is(err, space.ErrUnavailable) {
		t.Fatalf("closing a lost Run must report unavailable execution: %v", err)
	}
}
