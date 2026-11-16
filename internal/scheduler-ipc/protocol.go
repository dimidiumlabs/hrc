// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

// Package scheduleripc carries a fixed set of scheduler commands and events.
// Each frame is a byte tag, a big-endian uint32 length, and a JSON value.
// Provider payloads are opaque bytes; no process streams cross this connection.
package scheduleripc

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"dimidiumlabs.io/hrc/internal/provider"
	"dimidiumlabs.io/hrc/internal/slot"
	"dimidiumlabs.io/hrc/internal/space"
)

type Type byte

const (
	Init Type = iota + 1
	Ready
	Submit
	Cancel
	ShutdownRun
	Kill
	Finished
	Stop
	Inspect
	Snapshot
)

const Version = 1
const MaxFrame = 64 << 20

type Message struct {
	Type      Type            `json:"-"`
	Version   int             `json:"version,omitempty"`
	ID        uint64          `json:"id,omitempty"`
	Provider  string          `json:"provider,omitempty"`
	Payload   []byte          `json:"payload,omitempty"`
	Capacity  int             `json:"capacity,omitempty"`
	Result    provider.Result `json:"result,omitempty"`
	Data      []byte          `json:"data,omitempty"`
	Error     string          `json:"error,omitempty"`
	ErrorKind string          `json:"errorKind,omitempty"`
	State     space.State     `json:"state,omitempty"`
}

// Only stable public errors cross IPC; provider errors retain their message.
var wireErrors = map[string]error{
	"provider.invalidStopReason": provider.ErrInvalidStopReason,
	"provider.invalidConfig":     provider.ErrInvalidConfig, "provider.invalidJob": provider.ErrInvalidJob,
	"provider.unsupportedJob": provider.ErrUnsupportedJob, "provider.closed": provider.ErrClosed,
	"provider.unavailable": provider.ErrUnavailable, "provider.leaseLost": provider.ErrLeaseLost,
	"provider.unknown": provider.ErrUnknown,
	"slot.busy":        slot.ErrBusy, "slot.destroyed": slot.ErrDestroyed,
	"slot.unknownRun": slot.ErrUnknownRun, "slot.providerUnavailable": slot.ErrProviderUnavailable,
	"slot.invalidRun": slot.ErrInvalidRun, "slot.prepare": slot.ErrPrepare, "slot.unknown": slot.ErrUnknown,
	"space.unavailable": space.ErrUnavailable, "space.closed": space.ErrClosed,
	"space.invalidConfig": space.ErrInvalidConfig, "space.unknown": space.ErrUnknown,
	"context.canceled": context.Canceled, "context.deadline": context.DeadlineExceeded,
}

func (m *Message) SetError(err error) {
	if err == nil {
		return
	}
	m.Error = err.Error()
	for name, kind := range wireErrors {
		if errors.Is(err, kind) {
			m.ErrorKind = name
			break
		}
	}
}
func (m Message) Failure() error {
	if m.Error == "" {
		return nil
	}
	if kind := wireErrors[m.ErrorKind]; kind != nil {
		return fmt.Errorf("%s: %w", m.Error, kind)
	}
	return errors.New(m.Error)
}

type Conn struct {
	net.Conn
	writes chan struct{}
}

func NewConn(conn net.Conn) *Conn {
	c := &Conn{Conn: conn, writes: make(chan struct{}, 1)}
	c.writes <- struct{}{}
	return c
}

// Send serializes concurrent writers. Interrupted writes close the connection:
// a partially written frame cannot be resumed safely.
func (c *Conn) Send(ctx context.Context, m Message) error {
	if m.Type < Init || m.Type > Snapshot {
		return errors.New("unknown scheduler message")
	}
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if len(data) > MaxFrame {
		return errors.New("scheduler frame too large")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.writes:
	}
	defer func() { c.writes <- struct{}{} }()
	if err := ctx.Err(); err != nil {
		return err
	}
	stop := context.AfterFunc(ctx, func() { c.Conn.Close() })
	defer stop()
	var header [5]byte
	header[0] = byte(m.Type)
	binary.BigEndian.PutUint32(header[1:], uint32(len(data)))
	if _, err = io.Copy(c.Conn, io.MultiReader(
		bytes.NewReader(header[:]), bytes.NewReader(data),
	)); err != nil {
		c.Conn.Close()
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

func (c *Conn) Receive() (Message, error) {
	var header [5]byte
	if _, err := io.ReadFull(c.Conn, header[:]); err != nil {
		return Message{}, err
	}
	kind, n := Type(header[0]), binary.BigEndian.Uint32(header[1:])
	if kind < Init || kind > Snapshot || n > MaxFrame {
		return Message{}, errors.New("invalid scheduler frame header")
	}
	data := make([]byte, n)
	if _, err := io.ReadFull(c.Conn, data); err != nil {
		return Message{}, err
	}
	var m Message
	if err := json.Unmarshal(data, &m); err != nil {
		return Message{}, errors.New("invalid scheduler message")
	}
	m.Type = kind
	return m, nil
}
