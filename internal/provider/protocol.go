// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"

	"dimidiumlabs.io/hrc/internal/config"
)

func EncodeFailure(err error) *Failure {
	if err == nil {
		return nil
	}
	kind := "unknown"
	for _, entry := range failureKinds {
		if errors.Is(err, entry.err) {
			kind = entry.kind
			break
		}
	}
	return &Failure{Message: err.Error(), Kind: kind}
}

var failureKinds = []struct {
	kind string
	err  error
}{
	{"canceled", context.Canceled}, {"deadline", context.DeadlineExceeded},
	{"config", ErrInvalidConfig}, {"job", ErrInvalidJob}, {"unsupported", ErrUnsupportedJob},
	{"closed", ErrClosed}, {"unavailable", ErrUnavailable}, {"lease", ErrLeaseLost}, {"stop", ErrInvalidStopReason},
}

func (f *Failure) Err() error {
	if f == nil {
		return nil
	}
	for _, entry := range failureKinds {
		if f.Kind == entry.kind {
			return fmt.Errorf("%w: %s", entry.err, f.Message)
		}
	}
	return fmt.Errorf("%w: %s", ErrUnknown, f.Message)
}

// A provider process uses its one accepted socket; there is no reconnect.
type providerListener struct {
	conn     net.Conn
	accepted sync.Once
	closed   sync.Once
	done     chan struct{}
}

func (l *providerListener) Accept() (net.Conn, error) {
	var conn net.Conn
	l.accepted.Do(func() { conn = l.conn })
	if conn != nil {
		return conn, nil
	}
	<-l.done
	return nil, net.ErrClosed
}
func (l *providerListener) Close() error {
	l.closed.Do(func() { l.conn.Close(); close(l.done) })
	return nil
}
func (l *providerListener) Addr() net.Addr { return l.conn.LocalAddr() }

// OpaqueJob is transported without decoding provider-owned bytes.
type OpaqueJob struct {
	Key, Provider string
	Payload       []byte
}

func (j *OpaqueJob) ID() string                  { return j.Key }
func (j *OpaqueJob) Type() string                { return j.Provider }
func (j *OpaqueJob) Marshal() ([]byte, error)    { return bytes.Clone(j.Payload), nil }
func (j *OpaqueJob) Unmarshal(data []byte) error { j.Payload = bytes.Clone(data); return nil }

// ProcessConfig selects a trusted executable in the target environment.
// Config is interpreted only by that provider process.
type ProcessConfig struct {
	Command []string        `json:"command"`
	Config  json.RawMessage `json:"config"`
}

func (c *ProcessConfig) Validate() error {
	if len(c.Command) == 0 || c.Command[0] == "" {
		return fmt.Errorf("%w: process command is required", ErrInvalidConfig)
	}
	if len(c.Config) != 0 && !json.Valid(c.Config) {
		return fmt.Errorf("%w: invalid process configuration", ErrInvalidConfig)
	}
	return nil
}

// QueueConfig contains runtime placement and opaque provider configuration.
type QueueConfig struct {
	config.Provider
	ProcessConfig
}

func (c *QueueConfig) Validate() error {
	if c.Type == config.ProviderUnknown {
		c.Type = config.ProviderProcess
	}
	if c.Type != config.ProviderProcess {
		return fmt.Errorf("unknown queue type %q", c.Type)
	}
	return c.ProcessConfig.Validate()
}
func (c *QueueConfig) NewQueue(id, version string) (Queue, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &ProcessQueue{Command: c.Command, Config: c.Config, Instance: id, Version: version, Name: string(c.ID)}, nil
}
