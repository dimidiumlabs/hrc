// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package config

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

type secretRead struct {
	done  chan struct{}
	value string
	err   error
}

type Secret struct {
	Src string `json:"type"`

	Value   string   `json:"value,omitempty"`
	Command []string `json:"command,omitempty"`

	TTL     string `json:"ttl,omitempty"`
	Timeout string `json:"timeout,omitempty"`

	mu      sync.Mutex
	value   string
	expires time.Time
	flight  *secretRead
}

func (h *Secret) Invalidate() {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.value = ""
	h.expires = time.Time{}
}

// Take returns the secret without surrounding whitespace, such as the newline
// that ends a secret file. An empty secret is an error.
func (h *Secret) Take(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := h.Validate(); err != nil {
		return "", err
	}

	h.mu.Lock()
	if h.value != "" && time.Now().Before(h.expires) {
		value := h.value
		h.mu.Unlock()
		return value, nil
	}
	if flight := h.flight; flight != nil {
		h.mu.Unlock()
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-flight.done:
			return flight.value, flight.err
		}
	}
	flight := &secretRead{done: make(chan struct{})}
	h.flight = flight
	h.value = ""
	h.expires = time.Time{}
	h.mu.Unlock()

	started := time.Now()
	value, err := h.read(ctx)
	h.mu.Lock()
	defer h.mu.Unlock()
	ttl, _ := time.ParseDuration(h.TTL)
	if err == nil && ttl > 0 {
		h.value = value
		h.expires = started.Add(ttl)
	}
	flight.value, flight.err = value, err
	h.flight = nil
	close(flight.done)
	return value, err
}

func (c *Secret) Validate() error {
	if c.Src != "file" && c.Src != "env" && c.Src != "inline" && c.Src != "subcommand" {
		return fmt.Errorf("secret type must be one of: 'inline', 'file', 'env', 'subcommand'")
	}
	if c.Src == "subcommand" {
		if len(c.Command) == 0 || c.Command[0] == "" {
			return fmt.Errorf("secret command must contain an executable")
		}
		if c.Value != "" {
			return fmt.Errorf("subcommand secret must not set value")
		}
	} else {
		if c.Value == "" {
			return fmt.Errorf("secret value could not be empty")
		}
		if len(c.Command) != 0 || c.Timeout != "" {
			return fmt.Errorf("command and timeout require a subcommand secret")
		}
	}
	if c.TTL != "" {
		ttl, err := time.ParseDuration(c.TTL)
		if err != nil || ttl < 0 {
			return fmt.Errorf("secret ttl must be a non-negative duration")
		}
	}
	if c.Timeout != "" {
		timeout, err := time.ParseDuration(c.Timeout)
		if err != nil || timeout <= 0 {
			return fmt.Errorf("secret timeout must be a positive duration")
		}
	}
	return nil
}

func (c *Secret) read(ctx context.Context) (string, error) {
	var value string

	switch c.Src {
	case "env":
		value = os.Getenv(c.Value)

	case "file":
		data, err := os.ReadFile(c.Value)
		if err != nil {
			return "", err
		}
		value = string(data)

	case "inline":
		value = c.Value

	case "subcommand":
		var err error
		value, err = c.run(ctx)
		if err != nil {
			return "", err
		}

	default:
		return "", fmt.Errorf("unknown secret type")
	}

	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("%s secret is empty", c.Src)
	}

	return value, nil
}

func (c *Secret) run(ctx context.Context) (string, error) {
	timeout := SecretCommandTimeout
	if c.Timeout != "" {
		timeout, _ = time.ParseDuration(c.Timeout)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, c.Command[0], c.Command[1:]...)
	cmd.WaitDelay = SecretCommandWaitDelay
	output := &secretOutput{}
	cmd.Stdout = output
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return "", fmt.Errorf("secret subcommand: %w", ctx.Err())
		}
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			return "", fmt.Errorf("secret subcommand exited with code %d", exit.ExitCode())
		}
		return "", fmt.Errorf("secret subcommand failed")
	}
	if output.overflow {
		return "", fmt.Errorf("secret subcommand output exceeds 1 MiB")
	}
	return output.value.String(), nil
}

type secretOutput struct {
	value    strings.Builder
	overflow bool
}

func (b *secretOutput) Write(p []byte) (int, error) {
	const limit = 1 << 20
	if b.value.Len()+len(p) > limit {
		b.overflow = true
		return len(p), nil
	}
	if !b.overflow {
		b.value.Write(p)
	}
	return len(p), nil
}
