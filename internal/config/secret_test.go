// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package config

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSecretOptions(t *testing.T) {
	for _, tt := range []struct {
		name   string
		secret *Secret
		want   string
	}{
		{"command", &Secret{Src: "subcommand", Command: []string{"helper", "argument"}, TTL: "5m", Timeout: "10s"}, ""},
		{"missing executable", &Secret{Src: "subcommand"}, "executable"},
		{"empty executable", &Secret{Src: "subcommand", Command: []string{""}}, "executable"},
		{"command value", &Secret{Src: "subcommand", Command: []string{"helper"}, Value: "secret"}, "must not set value"},
		{"file command", &Secret{Src: "file", Value: "key", Command: []string{"helper"}}, "require a subcommand"},
		{"file timeout", &Secret{Src: "file", Value: "key", Timeout: "1s"}, "require a subcommand"},
		{"invalid ttl", &Secret{Src: "inline", Value: "secret", TTL: "later"}, "ttl"},
		{"negative ttl", &Secret{Src: "inline", Value: "secret", TTL: "-1s"}, "ttl"},
		{"zero ttl", &Secret{Src: "inline", Value: "secret", TTL: "0s"}, ""},
		{"negative timeout", &Secret{Src: "subcommand", Command: []string{"helper"}, Timeout: "-1s"}, "timeout"},
		{"zero timeout", &Secret{Src: "subcommand", Command: []string{"helper"}, Timeout: "0s"}, "timeout"},
		{"invalid timeout", &Secret{Src: "subcommand", Command: []string{"helper"}, Timeout: "later"}, "timeout"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			checkError(t, tt.secret.Validate(), tt.want)
		})
	}
}

func TestSecretCache(t *testing.T) {
	for _, src := range []string{"env", "file", "inline"} {
		t.Run(src, func(t *testing.T) {
			secret := &Secret{Src: src, TTL: "1h"}
			set := func(value string) {
				switch src {
				case "inline":
					secret.Value = value
				case "env":
					secret.Value = "HRC_CACHE_TEST"
					t.Setenv(secret.Value, value)
				case "file":
					if secret.Value == "" {
						secret.Value = filepath.Join(t.TempDir(), "secret")
					}
					if err := os.WriteFile(secret.Value, []byte(value), 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			set("first")
			if value, err := secret.Take(context.Background()); err != nil || value != "first" {
				t.Fatalf("first read = %q, %v", value, err)
			}
			set("second")
			if value, err := secret.Take(context.Background()); err != nil || value != "first" {
				t.Fatalf("cached read = %q, %v", value, err)
			}
			secret.expires = time.Now().Add(-time.Second)
			if value, err := secret.Take(context.Background()); err != nil || value != "second" {
				t.Fatalf("expired read = %q, %v", value, err)
			}
			secret.Invalidate()
			set(" \n")
			if value, err := secret.Take(context.Background()); err == nil || value != "" {
				t.Fatal("returned a stale or empty secret")
			}
			set("recovered")
			if value, err := secret.Take(context.Background()); err != nil || value != "recovered" {
				t.Fatalf("recovery = %q, %v", value, err)
			}
		})
	}
}

func TestSecretNoCache(t *testing.T) {
	for _, ttl := range []string{"", "0s"} {
		secret := &Secret{Src: "env", Value: "HRC_CACHE_TEST", TTL: ttl}
		t.Setenv(secret.Value, "first")
		if _, err := secret.Take(context.Background()); err != nil {
			t.Fatal(err)
		}
		t.Setenv(secret.Value, "second")
		if value, err := secret.Take(context.Background()); err != nil || value != "second" {
			t.Fatalf("uncached read = %q, %v", value, err)
		}
	}
}

func TestSecretIndependentCaches(t *testing.T) {
	first := &Secret{Src: "env", Value: "HRC_CACHE_TEST", TTL: "1h"}
	second := &Secret{Src: "env", Value: "HRC_CACHE_TEST", TTL: "1h"}
	ctx := context.Background()
	t.Setenv(first.Value, "first")
	if value, err := first.Take(ctx); err != nil || value != "first" {
		t.Fatalf("first secret = %q, %v", value, err)
	}
	t.Setenv(first.Value, "second")
	if value, err := second.Take(ctx); err != nil || value != "second" {
		t.Fatalf("second secret = %q, %v", value, err)
	}
	if value, err := first.Take(ctx); err != nil || value != "first" {
		t.Fatalf("first cache changed = %q, %v", value, err)
	}
	first.Invalidate()
	t.Setenv(first.Value, "third")
	if value, err := first.Take(ctx); err != nil || value != "third" {
		t.Fatalf("first refresh = %q, %v", value, err)
	}
	if value, err := second.Take(ctx); err != nil || value != "second" {
		t.Fatalf("second cache invalidated = %q, %v", value, err)
	}
}

func secretCommand(t *testing.T, mode string, args ...string) *Secret {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("HRC_SECRET_HELPER", "1")
	command := []string{executable, "-test.run=^TestSecretSubcommandProcess$", "--", mode}
	return &Secret{Src: "subcommand", Command: append(command, args...)}
}

func TestSecretSubcommandProcess(t *testing.T) {
	if os.Getenv("HRC_SECRET_HELPER") != "1" {
		return
	}
	args := os.Args
	for len(args) > 0 && args[0] != "--" {
		args = args[1:]
	}
	args = args[1:]
	switch args[0] {
	case "echo":
		fmt.Fprint(os.Stdout, " \n"+strings.Join(args[1:], "|")+"\n")
	case "fail":
		fmt.Fprint(os.Stdout, "SENSITIVE_STDOUT")
		fmt.Fprint(os.Stderr, "SENSITIVE_STDERR")
		os.Exit(7)
	case "sleep":
		time.Sleep(time.Minute)
	case "large":
		fmt.Fprint(os.Stdout, strings.Repeat("x", (1<<20)+1))
	case "count":
		file, err := os.OpenFile(args[1], os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			os.Exit(8)
		}
		fmt.Fprint(file, "x")
		file.Close()
		time.Sleep(100 * time.Millisecond)
		fmt.Fprint(os.Stdout, "token")
	}
	os.Exit(0)
}

func TestSecretSubcommand(t *testing.T) {
	t.Run("argv without shell", func(t *testing.T) {
		secret := secretCommand(t, "echo", "$(not-a-command)", "a b", ";")
		if value, err := secret.Take(context.Background()); err != nil || value != "$(not-a-command)|a b|;" {
			t.Fatalf("read = %q, %v", value, err)
		}
	})
	for _, tt := range []struct{ mode, want string }{{"echo", "is empty"}, {"fail", "code 7"}, {"large", "exceeds 1 MiB"}} {
		t.Run(tt.mode, func(t *testing.T) {
			secret := secretCommand(t, tt.mode)
			value, err := secret.Take(context.Background())
			checkError(t, err, tt.want)
			if value != "" || strings.Contains(err.Error(), "SENSITIVE") {
				t.Fatal("subcommand exposed secret output")
			}
		})
	}
	t.Run("timeout", func(t *testing.T) {
		secret := secretCommand(t, "sleep")
		secret.Timeout = "100ms"
		_, err := secret.Take(context.Background())
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("timeout error = %v", err)
		}
	})
	t.Run("cancellation", func(t *testing.T) {
		secret := secretCommand(t, "sleep")
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(100*time.Millisecond, cancel)
		defer cancel()
		_, err := secret.Take(ctx)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel error = %v", err)
		}
	})
	t.Run("missing executable", func(t *testing.T) {
		secret := &Secret{Src: "subcommand", Command: []string{filepath.Join(t.TempDir(), "missing")}}
		_, err := secret.Take(context.Background())
		checkError(t, err, "subcommand failed")
	})
	t.Run("concurrent cache", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "calls")
		secret := secretCommand(t, "count", path)
		secret.TTL = "1h"
		var wg sync.WaitGroup
		for range 20 {
			wg.Go(func() {
				value, err := secret.Take(context.Background())
				if err != nil || value != "token" {
					t.Errorf("read = %q, %v", value, err)
				}
			})
		}
		wg.Wait()
		calls, err := os.ReadFile(path)
		if err != nil || string(calls) != "x" {
			t.Fatalf("executions = %q, %v", calls, err)
		}
	})
	t.Run("canceled waiter", func(t *testing.T) {
		secret := secretCommand(t, "count", filepath.Join(t.TempDir(), "calls"))
		done := make(chan struct{})
		go func() { defer close(done); _, _ = secret.Take(context.Background()) }()
		deadline := time.Now().Add(time.Second)
		for {
			secret.mu.Lock()
			flight := secret.flight
			secret.mu.Unlock()
			if flight != nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("read did not start")
			}
			time.Sleep(time.Millisecond)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		if _, err := secret.Take(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("waiter error = %v", err)
		}
		<-done
	})
}
