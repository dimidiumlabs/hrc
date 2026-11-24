// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"context"
	"encoding/json/v2"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"

	runnerv1 "code.forgejo.org/forgejo/actions-proto/runner/v1"
	"code.forgejo.org/forgejo/runner/v13/hrcbridge"
	"dimidiumlabs.io/hrc/internal/provider"
)

func main() {
	socket := flag.String("connect", "", "HRC parent socket")
	flag.Parse()
	if *socket == "" {
		flag.Usage()
		os.Exit(1)
	}
	ctx := context.Background()
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", *socket)
	if err == nil {
		err = (&provider.WorkerClient{Start: start}).Connect(ctx, conn)
	}
	if err != nil {
		slog.Error("Forgejo worker stopped", "err", err)
		os.Exit(1)
	}
}

func start(parent context.Context, task *provider.TaskStart) (provider.Run, error) {
	var assignment bridge.Assignment
	if err := json.Unmarshal(task.Payload, &assignment, json.RejectUnknownMembers(true)); err != nil {
		return nil, fmt.Errorf("%w: malformed assignment", provider.ErrInvalidJob)
	}
	if len(assignment.Task) == 0 || assignment.Connection.URL == "" || assignment.Connection.UUID == "" || assignment.Connection.Token == "" {
		return nil, provider.ErrInvalidJob
	}
	var settings struct{ Dir, LogRoot string }
	if err := json.Unmarshal(task.Config, &settings, json.RejectUnknownMembers(true)); err != nil {
		return nil, fmt.Errorf("%w: malformed worker settings: %v", provider.ErrInvalidConfig, err)
	}
	if settings.LogRoot == "" {
		settings.LogRoot = "_diag"
	}
	root := filepath.Join(settings.LogRoot, "jobs")
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, fmt.Errorf("create diagnostics root: %w", err)
	}
	diag, err := os.MkdirTemp(root, "forgejo-")
	if err != nil {
		return nil, fmt.Errorf("create job diagnostics: %w", err)
	}
	log, err := os.OpenFile(filepath.Join(diag, "worker.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, fmt.Errorf("open job diagnostics: %w", err)
	}
	logger := slog.New(slog.NewTextHandler(log, &slog.HandlerOptions{Level: slog.LevelDebug}))
	workdir, err := os.Getwd()
	if err != nil {
		log.Close()
		return nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	r := &run{cancel: cancel, done: make(chan error, 1), finished: make(chan struct{})}
	go func() {
		defer cancel()
		logger.Debug("Starting Forgejo task", "workdir", workdir, "runtime", settings.Dir)
		outcome, err := bridge.Execute(ctx, assignment.Connection, bridge.Execution{Workdir: workdir, Dir: settings.Dir, Log: log}, assignment.Task)
		logger.Debug("Finished Forgejo execution", "result", outcome.String(), "err", err)
		r.mu.Lock()
		if err == nil {
			switch outcome {
			case runnerv1.Result_RESULT_SUCCESS:
				r.result = provider.Succeeded
			case runnerv1.Result_RESULT_FAILURE:
				r.result = provider.Failed
			case runnerv1.Result_RESULT_CANCELLED:
				r.result = provider.Canceled
			case runnerv1.Result_RESULT_SKIPPED:
				r.result = provider.Skipped
			default:
				r.result = provider.Unknown
			}
			r.data = []byte(bridge.Reported)
		}
		r.mu.Unlock()
		if closeErr := log.Close(); err == nil && closeErr != nil {
			err = fmt.Errorf("close diagnostics: %w", closeErr)
		}
		close(r.finished)
		r.done <- err
		close(r.done)
	}()
	return r, nil
}

type run struct {
	cancel   context.CancelFunc
	done     chan error
	finished chan struct{}
	mu       sync.Mutex
	result   provider.Result
	data     []byte
}

func (r *run) Done() <-chan error { return r.done }
func (r *run) Result() (provider.Result, []byte) {
	select {
	case <-r.finished:
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.result, append([]byte(nil), r.data...)
	default:
		return provider.Unknown, nil
	}
}
func (r *run) Stop(ctx context.Context, reason provider.StopReason) error {
	if reason > provider.StopForce {
		return provider.ErrInvalidStopReason
	}
	r.cancel()
	if reason != provider.StopForce {
		return nil
	}
	select {
	case <-r.finished:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
