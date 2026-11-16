// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package github

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"dimidiumlabs.io/hrc/internal/provider"
	"dimidiumlabs.io/hrc/internal/provider-github/worker"
)

// Run executes a GitHub Job. The slot is already exclusive to this
// execution; credentials and Listener supervision stay with Handle in hrcd.
func (c WorkerConfig) Run(ctx context.Context, target worker.Environment, description provider.Job) (provider.Run, error) {
	payload, ok := description.(*job)
	if !ok || payload == nil {
		return nil, fmt.Errorf("%w: GitHub job is required", provider.ErrUnsupportedJob)
	}
	if err := payload.validate(); err != nil {
		return nil, err
	}
	if target == nil {
		return nil, fmt.Errorf("%w: execution slot is required", provider.ErrInvalidConfig)
	}
	if c.Path == "" {
		c.Path = "hrcw-github"
	}
	registration := runnerRegistration{settings: payload.Settings}
	config := registration.workerConfig(target.Cwd())
	config.Environment = target
	config.Path, config.Dir, config.Env = c.Path, c.Dir, c.Env
	config.Stdout, config.Stderr = c.Stdout, c.Stderr
	local, err := worker.New(config)
	if err != nil {
		return nil, fmt.Errorf("%w: create Worker: %w", provider.ErrInvalidConfig, err)
	}
	logRoot := c.LogRoot
	if logRoot == "" {
		logRoot = target.Path(target.Cwd(), "_diag")
	}
	diag := target.Path(logRoot, "jobs", payload.ID())
	if err := local.StartJob(ctx, payload.Payload, diag); err != nil {
		return nil, providerError(errors.Join(err, local.Close()))
	}
	r := &run{local: local, done: make(chan error, 1), finished: make(chan struct{})}
	go func() {
		result, data, err := r.wait()
		r.result, r.data = result, data
		r.cleanupErr = providerError(local.Close())
		err = errors.Join(err, r.cleanupErr)
		close(r.finished)
		r.done <- err
		close(r.done)
	}()
	return r, nil
}

type run struct {
	local      *worker.Worker
	done       chan error
	finished   chan struct{}
	result     provider.Result
	data       []byte
	stopOnce   sync.Once
	cleanupErr error
}

func (r *run) Done() <-chan error { return r.done }
func (r *run) Result() (provider.Result, []byte) {
	select {
	case <-r.finished:
		return r.result, r.data
	default:
		return provider.Unknown, nil
	}
}
func (r *run) wait() (provider.Result, []byte, error) {
	result, err := r.local.Wait()
	mapped := provider.Unknown
	var data []byte
	switch result {
	case worker.Succeeded:
		mapped = provider.Succeeded
	case worker.SucceededWithIssues:
		mapped = provider.SucceededWithIssues
	case worker.Failed:
		mapped = provider.Failed
	case worker.Canceled:
		mapped = provider.Canceled
	case worker.Skipped:
		mapped = provider.Skipped
	case worker.Abandoned:
		mapped = provider.Abandoned
	}
	if result.Valid() {
		data = []byte{1}
	}
	return mapped, data, providerError(err)
}
func (r *run) Stop(ctx context.Context, reason provider.StopReason) error {
	switch reason {
	case provider.StopCancel:
		return providerError(r.local.CancelJob(ctx))
	case provider.StopShutdown:
		return providerError(r.local.Shutdown(ctx, worker.ShutdownRequested))
	case provider.StopForce:
		r.stopOnce.Do(func() { go r.local.Close() })
		select {
		case <-r.finished:
			return r.cleanupErr
		case <-ctx.Done():
			return ctx.Err()
		}
	default:
		return provider.ErrInvalidStopReason
	}
}
