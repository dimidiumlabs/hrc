// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	jobdata "dimidiumlabs.io/hrc/cmd/hrcp-github/queue/internal/job"
	"dimidiumlabs.io/hrc/cmd/hrcp-github/queue/internal/listener"
	hrcconfig "dimidiumlabs.io/hrc/internal/config"
	"dimidiumlabs.io/hrc/internal/provider"

	"github.com/actions/scaleset"
)

// acquire registers independently of any workspace or execution process.
// The resulting Job can describe a Run for a separately configured executor.
func (q *Queue) acquire(ctx context.Context, client *scaleset.Client, scaleSetID int) (*handle, error) {
	jit, err := client.GenerateJitRunnerConfig(ctx, &scaleset.RunnerScaleSetJitRunnerSetting{
		Name: q.config.ID + "-runner-" + rand.Text(), WorkFolder: ".",
	}, scaleSetID)
	if err != nil {
		return nil, fmt.Errorf("generate runner config: %w", err)
	}
	registration, err := parseRunnerRegistration(jit.EncodedJITConfig)
	if err != nil {
		return nil, fmt.Errorf("decode runner config: %w", err)
	}
	api, err := listener.NewClient(listener.ClientConfig{
		BrokerURL: registration.settings.ServerURLV2, Version: q.config.RunnerVersion,
		Credentials: registration.credentials, PrivateKey: registration.key,
	})
	if err != nil {
		return nil, fmt.Errorf("create runner API: %w", err)
	}
	networkConfig := registration.listenerConfig(q.config.RunnerVersion)
	networkConfig.OS, networkConfig.Architecture = q.config.RunnerOS, q.config.RunnerArchitecture
	network, err := listener.New(api, networkConfig)
	if err != nil {
		return nil, errors.Join(err, api.Close())
	}
	raw, err := network.WaitJob(ctx)
	j := &handle{
		registration: registration, network: network, api: api, raw: raw,
		events: make(chan provider.Event), closed: make(chan struct{}),
	}
	go j.watch()
	if err != nil {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), hrcconfig.JobCleanupTimeout)
		defer cancel()
		if raw != nil {
			err = errors.Join(err, j.Finish(cleanup, provider.Failed, nil))
		}
		closeErr := j.Close(cleanup)
		if errors.Is(err, listener.ErrShutdown) || errors.Is(err, listener.ErrJobUnavailable) {
			return nil, closeErr
		}
		return nil, errors.Join(err, closeErr)
	}
	return j, nil
}

type handle struct {
	registration *runnerRegistration
	network      *listener.Listener
	api          *listener.Client
	raw          *listener.RunJob
	events       chan provider.Event
	closed       chan struct{}
	closeOnce    sync.Once
	closeErr     error
	reservation  provider.Reservation
	running      *atomic.Int32
}

func (j *handle) Job() provider.Job {
	return &job{Key: j.raw.JobID, Settings: j.registration.settings, Payload: append(json.RawMessage(nil), j.raw.Raw...)}
}
func (j *handle) Events() <-chan provider.Event { return j.events }

func (j *handle) watch() {
	defer close(j.events)
	for {
		select {
		case event, ok := <-j.network.Events():
			if !ok {
				return
			}
			converted := provider.Event{Err: event.Err}
			switch event.Type {
			case listener.JobCancellation:
				converted.Type = provider.Cancellation
				if event.Timeout != nil {
					// GitHub grants at least a minute, reserving 15s for completion.
					converted.Grace = max(time.Minute, *event.Timeout) - 15*time.Second
				}
			case listener.RunnerShutdown:
				converted.Type = provider.Shutdown
			case listener.LeaseLost:
				converted.Type = provider.LeaseLost
				converted.Err = errors.Join(provider.ErrLeaseLost, event.Err)
			case listener.NetworkError:
				converted.Type = provider.NetworkError
				converted.Err = errors.Join(provider.ErrUnavailable, event.Err)
			default:
				converted.Type = provider.NetworkError
				converted.Err = fmt.Errorf("%w: unsupported runner network event", provider.ErrUnknown)
			}
			select {
			case j.events <- converted:
			case <-j.closed:
				return
			}
		case <-j.closed:
			return
		}
	}
}

type job = jobdata.Job

type runnerSettings = jobdata.RunnerSettings

func (j *handle) Finish(ctx context.Context, result provider.Result, data []byte) error {
	select {
	case <-j.closed:
		return provider.ErrClosed
	default:
	}
	// Execution reports normal .NET completion through provider-owned data.
	// No Worker pointer, process or slot crosses the Job/Run boundary.
	if len(data) == 1 && data[0] == 1 {
		return nil
	}
	if !result.Valid() || result == provider.Succeeded || result == provider.SucceededWithIssues {
		result = provider.Failed
	}
	request := &listener.CompleteJobRequest{Conclusion: result.String()}
	err := j.network.CompleteJob(ctx, request)
	if errors.Is(err, listener.ErrClosed) {
		return errors.Join(provider.ErrClosed, err)
	}
	if err != nil {
		return errors.Join(provider.ErrUnavailable, err)
	}
	return nil
}

func (j *handle) Close(ctx context.Context) error {
	j.closeOnce.Do(func() {
		close(j.closed)
		j.closeErr = errors.Join(j.network.Close(ctx), j.api.Close())
		if j.reservation != nil {
			// Update the group before advertising newly available capacity.
			j.running.Add(-1)
			j.closeErr = errors.Join(j.closeErr, j.reservation.Close())
		}
	})
	return providerError(j.closeErr)
}

func providerError(err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if errors.Is(err, listener.ErrClosed) {
		return errors.Join(provider.ErrClosed, err)
	}
	return errors.Join(provider.ErrUnknown, err)
}
