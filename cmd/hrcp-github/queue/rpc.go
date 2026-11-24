// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"encoding/json/v2"
	"errors"
	"net"
	"time"

	hrcconfig "dimidiumlabs.io/hrc/internal/config"
	"dimidiumlabs.io/hrc/internal/provider"
)

func ServeQueue(ctx context.Context, conn net.Conn) error {
	client := &provider.QueueClient{New: func(start *provider.QueueStart) (provider.Queue, error) {
		var config ForgeConfig
		if err := json.Unmarshal(start.Config, &config, json.RejectUnknownMembers(true)); err != nil {
			return nil, errors.Join(provider.ErrInvalidConfig, err)
		}
		config.ID = hrcconfig.ID(start.Name)
		queue, err := config.NewQueue(start.Instance, start.Version)
		if err != nil {
			return nil, errors.Join(provider.ErrInvalidConfig, err)
		}
		return retryQueue{queue}, nil
	}}
	return client.Connect(ctx, conn)
}

// Retry acquisition without replacing the process that owns delivered handles.
type retryQueue struct{ provider.Queue }

func (q retryQueue) Serve(ctx context.Context, demand provider.Demand) <-chan provider.Handle {
	jobs := make(chan provider.Handle)
	go func() {
		defer close(jobs)
		delay := hrcconfig.QueueRetryMin
		for ctx.Err() == nil {
			for handle := range q.Queue.Serve(ctx, demand) {
				select {
				case jobs <- handle:
					delay = hrcconfig.QueueRetryMin
				case <-ctx.Done():
					cleanup, cancel := context.WithTimeout(context.Background(), hrcconfig.JobCleanupTimeout)
					handle.Finish(cleanup, provider.Canceled, nil)
					handle.Close(cleanup)
					cancel()
				}
			}
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			delay = min(delay*2, hrcconfig.QueueRetryMax)
		}
	}()
	return jobs
}
