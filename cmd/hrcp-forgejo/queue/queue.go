// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"context"
	"encoding/json/v2"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"time"

	runnerv1 "code.forgejo.org/forgejo/actions-proto/runner/v1"
	"code.forgejo.org/forgejo/runner/v13/hrcbridge"
	"dimidiumlabs.io/hrc/internal/provider"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

type queue struct{ connection bridge.Connection }

func pause(ctx context.Context) bool {
	timer := time.NewTimer(250 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (q *queue) Serve(ctx context.Context, demand provider.Demand) <-chan provider.Handle {
	jobs := make(chan provider.Handle)
	go func() {
		defer close(jobs)
		declare, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := q.connection.Declare(declare)
		cancel()
		if err != nil {
			slog.Error("Declare Forgejo runner", "err", err)
			return
		}
		for ctx.Err() == nil {
			reservation := demand.Reserve()
			if err := reservation.Wait(ctx); err != nil {
				reservation.Close()
				return
			}
			// Hold the reservation and key across ambiguous fetch failures.
			// A process restart is not recovered by this PoC.
			key := uuid.New()
			var task []byte
			for ctx.Err() == nil {
				fetch, cancel := context.WithTimeout(ctx, 10*time.Second)
				task, err = q.connection.Fetch(fetch, key)
				cancel()
				if err == nil || errors.Is(err, bridge.ErrNoTask) {
					break
				}
				slog.Warn("Fetch Forgejo task", "err", err)
				if !pause(ctx) {
					break
				}
			}
			if err != nil || len(task) == 0 {
				reservation.Close()
				if ctx.Err() != nil || !pause(ctx) {
					return
				}
				continue
			}
			var decoded runnerv1.Task
			if err := proto.Unmarshal(task, &decoded); err != nil {
				reservation.Close()
				return
			}
			payload, err := json.Marshal(bridge.Assignment{Connection: q.connection, Task: task})
			if err != nil {
				reservation.Close()
				return
			}
			h := &handle{
				connection: q.connection, task: task, reservation: reservation,
				job:    &provider.OpaqueJob{Key: strconv.FormatInt(decoded.Id, 10), Provider: "forgejo", Payload: payload},
				events: make(chan provider.Event),
			}
			select {
			case jobs <- h:
				slog.Info("Acquired Forgejo task", "task", decoded.Id)
			case <-ctx.Done():
				cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				h.Finish(cleanup, provider.Canceled, nil)
				h.Close(cleanup)
				cancel()
				return
			}
		}
	}()
	return jobs
}

type handle struct {
	connection  bridge.Connection
	task        []byte
	job         provider.Job
	events      chan provider.Event
	reservation provider.Reservation
	mu          sync.Mutex
	finished    bool
	closeOnce   sync.Once
	closeErr    error
}

func (h *handle) Job() provider.Job             { return h.job }
func (h *handle) Events() <-chan provider.Event { return h.events }
func (h *handle) Finish(ctx context.Context, result provider.Result, data []byte) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.finished {
		return nil
	}
	if string(data) != bridge.Reported {
		outcome := runnerv1.Result_RESULT_FAILURE
		if result == provider.Canceled || result == provider.Abandoned {
			outcome = runnerv1.Result_RESULT_CANCELLED
		}
		if err := h.connection.FinishUnreported(ctx, h.task, outcome); err != nil {
			return err
		}
	}
	h.finished = true
	slog.Info("Finished Forgejo task", "task", h.job.ID(), "result", result, "workerReported", string(data) == bridge.Reported)
	return nil
}
func (h *handle) Close(context.Context) error {
	h.closeOnce.Do(func() { h.closeErr = h.reservation.Close() })
	return h.closeErr
}
