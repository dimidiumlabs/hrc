// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"context"
	"errors"
	"log/slog"

	hrcconfig "dimidiumlabs.io/hrc/internal/config"
	"dimidiumlabs.io/hrc/internal/resource"
	"dimidiumlabs.io/hrc/internal/runtime"
)

// runtime.Pool remains the low-level child membership mechanism inside each
// queue/space, not an independent daemon-wide owner. Domains share no processes.
func collectProcesses(ctx context.Context, processes *runtime.Pool, level resource.Level) error {
	if processes == nil {
		return nil
	}
	var result error
	for _, child := range processes.Children() {
		grace := hrcconfig.ProcessExitTimeout
		if level == resource.Kill || ctx.Err() != nil {
			grace = 0
		}
		abort := context.AfterFunc(ctx, func() { child.Kill() })
		exit, cleanup := child.Close(grace)
		abort()
		if exit != nil {
			slog.Warn("Child exited during resource collection", "err", exit)
		}
		result = errors.Join(result, cleanup)
	}
	return result
}
