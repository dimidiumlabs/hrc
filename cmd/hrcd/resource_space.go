// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"context"
	"fmt"

	"dimidiumlabs.io/hrc/internal/provider"
	"dimidiumlabs.io/hrc/internal/resource"
	"dimidiumlabs.io/hrc/internal/runtime"
	"dimidiumlabs.io/hrc/internal/space"
)

type managedSpace struct {
	id          string
	level       resource.Level
	environment space.Space
	demand      provider.Demand
	processes   *runtime.Pool
	ref         *resource.Ref
}

func (s *managedSpace) Quiesce(context.Context) error { return nil }
func (s *managedSpace) Shutdown(ctx context.Context, level resource.Level) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if level == resource.Kill {
		if err := collectProcesses(ctx, s.processes, level); err != nil {
			return err
		}
	}
	if err := s.environment.Destroy(); err != nil {
		return fmt.Errorf("close space %q: %w", s.id, err)
	}
	return collectProcesses(ctx, s.processes, level)
}
