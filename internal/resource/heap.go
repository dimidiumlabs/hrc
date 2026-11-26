// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

// Package resource owns resources and releases consumers before their dependencies.
// The caller chooses shutdown levels, deadlines, failure scope and recovery.
package resource

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
)

// Level describes requested shutdown, not observed resource health.
type Level uint8

const (
	Down Level = iota // Finish accepted work, including any remaining finalization.
	Term              // Cancel work and perform available cleanup.
	Kill              // Use available forced cleanup.
)

// Resource separates admission control from release. Methods must honor ctx,
// tolerate retries and escalation, and preserve completed cleanup steps. A nil
// Shutdown result discharges all release obligations; an error retains ownership.
type Resource interface {
	Quiesce(context.Context) error
	Shutdown(context.Context, Level) error
}

type state uint8

const (
	owned state = iota
	requested
	releasing // Release has started; errors retain this state for retries.
	released
)

// Ref identifies an owned resource. Only registered consumers, not Go references
// to Ref, retain dependencies. A zero consumer count alone does not request collection.
type Ref struct {
	owner        *Heap
	id           uint64
	resource     Resource
	dependencies []*Ref
	users        int
	quiesced     bool
	state        state
	level        Level
}

// Error identifies the object and operation which failed. Unwrap preserves the
// underlying cause. Errors from independent objects are joined, not discarded.
type Error struct {
	Ref       *Ref
	Resource  Resource
	Operation string
	Level     Level
	Err       error
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s resource %d: %v", e.Operation, e.Ref.id, e.Err)
}
func (e *Error) Unwrap() error { return e.Err }

// Heap is an owning collection; the zero value is ready for use and must not be
// copied. Resources must be transferred exactly once. Request and Add may run
// concurrently with callbacks, but lifecycle passes are serialized. Callbacks
// must not recursively call Collect, Quiesce or Shutdown on the same collection.
type Heap struct {
	mu     sync.Mutex
	gate   chan struct{}
	sealed bool
	next   uint64
	level  Level
	refs   []*Ref
}

// Add transfers ownership. Dependencies must be distinct, live resources whose
// release has not started. Requested dependencies may still serve accepted work;
// its acquisition owners must retain them until handoff finishes. This graph
// construction cannot create cycles.
// On error, ownership remains with the caller.
func (g *Heap) Add(resource Resource, dependencies ...*Ref) (*Ref, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.sealed {
		return nil, errors.New("resource collection is sealed")
	}
	if resource == nil {
		return nil, errors.New("resource is required")
	}
	seen := make(map[*Ref]struct{}, len(dependencies))
	for _, dependency := range dependencies {
		if dependency == nil || dependency.owner != g || dependency.state >= releasing || !slices.Contains(g.refs, dependency) {
			return nil, errors.New("dependency must be a live resource whose release has not started")
		}
		if _, duplicate := seen[dependency]; duplicate {
			return nil, errors.New("duplicate dependency")
		}
		seen[dependency] = struct{}{}
	}
	g.next++
	ref := &Ref{owner: g, id: g.next, resource: resource, dependencies: append([]*Ref(nil), dependencies...)}
	for _, dependency := range ref.dependencies {
		dependency.users++
	}
	g.refs = append(g.refs, ref)
	return ref, nil
}

// Len includes failed resources and their retained dependencies.
func (g *Heap) Len() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.refs)
}

// Request marks one object for collection without collecting its dependencies
// or consumers. Already collected objects are harmless; levels never decrease.
// Marking a dependency does not prevent existing consumers from finishing.
func (g *Heap) Request(ref *Ref, level Level) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if ref == nil || ref.owner != g || level > Kill || (ref.state != released && !slices.Contains(g.refs, ref)) {
		return errors.New("invalid collection request")
	}
	if ref.state != released {
		ref.state = max(ref.state, requested)
		ref.level = max(ref.level, level)
	}
	return nil
}

func (g *Heap) begin(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	g.mu.Lock()
	if g.gate == nil {
		g.gate = make(chan struct{}, 1)
		g.gate <- struct{}{}
	}
	gate := g.gate
	g.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-gate:
	}
	if err := ctx.Err(); err != nil {
		g.end()
		return err
	}
	return nil
}
func (g *Heap) end() { g.gate <- struct{}{} }

func (g *Heap) snapshot() []*Ref {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]*Ref(nil), g.refs...)
}

// Quiesce stops admission dependency-first, retrying failed callbacks. This is a
// snapshot: registration stays open for resources created by accepted work.
// Admission is controlled by resources, not the collection's registration gate.
func (g *Heap) Quiesce(ctx context.Context) error {
	if err := g.begin(ctx); err != nil {
		return err
	}
	defer g.end()
	var result error
	for _, ref := range g.snapshot() {
		if err := ctx.Err(); err != nil {
			return errors.Join(result, err)
		}
		if ref.quiesced {
			continue
		}
		if err := ref.resource.Quiesce(ctx); err != nil {
			result = errors.Join(result, &Error{Ref: ref, Resource: ref.resource, Operation: "quiesce", Err: err})
		} else {
			ref.quiesced = true
		}
	}
	return result
}

// Collect makes one pass over requested resources, consumers first. Failed
// consumers retain their dependencies. Successful ones release references, so
// requested dependencies can be collected in the same pass. Unrequested objects
// remain owned even when unused. Nil means no attempted operation failed, not
// that every requested object is free of consumers. Errors do not auto-escalate.
func (g *Heap) Collect(ctx context.Context) error {
	if err := g.begin(ctx); err != nil {
		return err
	}
	defer g.end()
	return g.collect(ctx)
}

func (g *Heap) collect(ctx context.Context) error {
	var result error
	refs := g.snapshot()
	for i := len(refs) - 1; i >= 0; i-- {
		if err := ctx.Err(); err != nil {
			return errors.Join(result, err)
		}
		ref := refs[i]
		g.mu.Lock()
		resource, level := ref.resource, ref.level
		ready := (ref.state == requested || ref.state == releasing) && ref.users == 0
		if ready {
			ref.state = releasing
		}
		g.mu.Unlock()
		if !ready {
			continue
		}
		if err := resource.Shutdown(ctx, level); err != nil {
			result = errors.Join(result, &Error{Ref: ref, Resource: resource, Operation: "shutdown", Level: level, Err: err})
			continue
		}
		g.mu.Lock()
		for _, dependency := range ref.dependencies {
			dependency.users--
		}
		ref.state = released
		ref.resource, ref.dependencies = nil, nil
		for index, owned := range g.refs {
			if owned == ref {
				copy(g.refs[index:], g.refs[index+1:])
				g.refs[len(g.refs)-1] = nil
				g.refs = g.refs[:len(g.refs)-1]
				break
			}
		}
		g.mu.Unlock()
	}
	return result
}

// Shutdown seals registration, requests collection of the whole graph, and makes
// a pass. It neither invokes Quiesce nor escalates automatically. Subsequent
// calls retry remaining resources at no less than the highest requested level.
func (g *Heap) Shutdown(ctx context.Context, level Level) error {
	if level > Kill {
		return errors.New("invalid shutdown level")
	}
	if err := g.begin(ctx); err != nil {
		return err
	}
	defer g.end()
	g.mu.Lock()
	g.sealed = true
	g.level = max(g.level, level)
	for _, ref := range g.refs {
		ref.state = max(ref.state, requested)
		ref.level = max(ref.level, g.level)
	}
	g.mu.Unlock()
	return g.collect(ctx)
}
