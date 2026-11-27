// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package resource

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestPartialCollectionRetainsInfrastructure(t *testing.T) {
	var g Heap
	var released []string
	queue := own(t, &g, &resource{shutdown: func(context.Context, Level) error {
		released = append(released, "queue")
		return nil
	}})
	job := own(t, &g, &resource{shutdown: func(context.Context, Level) error {
		released = append(released, "job")
		return nil
	}}, queue)
	if err := g.Request(job, Down); err != nil {
		t.Fatal(err)
	}
	if err := g.Collect(t.Context()); err != nil || g.Len() != 1 || !reflect.DeepEqual(released, []string{"job"}) {
		t.Fatalf("remaining=%d released=%v error=%v", g.Len(), released, err)
	}
	// Releasing a completed consumer does not seal registration or destroy
	// an unused dependency that its owner still needs.
	own(t, &g, &resource{}, queue)
	if err := g.Request(job, Kill); err != nil {
		t.Fatal("requesting an already collected object must be harmless", err)
	}
	if err := g.Shutdown(t.Context(), Down); err != nil || g.Len() != 0 {
		t.Fatal(err)
	}
}

func TestFailedFinalizationAndScopeExpansion(t *testing.T) {
	var g Heap
	failure := errors.New("finalization failed")
	var levels []Level
	queue := own(t, &g, &resource{})
	finalizer := &resource{shutdown: func(_ context.Context, level Level) error {
		levels = append(levels, level)
		if level < Kill {
			return failure
		}
		return nil
	}}
	job := own(t, &g, finalizer, queue)
	if err := g.Request(job, Down); err != nil {
		t.Fatal(err)
	}
	err := g.Collect(t.Context())
	var identified *Error
	if !errors.As(err, &identified) || identified.Ref != job || identified.Resource != finalizer || !errors.Is(err, failure) || g.Len() != 2 {
		t.Fatalf("resource identification/ownership lost: %v", err)
	}
	if err := g.Request(queue, Term); err != nil {
		t.Fatal(err)
	}
	if err := g.Request(job, Term); err != nil {
		t.Fatal(err)
	}
	if err := g.Collect(t.Context()); !errors.Is(err, failure) || g.Len() != 2 {
		t.Fatalf("dependency released before finalization: %v", err)
	}
	if err := g.Request(job, Kill); err != nil {
		t.Fatal(err)
	}
	if err := g.Collect(t.Context()); err != nil || g.Len() != 0 {
		t.Fatalf("requested dependency did not cascade: %v", err)
	}
	if !reflect.DeepEqual(levels, []Level{Down, Term, Kill}) {
		t.Fatal(levels)
	}
}

func TestAcquisitionCanAdoptBeforeDependencyReleaseStarts(t *testing.T) {
	var g Heap
	queue := own(t, &g, &resource{})
	acquisition := own(t, &g, &resource{}, queue)
	if err := g.Request(queue, Term); err != nil {
		t.Fatal(err)
	}
	if err := g.Collect(t.Context()); err != nil || g.Len() != 2 {
		t.Fatal("live acquisition must retain the queue", err)
	}
	job := own(t, &g, &resource{}, queue)
	g.Request(acquisition, Down)
	if err := g.Collect(t.Context()); err != nil || g.Len() != 2 {
		t.Fatal("in-flight job not adopted before acquisition released", err)
	}
	g.Request(job, Term)
	if err := g.Collect(t.Context()); err != nil || g.Len() != 0 {
		t.Fatal(err)
	}
}

func TestReleaseStatesAndIndependentQuiesce(t *testing.T) {
	for _, quiesceFirst := range []bool{false, true} {
		name := "quiesce-after-release-starts"
		if quiesceFirst {
			name = "quiesce-before-request"
		}
		t.Run(name, func(t *testing.T) {
			var g Heap
			var ref *Ref
			failure := errors.New("release failed")
			ref = own(t, &g, &resource{shutdown: func(_ context.Context, level Level) error {
				if ref.state != releasing {
					t.Fatalf("callback state=%v, want releasing", ref.state)
				}
				if level < Kill {
					return failure
				}
				return nil
			}})
			if ref.state != owned || ref.quiesced {
				t.Fatal("new resource must be owned and not quiesced")
			}
			if quiesceFirst {
				if err := g.Quiesce(t.Context()); err != nil || ref.state != owned || !ref.quiesced {
					t.Fatalf("quiesce changed release state: %v", err)
				}
			}
			if err := g.Request(ref, Down); err != nil || ref.state != requested || ref.quiesced != quiesceFirst {
				t.Fatalf("request state=%v quiesced=%v error=%v", ref.state, ref.quiesced, err)
			}
			if err := g.Collect(t.Context()); !errors.Is(err, failure) || ref.state != releasing || ref.quiesced != quiesceFirst {
				t.Fatalf("failed release state=%v quiesced=%v error=%v", ref.state, ref.quiesced, err)
			}
			if err := g.Request(ref, Term); err != nil || ref.state != releasing {
				t.Fatalf("request regressed release state: %v", err)
			}
			if err := g.Quiesce(t.Context()); err != nil || ref.state != releasing || !ref.quiesced {
				t.Fatalf("quiesce after failed release: %v", err)
			}
			if err := g.Shutdown(t.Context(), Kill); err != nil || ref.state != released || ref.resource != nil || g.Len() != 0 {
				t.Fatalf("successful release state=%v error=%v", ref.state, err)
			}
			if err := g.Request(ref, Down); err != nil || ref.state != released {
				t.Fatalf("request resurrected released resource: %v", err)
			}
		})
	}
}

func TestCannotAttachToResourceWhoseReleaseFailed(t *testing.T) {
	var g Heap
	ref := own(t, &g, &resource{shutdown: func(context.Context, Level) error {
		return errors.New("release failed")
	}})
	g.Request(ref, Down)
	if err := g.Collect(t.Context()); err == nil {
		t.Fatal("expected failure")
	}
	if _, err := g.Add(&resource{}, ref); err == nil {
		t.Fatal("new consumer attached after release started")
	}
}
