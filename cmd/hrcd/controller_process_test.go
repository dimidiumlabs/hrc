// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"os"
	"testing"
	"time"

	"dimidiumlabs.io/hrc/internal/resource"
	"dimidiumlabs.io/hrc/internal/runtime"
)

func TestControllerHeapProcessPeer(t *testing.T) {
	if os.Getenv("HRC_RESOURCE_PROCESS_PEER") != "1" {
		return
	}
	conn, err := runtime.InheritedConnection(flag.Args())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	io.Copy(io.Discard, conn)
}

type unreleasableResource struct{ err error }

func (*unreleasableResource) Quiesce(context.Context) error                    { return nil }
func (r *unreleasableResource) Shutdown(context.Context, resource.Level) error { return r.err }

func TestControllerOwnsProcessesThroughResourceCollection(t *testing.T) {
	for _, broken := range []bool{false, true} {
		name := "normal"
		if broken {
			name = "force-with-retained-consumer"
		}
		t.Run(name, func(t *testing.T) {
			self, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			processes := &runtime.Pool{}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			child, err := processes.Start(ctx, runtime.ChildSpec{
				Command: runtime.Command{Path: self, Args: []string{"-test.run=^TestControllerHeapProcessPeer$"}, Env: append(os.Environ(), "HRC_RESOURCE_PROCESS_PEER=1")},
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { child.Close(0) })
			c := &Controller{forges: []managedForge{{id: "peer", processes: processes}}}
			if err := c.initializeOwnership(); err != nil {
				t.Fatal(err)
			}
			failure := errors.New("consumer cannot be released")
			if broken {
				if _, err := c.resources.Add(&unreleasableResource{failure}, c.forges[0].ref); err != nil {
					t.Fatal(err)
				}
			}
			err = c.Close()
			if broken {
				if !errors.Is(err, failure) || c.resources.Len() != 2 {
					t.Fatalf("failed obligations forgotten: remaining=%d error=%v", c.resources.Len(), err)
				}
			} else if err != nil || c.resources.Len() != 0 {
				t.Fatalf("normal collection: remaining=%d error=%v", c.resources.Len(), err)
			}
			select {
			case <-child.Done():
			default:
				t.Fatal("controller left a reachable process alive")
			}
			if len(processes.Children()) != 0 {
				t.Fatal("child cleanup did not release process membership")
			}
		})
	}
}
