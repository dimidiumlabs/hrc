// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"encoding/binary"
	"io"
	"path/filepath"
	"testing"

	"dimidiumlabs.io/hrc/internal/provider"
	"dimidiumlabs.io/hrc/internal/provider-github/worker"
)

// The provider needs only an execution environment, not a concrete slot.
type runEnvironment struct {
	config worker.Config
	exit   int
}

func (*runEnvironment) Cwd() string                 { return "workspace" }
func (*runEnvironment) Path(parts ...string) string { return filepath.Join(parts...) }
func (e *runEnvironment) StartWorker(_ context.Context, c worker.Config, _ []string) (worker.Process, io.WriteCloser, error) {
	e.config = c
	read, write := io.Pipe()
	process := &runProcess{exit: e.exit, done: make(chan struct{}), read: read}
	go func() {
		defer close(process.done)
		defer read.Close()
		var header [8]byte
		if _, err := io.ReadFull(read, header[:]); err != nil {
			return
		}
		io.CopyN(io.Discard, read, int64(binary.LittleEndian.Uint32(header[4:])))
	}()
	return process, write, nil
}

type runProcess struct {
	exit int
	done chan struct{}
	read *io.PipeReader
}

func (p *runProcess) Wait() (int, error) { <-p.done; return p.exit, nil }
func (p *runProcess) Kill() error        { return p.read.Close() }
func (p *runProcess) Close() error       { return p.read.Close() }

func TestRunMapsWorkerOutcomeWithoutSlotImplementation(t *testing.T) {
	for _, tt := range []struct {
		exit     int
		result   provider.Result
		reported bool
	}{
		{100, provider.Succeeded, true}, {102, provider.Failed, true},
		{103, provider.Canceled, true}, {7, provider.Unknown, false},
	} {
		target := &runEnvironment{exit: tt.exit}
		run, err := (WorkerConfig{Dir: "assets"}).Run(t.Context(), target, &job{Key: "job", Payload: []byte(`{"opaque":true}`)})
		if err != nil {
			t.Fatal(err)
		}
		err = <-run.Done()
		result, data := run.Result()
		if result != tt.result || (err == nil) != tt.reported || (len(data) == 1) != tt.reported {
			t.Fatalf("exit %d: result=%v data=%v err=%v", tt.exit, result, data, err)
		}
		if target.config.Path != "hrcw-github" || target.config.Environment != target || target.config.WorkDir != target.Cwd() {
			t.Fatal("execution defaults or target lost")
		}
	}
}
