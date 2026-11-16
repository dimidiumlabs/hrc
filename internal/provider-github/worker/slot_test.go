// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package worker

import (
	"context"
	"encoding/binary"
	"io"
	"strings"
	"sync"
	"testing"
)

// This slot's paths do not exist in the scheduler's filesystem.
type remoteSlot struct {
	config  Config
	env     []string
	process *remoteProcess
}

func (*remoteSlot) Cwd() string                 { return `Z:\work` }
func (*remoteSlot) Path(parts ...string) string { return strings.Join(parts, `\`) }
func (*remoteSlot) Close() error                { return nil }
func (s *remoteSlot) StartWorker(_ context.Context, config Config, env []string) (Process, io.WriteCloser, error) {
	s.config, s.env = config, env
	read, write := io.Pipe()
	p := &remoteProcess{input: write, done: make(chan struct{})}
	s.process = p
	go func() {
		defer read.Close()
		defer p.finish()
		var header [8]byte
		if _, err := io.ReadFull(read, header[:]); err != nil {
			return
		}
		n := binary.LittleEndian.Uint32(header[4:])
		io.CopyN(io.Discard, read, int64(n))
	}()
	return p, write, nil
}

type remoteProcess struct {
	input *io.PipeWriter
	done  chan struct{}
	once  sync.Once
}

func (p *remoteProcess) finish()               { p.once.Do(func() { close(p.done) }) }
func (p *remoteProcess) Stdin() io.WriteCloser { return p.input }
func (p *remoteProcess) Wait() (int, error)    { <-p.done; return 100, nil }
func (p *remoteProcess) Kill() error           { p.input.Close(); p.finish(); return nil }
func (p *remoteProcess) Close() error          { return nil }

func TestWorkerUsesTargetFilesystemAndStreams(t *testing.T) {
	target := &remoteSlot{}
	w, err := New(Config{Environment: target, Path: `Z:\bin\hrcw-github.exe`, Dir: `Z:\runner`, Name: "target"})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.StartJob(t.Context(), []byte(`{"unknown":"preserved"}`), `Z:\logs\job`); err != nil {
		t.Fatal(err)
	}
	if result, err := w.Wait(); err != nil || result != Succeeded {
		t.Fatalf("remote slot: %v, %v", result, err)
	}
	if target.config.WorkDir != target.Cwd() || target.config.Dir != `Z:\runner` || target.config.Path != `Z:\bin\hrcw-github.exe` {
		t.Fatalf("Worker target paths lost: %+v", target.config)
	}
}
