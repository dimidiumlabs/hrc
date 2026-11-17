// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package spacehost

import (
	"context"
	"net"
	"os"
	"sync"
	"sync/atomic"

	"dimidiumlabs.io/hrc/internal/provider"
	"dimidiumlabs.io/hrc/internal/space"
	"google.golang.org/grpc"
)

// schedulerPeer implements only the messages needed by host lifecycle tests.
// Production scheduler behavior is tested in cmd/hrcs.
type schedulerPeer struct {
	capacity atomic.Uint64
	running  atomic.Uint64
}

var _ space.SchedulerServer = (*schedulerPeer)(nil)

func (p *schedulerPeer) Init(_ context.Context, request *space.InitRequest) (*space.InitResponse, error) {
	p.capacity.Store(request.Config.Capacity)
	return &space.InitResponse{Version: space.Version, Capacity: p.capacity.Load()}, nil
}

func (p *schedulerPeer) Inspect(context.Context, *space.InspectRequest) (*space.Snapshot, error) {
	return &space.Snapshot{Capacity: p.capacity.Load(), Running: p.running.Load()}, nil
}

func (p *schedulerPeer) Run(stream grpc.BidiStreamingServer[space.RunCommand, space.RunEvent]) error {
	command, err := stream.Recv()
	if err != nil {
		return err
	}
	start := command.GetStart()
	switch start.Provider {
	case "disconnect":
		os.Exit(0)
	case "crash":
		os.Exit(12)
	case "wait":
		p.running.Add(1)
		<-stream.Context().Done()
		return stream.Context().Err()
	}
	return stream.Send(&space.RunEvent{Event: &space.RunEvent_FinishResult{
		FinishResult: &space.FinishResult{Result: uint32(provider.Succeeded), Data: start.Payload},
	}})
}

func serveTestPeer(conn net.Conn) error {
	watched := &peerConn{Conn: conn, done: make(chan struct{})}
	listener := &peerListener{conn: watched, done: make(chan struct{})}
	server := grpc.NewServer(grpc.WaitForHandlers(true))
	space.RegisterSchedulerServer(server, &schedulerPeer{})
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	select {
	case <-watched.done:
		server.Stop()
		<-done
		return nil
	case err := <-done:
		server.Stop()
		return err
	}
}

type peerConn struct {
	net.Conn
	done chan struct{}
	once sync.Once
}

var _ net.Conn = (*peerConn)(nil)

func (c *peerConn) Read(data []byte) (int, error) {
	n, err := c.Conn.Read(data)
	if err != nil {
		c.once.Do(func() { close(c.done) })
	}
	return n, err
}

func (c *peerConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() { close(c.done) })
	return err
}

type peerListener struct {
	conn net.Conn
	done chan struct{}
	once sync.Once
	stop sync.Once
}

var _ net.Listener = (*peerListener)(nil)

func (l *peerListener) Accept() (net.Conn, error) {
	var conn net.Conn
	l.once.Do(func() { conn = l.conn })
	if conn != nil {
		return conn, nil
	}
	<-l.done
	return nil, net.ErrClosed
}

func (l *peerListener) Close() error   { l.stop.Do(func() { close(l.done) }); return nil }
func (l *peerListener) Addr() net.Addr { return l.conn.LocalAddr() }
