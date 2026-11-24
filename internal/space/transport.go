// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package space

import (
	"context"
	"net"
	"sync"

	"dimidiumlabs.io/hrc/internal/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// A generation has exactly one connection. gRPC must never reconnect/replay Jobs.
func dial(conn net.Conn) (*grpc.ClientConn, error) {
	var once sync.Once
	return grpc.NewClient("passthrough:///space",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDisableRetry(),
		// This transport cannot reconnect. The default 30-minute idle
		// timeout would otherwise retire a healthy scheduler generation.
		grpc.WithIdleTimeout(0),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			var accepted net.Conn
			once.Do(func() { accepted = conn })
			if accepted == nil {
				return nil, net.ErrClosed
			}
			return accepted, nil
		}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(config.GRPCMaxMessageSize), grpc.MaxCallSendMsgSize(config.GRPCMaxMessageSize)),
	)
}

// Closing the transport ends this scheduler generation, including idle periods.
type watchedConn struct {
	net.Conn
	once sync.Once
	done chan struct{}
}

var _ net.Conn = (*watchedConn)(nil)

func (c *watchedConn) Read(data []byte) (int, error) {
	n, err := c.Conn.Read(data)
	if err != nil {
		c.once.Do(func() { close(c.done) })
	}
	return n, err
}

func (c *watchedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() { close(c.done) })
	return err
}
