// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package space

import (
	"context"
	"net"
	"sync"

	hrcconfig "dimidiumlabs.io/hrc/internal/config"
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
		// timeout would otherwise retire a healthy monitor generation.
		grpc.WithIdleTimeout(0),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			var accepted net.Conn
			once.Do(func() { accepted = conn })
			if accepted == nil {
				return nil, net.ErrClosed
			}
			return accepted, nil
		}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(hrcconfig.GRPCMaxMessageSize), grpc.MaxCallSendMsgSize(hrcconfig.GRPCMaxMessageSize)),
	)
}
