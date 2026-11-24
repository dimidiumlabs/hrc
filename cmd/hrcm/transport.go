// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"net"
	"sync"
)

// A scheduler serves one connection and stops when its transport is lost.
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

type singleListener struct {
	conn      net.Conn
	once      sync.Once
	closeOnce sync.Once
	done      chan struct{}
}

var _ net.Listener = (*singleListener)(nil)

func (l *singleListener) Accept() (net.Conn, error) {
	var conn net.Conn
	l.once.Do(func() { conn = l.conn })
	if conn != nil {
		return conn, nil
	}
	<-l.done
	return nil, net.ErrClosed
}

func (l *singleListener) Close() error   { l.closeOnce.Do(func() { close(l.done) }); return nil }
func (l *singleListener) Addr() net.Addr { return l.conn.LocalAddr() }
