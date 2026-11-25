// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package runtime

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

// Connection is a control transport with lifecycle notification. Done closes
// on a read error (including EOF) or an explicit Close. It does not monitor the
// peer independently: the protocol must read the connection to detect loss.
// Process exit alone does not close Done or discard buffered transport data.
type Connection interface {
	net.Conn
	Done() <-chan struct{}
	// Listener adapts this connection for servers accepting a net.Listener.
	// It accepts once; closing the listener also closes the connection.
	Listener() net.Listener
}

var _ net.Listener = (*singleListener)(nil)

// WatchConnection adds lifecycle notification to an existing transport.
// Connections already implementing the contract are returned unchanged.
func WatchConnection(conn net.Conn) Connection {
	if watched, ok := conn.(Connection); ok {
		return watched
	}
	return &watchedConnection{Conn: conn, done: make(chan struct{})}
}

type watchedConnection struct {
	net.Conn
	once sync.Once
	done chan struct{}
}

func (c *watchedConnection) Done() <-chan struct{} { return c.done }
func (c *watchedConnection) Listener() net.Listener {
	return &singleListener{conn: c, done: make(chan struct{})}
}
func (c *watchedConnection) Read(data []byte) (int, error) {
	n, err := c.Conn.Read(data)
	if err != nil {
		c.once.Do(func() { close(c.done) })
	}
	return n, err
}
func (c *watchedConnection) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() { close(c.done) })
	return err
}

type singleListener struct {
	conn             Connection
	accepted, closed sync.Once
	done             chan struct{}
	err              error
}

func (l *singleListener) Accept() (net.Conn, error) {
	select {
	case <-l.done:
		return nil, net.ErrClosed
	default:
	}
	var conn net.Conn
	l.accepted.Do(func() { conn = l.conn })
	if conn != nil {
		return conn, nil
	}
	<-l.done
	return nil, net.ErrClosed
}
func (l *singleListener) Close() error {
	l.closed.Do(func() {
		l.err = l.conn.Close()
		close(l.done)
	})
	return l.err
}
func (l *singleListener) Addr() net.Addr { return l.conn.LocalAddr() }

// anonymousConnection prefers a socketpair, falling back only when unavailable.
func anonymousConnection() (net.Conn, *os.File, *os.File, error) {
	conn, child, err := socketPair()
	if err != nil {
		return nil, nil, nil, err
	}
	if conn != nil {
		return conn, child, child, nil
	}
	return pipeConnection()
}

func pipeConnection() (net.Conn, *os.File, *os.File, error) {
	input, writer, err := Pipe()
	if err != nil {
		return nil, nil, nil, err
	}
	reader, output, err := Pipe()
	if err != nil {
		input.Close()
		writer.Close()
		return nil, nil, nil, err
	}
	return &fileConnection{reader: reader, writer: writer}, input, output, nil
}

// InheritedConnection takes ownership of the read and write fd/handle values
// supplied as the child's final two positional arguments. Stdio is untouched.
// The endpoints are made non-inheritable before returning to the caller.
func InheritedConnection(args []string) (Connection, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("%w: expected read and write descriptors", os.ErrInvalid)
	}
	var descriptors [2]uintptr
	for i, arg := range args {
		value, err := strconv.ParseUint(arg, 10, strconv.IntSize)
		if err != nil || value <= 2 || uintptr(value) == ^uintptr(0) {
			return nil, fmt.Errorf("%w: invalid control descriptor", os.ErrInvalid)
		}
		descriptors[i] = uintptr(value)
	}
	input := os.NewFile(descriptors[0], "control-read")
	output := input
	if descriptors[1] != descriptors[0] {
		output = os.NewFile(descriptors[1], "control-write")
	}
	closeFiles := func() {
		input.Close()
		if output != input {
			output.Close()
		}
	}
	if input == nil || output == nil {
		closeFiles()
		return nil, os.ErrInvalid
	}
	if err := protectControlFiles(input, output); err != nil {
		closeFiles()
		return nil, err
	}
	conn, err := inheritedSocket(input)
	if err != nil {
		closeFiles()
		return nil, err
	}
	if conn != nil {
		closeFiles()
		return WatchConnection(conn), nil
	}
	return WatchConnection(&fileConnection{reader: input, writer: output}), nil
}

type fileConnection struct {
	reader, writer *os.File
	once           sync.Once
	err            error
}

func (c *fileConnection) Read(p []byte) (int, error)  { return c.reader.Read(p) }
func (c *fileConnection) Write(p []byte) (int, error) { return c.writer.Write(p) }
func (c *fileConnection) Close() error {
	c.once.Do(func() {
		c.err = c.reader.Close()
		if c.writer != c.reader {
			c.err = errors.Join(c.err, c.writer.Close())
		}
	})
	return c.err
}
func (*fileConnection) LocalAddr() net.Addr  { return controlAddress{} }
func (*fileConnection) RemoteAddr() net.Addr { return controlAddress{} }
func (c *fileConnection) SetDeadline(t time.Time) error {
	return errors.Join(c.SetReadDeadline(t), c.SetWriteDeadline(t))
}
func (c *fileConnection) SetReadDeadline(t time.Time) error  { return c.reader.SetReadDeadline(t) }
func (c *fileConnection) SetWriteDeadline(t time.Time) error { return c.writer.SetWriteDeadline(t) }

type controlAddress struct{}

func (controlAddress) Network() string { return "anonymous" }
func (controlAddress) String() string  { return "inherited" }
