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
func InheritedConnection(args []string) (net.Conn, error) {
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
	if err != nil || conn != nil {
		closeFiles()
		return conn, err
	}
	return &fileConnection{reader: input, writer: output}, nil
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
