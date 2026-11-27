// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package runtime

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
	"unsafe"

	hrcconfig "dimidiumlabs.io/hrc/internal/config"
	"golang.org/x/sys/windows"
)

// Host monitors select a named pipe explicitly. External providers and workers
// keep their inherited control handles, independent of stdio.
type childControl struct {
	inherited *inheritedControl
	handle    windows.Handle
	name      string
}

func prepareControl(command Command) (*childControl, error) {
	switch command.Transport {
	case NamedPipeTransport:
	case InheritedTransport:
		inherited, err := newInheritedControl()
		return &childControl{inherited: inherited}, err
	default:
		return nil, fmt.Errorf("%w: unsupported control transport %d", os.ErrInvalid, command.Transport)
	}
	current, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return nil, err
	}
	sid := current.User.Sid.String()
	// Exclude FILE_CREATE_PIPE_INSTANCE from the client's read/write rights.
	sd, err := windows.SecurityDescriptorFromString("D:P(A;;GA;;;SY)(A;;GA;;;" + current.User.Sid.String() + ")(A;;0x12019b;;;" + sid + ")")
	if err != nil {
		return nil, err
	}
	sa := windows.SecurityAttributes{Length: uint32(unsafe.Sizeof(windows.SecurityAttributes{})), SecurityDescriptor: sd}
	name := "\\\\.\\pipe\\hrc-" + rand.Text()
	path, _ := windows.UTF16PtrFromString(name)
	handle, err := windows.CreateNamedPipe(path, windows.PIPE_ACCESS_DUPLEX|windows.FILE_FLAG_FIRST_PIPE_INSTANCE|windows.FILE_FLAG_OVERLAPPED, windows.PIPE_TYPE_BYTE|windows.PIPE_READMODE_BYTE|windows.PIPE_REJECT_REMOTE_CLIENTS, 1, 65536, 65536, 0, &sa)
	if err != nil {
		return nil, err
	}
	return &childControl{handle: handle, name: name}, nil
}
func (c *childControl) files() []*os.File {
	if c.inherited != nil {
		return c.inherited.files()
	}
	return nil
}
func (c *childControl) args() []string {
	if c.inherited != nil {
		return nil
	}
	return []string{c.name, strconv.Itoa(os.Getpid())}
}
func (c *childControl) close() {
	if c.inherited != nil {
		c.inherited.close()
	}
	if c.handle != 0 {
		windows.CloseHandle(c.handle)
		c.handle = 0
	}
}
func connectPipe(ctx context.Context, handle windows.Handle) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	event, err := windows.CreateEvent(nil, 1, 0, nil)
	if err != nil {
		return err
	}
	defer windows.CloseHandle(event)
	op := windows.Overlapped{HEvent: event}
	err = windows.ConnectNamedPipe(handle, &op)
	if err == windows.ERROR_PIPE_CONNECTED || err == nil {
		return nil
	}
	if err != windows.ERROR_IO_PENDING {
		return err
	}
	canceled := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { windows.CancelIoEx(handle, &op); close(canceled) })
	defer func() {
		if !stop() {
			<-canceled
		}
	}()
	var n uint32
	err = windows.GetOverlappedResult(handle, &op, &n, true)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}
func (c *childControl) take(ctx context.Context, child *Child) (Connection, error) {
	if c.inherited != nil {
		return c.inherited.take(ctx, child)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-child.Done():
			cancel()
		case <-ctx.Done():
		}
	}()
	for {
		if err := connectPipe(ctx, c.handle); err != nil {
			return nil, err
		}
		var pid uint32
		if err := windows.GetNamedPipeClientProcessId(c.handle, &pid); err != nil {
			return nil, err
		}
		if pid == uint32(child.cmd.Process.Pid) {
			conn := namedPipeConnection(c.handle, c.name)
			c.handle = 0
			return conn, nil
		}
		if err := windows.DisconnectNamedPipe(c.handle); err != nil {
			return nil, err
		}
	}
}

// InternalConnection opens hrcm's control channel and verifies the daemon's PID.
// InheritedConnection's external provider ABI is unchanged.
func InternalConnection(ctx context.Context, args []string) (Connection, error) {
	if len(args) != 2 || !strings.HasPrefix(args[0], "\\\\.\\pipe\\") {
		return InheritedConnection(args)
	}
	pid, err := strconv.ParseUint(args[1], 10, 32)
	if err != nil || pid == 0 {
		return nil, fmt.Errorf("%w: invalid daemon PID", os.ErrInvalid)
	}
	ctx, cancel := context.WithTimeout(ctx, hrcconfig.ProcessStartTimeout)
	defer cancel()
	path, err := windows.UTF16PtrFromString(args[0])
	if err != nil {
		return nil, err
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		handle, err := windows.CreateFile(path, (windows.FILE_GENERIC_READ|windows.FILE_GENERIC_WRITE)&^0x0004 /* FILE_CREATE_PIPE_INSTANCE */, 0, nil, windows.OPEN_EXISTING, windows.FILE_FLAG_OVERLAPPED|windows.SECURITY_SQOS_PRESENT|windows.SECURITY_IDENTIFICATION, 0)
		if err == windows.ERROR_PIPE_BUSY {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(10 * time.Millisecond):
				continue
			}
		}
		if err != nil {
			return nil, err
		}
		var actual uint32
		err = windows.GetNamedPipeServerProcessId(handle, &actual)
		if err != nil || actual != uint32(pid) {
			windows.CloseHandle(handle)
			return nil, errors.Join(err, errors.New("control pipe daemon PID mismatch"))
		}
		return namedPipeConnection(handle, args[0]), nil
	}
}

type namedConnection struct {
	fileConnection
	name string
}

func namedPipeConnection(handle windows.Handle, name string) Connection {
	file := os.NewFile(uintptr(handle), name)
	return WatchConnection(&namedConnection{fileConnection: fileConnection{reader: file, writer: file}, name: name})
}
func (c *namedConnection) LocalAddr() net.Addr  { return pipeAddress(c.name) }
func (c *namedConnection) RemoteAddr() net.Addr { return pipeAddress(c.name) }

type pipeAddress string

func (pipeAddress) Network() string  { return "pipe" }
func (p pipeAddress) String() string { return string(p) }
