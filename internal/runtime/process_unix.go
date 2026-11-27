// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

//go:build !windows

package runtime

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
)

func lookupUser(name string) (*User, error) {
	account, err := user.Lookup(name)
	if err != nil {
		return nil, fmt.Errorf("user %q: %w", name, err)
	}
	parse := func(value string) (uint32, error) {
		id, err := strconv.ParseUint(value, 10, 32)
		return uint32(id), err
	}
	uid, err := parse(account.Uid)
	if err != nil {
		return nil, fmt.Errorf("user %q: Unix identity is not supported: %w", name, err)
	}
	if uid == 0 {
		return nil, fmt.Errorf("user %q must not be root", name)
	}
	gid, err := parse(account.Gid)
	if err != nil {
		return nil, err
	}
	ids, err := account.GroupIds()
	if err != nil {
		return nil, fmt.Errorf("groups for %q: %w", name, err)
	}
	result := &User{Name: account.Username, HomeDir: account.HomeDir, uid: uid, gid: gid}
	if !filepath.IsAbs(result.HomeDir) {
		return nil, fmt.Errorf("user %q has no absolute home directory", name)
	}
	for _, id := range ids {
		group, err := parse(id)
		if err != nil {
			return nil, err
		}
		result.groups = append(result.groups, group)
	}
	return result, nil
}

func prepareControl(command Command) (*inheritedControl, error) {
	if command.Transport != InheritedTransport {
		return nil, fmt.Errorf("%w: unsupported control transport %d", os.ErrInvalid, command.Transport)
	}
	return newInheritedControl()
}

func userEnvironment(u *User) []string {
	return append(os.Environ(), "HOME="+u.HomeDir, "USER="+u.Name, "LOGNAME="+u.Name)
}

// InternalConnection opens the daemon's control channel.
func InternalConnection(_ context.Context, args []string) (Connection, error) {
	return InheritedConnection(args)
}

// configure starts the child in a process group of its own.
func (p *Child) configure(files []*os.File, user *User) error {
	p.cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if user != nil {
		if user.uid == 0 {
			return errors.New("child user must not be root")
		}
		p.cmd.SysProcAttr.Credential = &syscall.Credential{Uid: user.uid, Gid: user.gid, Groups: user.groups}
	}
	p.cmd.ExtraFiles = files
	for i := range files {
		p.cmd.Args = append(p.cmd.Args, strconv.Itoa(3+i))
	}
	return nil
}

func (p *Child) attach() error { return nil }

func findTrackedProcess(pid int) (*os.Process, error) {
	return os.FindProcess(pid)
}

func killTrackedProcess(p *os.Process, _ time.Time) error {
	return p.Kill()
}

func testDetachDescendant(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
}

// Unix has no console to allocate for the test's children.
func testShareConsole() {}

// testSendInterrupt interrupts the caller's process group, as the runner does
// to cancel a step.
func testSendInterrupt() error {
	group, err := os.FindProcess(-os.Getpid())
	if err != nil {
		return err
	}
	defer group.Release()
	return group.Signal(os.Interrupt)
}

func (p *Child) closeInherited() {}

func inheritedSocket(input *os.File) (net.Conn, error) {
	conn, err := net.FileConn(input)
	if errors.Is(err, syscall.ENOTSOCK) {
		return nil, nil
	}
	return conn, err
}

func protectControlFiles(input, output *os.File) error {
	syscall.CloseOnExec(int(input.Fd()))
	syscall.CloseOnExec(int(output.Fd()))
	return nil
}

func socketPair() (net.Conn, *os.File, error) {
	syscall.ForkLock.RLock()
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err == nil {
		syscall.CloseOnExec(fds[0])
		syscall.CloseOnExec(fds[1])
	}
	syscall.ForkLock.RUnlock()
	if err != nil {
		if errors.Is(err, syscall.ENOSYS) || errors.Is(err, syscall.EAFNOSUPPORT) || errors.Is(err, syscall.EPROTONOSUPPORT) || errors.Is(err, syscall.EOPNOTSUPP) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	parent := os.NewFile(uintptr(fds[0]), "control-parent")
	defer parent.Close()
	child := os.NewFile(uintptr(fds[1]), "control-child")
	conn, err := net.FileConn(parent)
	if err != nil {
		child.Close()
		return nil, nil, err
	}
	return conn, child, nil
}
