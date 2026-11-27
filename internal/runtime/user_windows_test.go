// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package runtime

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/registry"
)

var windowsUserChildMode = flag.String("runtime-user-child", "", "Windows user-launch test child mode")
var windowsUserSecret = flag.String("runtime-user-secret", "", "Protected fixture file for the Windows user-launch test")

type windowsUserReport struct {
	SID, Owner, Home, KnownHome, AppData, Temp, Cwd, Station, ParentSecret string
	SecretDenied                                                           bool
}

func TestWindowsUserLookup(t *testing.T) {
	for _, name := range []string{`NT AUTHORITY\SYSTEM`, "invalid\x00user"} {
		if _, err := LookupUser(name); err == nil {
			t.Fatalf("LookupUser(%q) succeeded", name)
		}
	}
	if env := (&User{}).Environ(); env != nil {
		t.Fatalf("profile environment must be built by Windows: %v", env)
	}
}

func TestWindowsUserUnsupportedOptions(t *testing.T) {
	for _, env := range [][]string{{}, {"HOME=service-home"}} {
		p := &Child{cmd: exec.Command("not-started.exe")}
		p.cmd.Env = env
		if err := p.configure(nil, &User{}); err == nil || !strings.Contains(err.Error(), "environment") {
			t.Fatalf("custom Env: %v", err)
		}
	}
	p := &Child{cmd: exec.Command("not-started.exe")}
	if err := p.configure([]*os.File{nil}, &User{}); err == nil || !strings.Contains(err.Error(), "inherit") {
		t.Fatalf("extra handles: %v", err)
	}
}

func TestWindowsUserStdioDrain(t *testing.T) {
	r, w, err := Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	defer r.Close()
	done := make(chan error, 1)
	go func() {
		_, err := io.Copy(io.Discard, r)
		r.Close()
		done <- err
	}()
	p := &Child{cmd: &exec.Cmd{WaitDelay: 20 * time.Millisecond}}
	p.platform.readers, p.platform.copied = []*os.File{r}, []<-chan error{done}
	if err := p.finishIO(); !errors.Is(err, exec.ErrWaitDelay) {
		t.Fatalf("drain: %v", err)
	}
}

// Privileged tests are opt-in. The caller must have SeTcbPrivilege and
// SeImpersonatePrivilege; HRC_TEST_WINDOWS_USER must name a regular test account
// with SeBatchLogonRight. Its profile and the test binary must be accessible.
func windowsTestUser(t *testing.T) *User {
	t.Helper()
	name := os.Getenv("HRC_TEST_WINDOWS_USER")
	if name == "" {
		t.Skip("requires HRC_TEST_WINDOWS_USER and privileged Windows launcher")
	}
	user, err := LookupUser(name)
	if err != nil {
		t.Fatal(err)
	}
	return user
}

func windowsUserCommand(t *testing.T, mode string) Command {
	t.Helper()
	user := windowsTestUser(t)
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return Command{Path: self, User: user, Dir: user.HomeDir, Transport: NamedPipeTransport,
		Args: []string{"-test.run=^TestWindowsUserChild$", "-runtime-user-child=" + mode,
			"-runtime-user-secret=" + os.Getenv("HRC_TEST_WINDOWS_SECRET")}, Input: true}
}

func launchWindowsUser(t *testing.T, command Command) *Child {
	t.Helper()
	pool := &Pool{}
	t.Cleanup(func() { pool.Close() })
	child, err := pool.Start(t.Context(), ChildSpec{Command: command, StartupTimeout: 15 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { child.Close(0) })
	child.Connection().SetDeadline(time.Now().Add(15 * time.Second))
	if _, err := child.Connection().Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	return child
}

func windowsStationName() (string, error) {
	h, _, err := desktopAPI.NewProc("GetProcessWindowStation").Call()
	if h == 0 {
		return "", err
	}
	var name [256]uint16
	var size uint32
	if err := windowsBool(desktopAPI.NewProc("GetUserObjectInformationW"), h, 2, uintptr(unsafe.Pointer(&name[0])), unsafe.Sizeof(name), uintptr(unsafe.Pointer(&size))); err != nil {
		return "", err
	}
	return windows.UTF16ToString(name[:]), nil
}

func TestWindowsUserLaunch(t *testing.T) {
	command := windowsUserCommand(t, "report")
	t.Setenv("HRC_RUNTIME_PARENT_SECRET", "must-not-inherit")
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	child := launchWindowsUser(t, command)
	if _, err := io.WriteString(child.Stdin(), "stdin"); err != nil {
		t.Fatal(err)
	}
	child.Stdin().Close()
	var report windowsUserReport
	decodeErr := json.NewDecoder(child.Connection()).Decode(&report)
	code, err := child.Wait()
	if decodeErr != nil || err != nil || code != 0 {
		t.Fatalf("decode=%v exit=%d wait=%v stdout=%q stderr=%q", decodeErr, code, err, stdout.String(), stderr.String())
	}
	if report.SID != command.User.sid || report.Owner != command.User.sid {
		t.Fatalf("wrong identity or file owner: %+v", report)
	}
	for _, home := range []string{report.Home, report.KnownHome, report.Cwd} {
		if !strings.EqualFold(filepath.Clean(home), filepath.Clean(command.User.HomeDir)) {
			t.Fatalf("wrong home/cwd %q, expected %q", home, command.User.HomeDir)
		}
	}
	for _, path := range []string{report.AppData, report.Temp} {
		rel, err := filepath.Rel(command.User.HomeDir, path)
		if err != nil || !filepath.IsAbs(path) || rel == ".." || strings.HasPrefix(rel, `..\`) {
			t.Fatalf("profile path %q is outside %q", path, command.User.HomeDir)
		}
	}
	if report.ParentSecret != "" {
		t.Fatal("daemon environment leaked into profile environment")
	}
	station, err := windowsStationName()
	if err != nil {
		t.Fatal(err)
	}
	if report.Station == "" || report.Station == station {
		t.Fatalf("child shares daemon window station %q", station)
	}
	if os.Getenv("HRC_TEST_WINDOWS_SECRET") != "" && !report.SecretDenied {
		t.Fatal("child read protected daemon file")
	}
	if !strings.Contains(stdout.String(), "user stdout:stdin") || !strings.Contains(stderr.String(), "user stderr") {
		t.Fatalf("stdio not independent: stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
	if exit, cleanup := child.Close(0); exit != nil || cleanup != nil {
		t.Fatalf("close: %v; %v", exit, cleanup)
	}
}

func TestWindowsUserExit(t *testing.T) {
	child := launchWindowsUser(t, windowsUserCommand(t, "exit"))
	data, err := io.ReadAll(child.Connection())
	if err != nil || string(data) != "last response" {
		t.Fatalf("buffered response=%q: %v", data, err)
	}
	code, err := child.Wait()
	if code != 17 || err != nil {
		t.Fatalf("exit=%d wait=%v", code, err)
	}
	var exit *ExitError
	if !errors.As(child.Err(), &exit) || exit.Code != 17 {
		t.Fatalf("exit error: %v", child.Err())
	}
}

func TestWindowsUserKill(t *testing.T) {
	child := launchWindowsUser(t, windowsUserCommand(t, "protected-hold"))
	var ready [1]byte
	if _, err := io.ReadFull(child.Connection(), ready[:]); err != nil {
		t.Fatal(err)
	}
	if err := child.Kill(); err != nil {
		t.Fatal(err)
	}
	code, err := child.Wait()
	if code != 1 || err != nil {
		t.Fatalf("exit=%d wait=%v", code, err)
	}
	if err := child.Kill(); err != nil {
		t.Fatalf("kill after reap: %v", err)
	}
}

var windowsUserWriteError = errors.New("user output writer failed")

type windowsUserErrorWriter struct{}

func (windowsUserErrorWriter) Write([]byte) (int, error) { return 0, windowsUserWriteError }

func TestWindowsUserOutputError(t *testing.T) {
	command := windowsUserCommand(t, "exit-output")
	command.Stdout = windowsUserErrorWriter{}
	child := launchWindowsUser(t, command)
	if data, err := io.ReadAll(child.Connection()); err != nil || string(data) != "last response" {
		t.Fatalf("buffered response=%q: %v", data, err)
	}
	code, err := child.Wait()
	if code != 17 || !errors.Is(err, windowsUserWriteError) {
		t.Fatalf("exit=%d wait=%v", code, err)
	}
}

func TestWindowsUserStartupRollback(t *testing.T) {
	command := windowsUserCommand(t, "report")
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	badDir := command
	badDir.Dir = filepath.Join(command.User.HomeDir, "hrc-nonexistent-dir", "missing")
	badArgs := command
	badArgs.Args = []string{"invalid\x00argument"}
	changedUser := *command.User
	changedUser.sid = "S-1-5-21-1-2-3-1001"
	badSID := command
	badSID.User = &changedUser
	pool := &Pool{}
	defer pool.Close()
	for _, config := range []Command{badDir, badArgs, badSID} {
		child, err := pool.Start(t.Context(), ChildSpec{Command: config})
		if err == nil {
			child.Close(0)
			t.Fatal("invalid launch succeeded")
		}
		if len(pool.children) != 0 {
			t.Fatal("failed launch remained in pool")
		}
		// Inspect owned resources, not the process-wide handle count: Go threads
		// and Windows RPC caches legitimately grow while these calls run.
		for range 20 {
			p := &Child{done: make(chan struct{})}
			err := p.startProcess(t.Context(), config)
			if p.input != nil {
				p.input.Close()
			}
			if err == nil || p.launched {
				p.kill(time.Now().Add(time.Second))
				if p.launched {
					<-p.done
				}
				t.Fatalf("invalid bare launch: %v", err)
			}
			stdin := p.cmd.Stdin.(*os.File)
			if stdin.Fd() != ^uintptr(0) {
				t.Fatal("child stdin endpoint was not closed")
			}
			if p.platform.process != 0 || p.platform.station != 0 || p.platform.desktop != 0 || len(p.platform.readers) != 0 || len(p.platform.copied) != 0 {
				t.Fatal("failed launch retained native resources")
			}
		}
	}
}

func TestWindowsUserChild(t *testing.T) {
	if *windowsUserChildMode == "" {
		t.Skip("subprocess helper")
	}
	conn, err := InternalConnection(t.Context(), flag.Args())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var hello [5]byte
	if _, err := io.ReadFull(conn, hello[:]); err != nil || string(hello[:]) != "hello" {
		t.Fatalf("control handshake: %q %v", hello, err)
	}
	switch *windowsUserChildMode {
	case "exit", "exit-output":
		if *windowsUserChildMode == "exit-output" {
			os.Stdout.Write(bytes.Repeat([]byte("x"), 2*1024*1024))
		}
		conn.Write([]byte("last response"))
		os.Exit(17)
	case "protected-hold":
		// The launcher must retain its creation handle, not reopen PROCESS_TERMINATE
		// by PID after the child has removed the launcher's process-DACL grant.
		sd, err := windows.SecurityDescriptorFromString("D:P(A;;GA;;;SY)")
		if err != nil {
			t.Fatal(err)
		}
		acl, _, err := sd.DACL()
		if err != nil {
			t.Fatal(err)
		}
		if err := windows.SetSecurityInfo(windows.CurrentProcess(), windows.SE_KERNEL_OBJECT, windows.DACL_SECURITY_INFORMATION, nil, nil, acl, nil); err != nil {
			t.Fatal(err)
		}
		conn.Write([]byte{1})
		io.Copy(io.Discard, conn)
		return
	case "report":
	default:
		t.Fatal("unknown child mode")
	}
	input, err := io.ReadAll(os.Stdin)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintf(os.Stdout, "user stdout:%s\n", input)
	fmt.Fprintln(os.Stderr, "user stderr")
	token, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		t.Fatal(err)
	}
	report := windowsUserReport{SID: token.User.Sid.String(), Home: os.Getenv("USERPROFILE"), AppData: os.Getenv("APPDATA"), Temp: os.Getenv("TEMP"), ParentSecret: os.Getenv("HRC_RUNTIME_PARENT_SECRET")}
	// Profile TEMP may use an 8.3 name. Resolve it under the child identity;
	// the service deliberately has no access to the user's profile directories.
	for _, path := range []*string{&report.Home, &report.AppData, &report.Temp} {
		ptr, err := windows.UTF16PtrFromString(*path)
		if err != nil {
			t.Fatal(err)
		}
		var long [32768]uint16
		n, err := windows.GetLongPathName(ptr, &long[0], uint32(len(long)))
		if err != nil || n == 0 || n >= uint32(len(long)) {
			t.Fatalf("resolve %q: %v", *path, err)
		}
		*path = windows.UTF16ToString(long[:n])
	}
	report.KnownHome, err = windows.KnownFolderPath(windows.FOLDERID_Profile, 0)
	if err != nil {
		t.Fatal(err)
	}
	report.Cwd, err = os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	report.Station, err = windowsStationName()
	if err != nil {
		t.Fatal(err)
	}
	if *windowsUserSecret != "" {
		_, err := os.ReadFile(*windowsUserSecret)
		report.SecretDenied = errors.Is(err, os.ErrPermission)
	}
	f, err := os.CreateTemp(report.Home, "hrc-runtime-user-test-*")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	defer os.Remove(f.Name())
	sd, err := windows.GetNamedSecurityInfo(f.Name(), windows.SE_FILE_OBJECT, windows.OWNER_SECURITY_INFORMATION)
	if err != nil {
		t.Fatal(err)
	}
	owner, _, err := sd.Owner()
	if err != nil {
		t.Fatal(err)
	}
	report.Owner = owner.String()
	// Verify that HKCU maps to this SID's real hive, not the service's hive.
	keyPath := fmt.Sprintf(`Software\hrc-runtime-test-%d`, os.Getpid())
	key, _, err := registry.CreateKey(registry.CURRENT_USER, keyPath, registry.ALL_ACCESS)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.DeleteKey(registry.CURRENT_USER, keyPath)
	defer key.Close()
	if err := key.SetStringValue("identity", report.SID); err != nil {
		t.Fatal(err)
	}
	hive, err := registry.OpenKey(registry.USERS, report.SID+`\`+keyPath, registry.READ)
	if err != nil {
		t.Fatal(err)
	}
	defer hive.Close()
	value, _, err := hive.GetStringValue("identity")
	if err != nil || value != report.SID {
		t.Fatalf("HKCU mismatch: %q %v", value, err)
	}
	if err := json.NewEncoder(conn).Encode(report); err != nil {
		t.Fatal(err)
	}
}
