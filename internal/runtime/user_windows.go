// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package runtime

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"reflect"
	goruntime "runtime"
	"strings"
	"sync"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

var tokenAPI = windows.NewLazySystemDLL("advapi32.dll")
var lsaAPI = windows.NewLazySystemDLL("secur32.dll")
var desktopAPI = windows.NewLazySystemDLL("user32.dll")
var stationMu sync.Mutex

// childPlatform owns resources created outside exec.Cmd.Start. Keep the
// creation handle for termination even if the child changes its process DACL.
type childPlatform struct {
	mu               sync.Mutex
	process          windows.Handle
	native           bool
	station, desktop windows.Handle
	readers          []*os.File
	copied           []<-chan error
}

func lookupUser(name string) (*User, error) {
	account, err := user.Lookup(name)
	if err != nil {
		return nil, fmt.Errorf("lookup user %q: %w", name, err)
	}
	// Service identities are not regular user accounts.
	if !strings.HasPrefix(account.Uid, "S-1-5-21-") {
		return nil, fmt.Errorf("user %q must be a regular user account", name)
	}
	if !filepath.IsAbs(account.HomeDir) {
		return nil, fmt.Errorf("user %q has no absolute profile directory", name)
	}
	return &User{Name: account.Username, HomeDir: account.HomeDir, sid: account.Uid}, nil
}

// Let seclogon construct the target's environment after loading its profile.
// In particular, never replace profile variables with the service's values.
func userEnvironment(*User) []string { return nil }

type lsaString struct {
	Length, MaximumLength uint16
	Buffer                unsafe.Pointer
}

func lsaANSI(s string) (lsaString, []byte) {
	b := append([]byte(s), 0)
	return lsaString{uint16(len(s)), uint16(len(b)), unsafe.Pointer(&b[0])}, b
}
func lsaError(status uintptr) error {
	if uint32(status) == 0 {
		return nil
	}
	return windows.NTStatus(status).Errno()
}
func windowsBool(proc *windows.LazyProc, args ...uintptr) error {
	ok, _, err := proc.Call(args...)
	if ok == 0 {
		return err
	}
	return nil
}
func enablePrivilege(name string) error {
	var tok windows.Token
	if err := windows.OpenProcessToken(windows.CurrentProcess(), windows.TOKEN_ADJUST_PRIVILEGES|windows.TOKEN_QUERY, &tok); err != nil {
		return err
	}
	defer tok.Close()
	value, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return err
	}
	var luid windows.LUID
	if err := windows.LookupPrivilegeValue(nil, value, &luid); err != nil {
		return err
	}
	p := windows.Tokenprivileges{PrivilegeCount: 1}
	p.Privileges[0] = windows.LUIDAndAttributes{Luid: luid, Attributes: windows.SE_PRIVILEGE_ENABLED}
	// AdjustTokenPrivileges can succeed while reporting ERROR_NOT_ALL_ASSIGNED.
	ok, _, last := tokenAPI.NewProc("AdjustTokenPrivileges").Call(uintptr(tok), 0, uintptr(unsafe.Pointer(&p)), 0, 0, 0)
	if ok == 0 || last == windows.ERROR_NOT_ALL_ASSIGNED {
		return fmt.Errorf("enable %s: %w", name, last)
	}
	return nil
}

func logonS4U(user string) (windows.Token, error) {
	var lsa uintptr
	status, _, _ := lsaAPI.NewProc("LsaConnectUntrusted").Call(uintptr(unsafe.Pointer(&lsa)))
	if err := lsaError(status); err != nil {
		return 0, err
	}
	defer lsaAPI.NewProc("LsaDeregisterLogonProcess").Call(lsa)
	pkg, pkgBytes := lsaANSI("MICROSOFT_AUTHENTICATION_PACKAGE_V1_0")
	var packageID uint32
	status, _, _ = lsaAPI.NewProc("LsaLookupAuthenticationPackage").Call(lsa, uintptr(unsafe.Pointer(&pkg)), uintptr(unsafe.Pointer(&packageID)))
	goruntime.KeepAlive(pkgBytes)
	if err := lsaError(status); err != nil {
		return 0, err
	}
	domainName, user, ok := strings.Cut(user, `\`)
	if !ok {
		return 0, fmt.Errorf("account must be qualified")
	}
	uname, err := windows.UTF16FromString(user)
	if err != nil {
		return 0, err
	}
	domain, err := windows.UTF16FromString(domainName)
	if err != nil {
		return 0, err
	}
	if len(uname) <= 1 || len(domain) <= 1 || len(uname) > 32767 || len(domain) > 32767 {
		return 0, errors.New("invalid S4U account name")
	}
	type auth struct {
		MessageType, Flags uint32
		User, Domain       lsaString
	}
	data := make([]byte, int(unsafe.Sizeof(auth{}))+(len(uname)+len(domain))*2)
	a := (*auth)(unsafe.Pointer(&data[0]))
	a.MessageType = 12
	offset := int(unsafe.Sizeof(auth{}))
	u := unsafe.Slice((*uint16)(unsafe.Pointer(&data[offset])), len(uname))
	copy(u, uname)
	a.User = lsaString{uint16((len(u) - 1) * 2), uint16(len(u) * 2), unsafe.Pointer(&u[0])}
	offset += len(uname) * 2
	d := unsafe.Slice((*uint16)(unsafe.Pointer(&data[offset])), len(domain))
	copy(d, domain)
	a.Domain = lsaString{uint16((len(d) - 1) * 2), uint16(len(d) * 2), unsafe.Pointer(&d[0])}
	origin, originBytes := lsaANSI("hrcd")
	source := struct {
		Name [8]byte
		ID   windows.LUID
	}{}
	copy(source.Name[:], "hrcd")
	if err := windowsBool(tokenAPI.NewProc("AllocateLocallyUniqueId"), uintptr(unsafe.Pointer(&source.ID))); err != nil {
		return 0, err
	}
	var profile uintptr
	var profileLen, substatus uint32
	var id windows.LUID
	var tok windows.Token
	quota := struct {
		Paged, NonPaged, MinWS, MaxWS, PageFile uintptr
		Time                                    int64
	}{}
	status, _, _ = lsaAPI.NewProc("LsaLogonUser").Call(lsa, uintptr(unsafe.Pointer(&origin)), 4, uintptr(packageID), uintptr(unsafe.Pointer(&data[0])), uintptr(len(data)), 0, uintptr(unsafe.Pointer(&source)), uintptr(unsafe.Pointer(&profile)), uintptr(unsafe.Pointer(&profileLen)), uintptr(unsafe.Pointer(&id)), uintptr(unsafe.Pointer(&tok)), uintptr(unsafe.Pointer(&quota)), uintptr(unsafe.Pointer(&substatus)))
	goruntime.KeepAlive(data)
	goruntime.KeepAlive(originBytes)
	if profile != 0 {
		lsaAPI.NewProc("LsaFreeReturnBuffer").Call(profile)
	}
	if err := lsaError(status); err != nil {
		return 0, fmt.Errorf("S4U %s: %w; substatus %v", user, err, lsaError(uintptr(substatus)))
	}
	var primary windows.Token
	err = windows.DuplicateTokenEx(tok, windows.TOKEN_ALL_ACCESS, nil, windows.SecurityImpersonation, windows.TokenPrimary, &primary)
	tok.Close()
	return primary, err
}

// privateDesktop gives this logon session its own noninteractive UI objects.
// It never grants space users access to the daemon's window station/desktop.
func privateDesktop(tok windows.Token) (station, desktop windows.Handle, name string, err error) {
	groups, err := tok.GetTokenGroups()
	if err != nil {
		return 0, 0, "", err
	}
	logon := ""
	for _, g := range groups.AllGroups() {
		if g.Attributes&windows.SE_GROUP_LOGON_ID == windows.SE_GROUP_LOGON_ID {
			logon = g.Sid.String()
			break
		}
	}
	if logon == "" {
		return 0, 0, "", errors.New("S4U token has no logon SID")
	}
	current, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return 0, 0, "", err
	}
	sd, err := windows.SecurityDescriptorFromString("D:P(A;;GA;;;SY)(A;;GA;;;" + current.User.Sid.String() + ")(A;;GA;;;" + logon + ")")
	if err != nil {
		return 0, 0, "", err
	}
	sa := windows.SecurityAttributes{Length: uint32(unsafe.Sizeof(windows.SecurityAttributes{})), SecurityDescriptor: sd}
	desktopUTF, _ := windows.UTF16PtrFromString("default")
	// SetProcessWindowStation is process-wide; serialize the brief create/restore
	// sequence. Keep this OS thread on its original desktop throughout.
	stationMu.Lock()
	defer stationMu.Unlock()
	goruntime.LockOSThread()
	defer goruntime.UnlockOSThread()
	old, _, e := desktopAPI.NewProc("GetProcessWindowStation").Call()
	if old == 0 {
		return 0, 0, "", fmt.Errorf("GetProcessWindowStation: %w", e)
	}
	if err := windowsBool(tokenAPI.NewProc("ImpersonateLoggedOnUser"), uintptr(tok)); err != nil {
		return 0, 0, "", err
	}
	h, _, e := desktopAPI.NewProc("CreateWindowStationW").Call(0, 1 /* CWF_CREATE_ONLY */, windows.GENERIC_ALL, uintptr(unsafe.Pointer(&sa)))
	if revert := windowsBool(tokenAPI.NewProc("RevertToSelf")); revert != nil {
		// Never return an impersonated OS thread to the Go scheduler.
		fmt.Fprintln(os.Stderr, "cannot revert user impersonation:", revert)
		os.Exit(1)
	}
	if h == 0 {
		return 0, 0, "", fmt.Errorf("CreateWindowStation: %w", e)
	}
	station = windows.Handle(h)
	defer func() {
		if err != nil {
			err = errors.Join(err, closeDesktop(station, desktop))
			station = 0
			desktop = 0
		}
	}()
	var stationBuffer [256]uint16
	var nameBytes uint32
	if err = windowsBool(desktopAPI.NewProc("GetUserObjectInformationW"), h, 2 /* UOI_NAME */, uintptr(unsafe.Pointer(&stationBuffer[0])), unsafe.Sizeof(stationBuffer), uintptr(unsafe.Pointer(&nameBytes))); err != nil {
		return station, 0, "", err
	}
	stationName := windows.UTF16ToString(stationBuffer[:])
	if err = windowsBool(desktopAPI.NewProc("SetProcessWindowStation"), h); err != nil {
		return station, 0, "", fmt.Errorf("SetProcessWindowStation: %w", err)
	}
	d, _, createErr := desktopAPI.NewProc("CreateDesktopW").Call(uintptr(unsafe.Pointer(desktopUTF)), 0, 0, 0, 0xf01ff, uintptr(unsafe.Pointer(&sa)))
	desktop = windows.Handle(d)
	if restoreErr := windowsBool(desktopAPI.NewProc("SetProcessWindowStation"), old); restoreErr != nil {
		// Continuing would put later daemon children on a user's window station.
		fmt.Fprintln(os.Stderr, "cannot restore daemon window station:", restoreErr)
		os.Exit(1)
	}
	goruntime.KeepAlive(sd)
	if d == 0 {
		err = fmt.Errorf("CreateDesktop: %w", createErr)
	}
	return station, desktop, stationName + "\\default", err
}
func closeDesktop(station, desktop windows.Handle) error {
	var err error
	if desktop != 0 {
		err = windowsBool(desktopAPI.NewProc("CloseDesktop"), uintptr(desktop))
	}
	if station != 0 {
		err = errors.Join(err, windowsBool(desktopAPI.NewProc("CloseWindowStation"), uintptr(station)))
	}
	return err
}

func (p *Child) start(config Command) (err error) {
	if config.User == nil {
		// Do not inherit a temporary window station from privateDesktop.
		stationMu.Lock()
		defer stationMu.Unlock()
		return p.cmd.Start()
	}
	// StdinPipe was prepared before entering this platform launcher. Cmd.Start
	// is not called here, so close its child endpoint on every startup path.
	if stdin, ok := p.cmd.Stdin.(*os.File); ok {
		defer stdin.Close()
	}
	for _, name := range []string{"SeTcbPrivilege", "SeImpersonatePrivilege"} {
		if err := enablePrivilege(name); err != nil {
			return err
		}
	}
	tok, err := logonS4U(config.User.Name)
	if err != nil {
		return err
	}
	defer tok.Close()
	target, err := tok.GetTokenUser()
	if err != nil {
		return err
	}
	if target.User.Sid.String() != config.User.sid {
		return errors.New("account SID changed since lookup")
	}
	station, desktop, desktopName, err := privateDesktop(tok)
	if err != nil {
		return fmt.Errorf("create private desktop: %w", err)
	}
	p.platform.station, p.platform.desktop = station, desktop
	var handles []windows.Handle
	var files []*os.File
	defer func() {
		for _, h := range handles {
			windows.CloseHandle(h)
		}
		for _, f := range files {
			f.Close()
		}
		if err != nil {
			err = errors.Join(err, p.finishIO())
		}
	}()
	null, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	files = append(files, null)
	stdin := null
	if p.cmd.Stdin != nil {
		stdin = p.cmd.Stdin.(*os.File)
	}
	output := func(w io.Writer) (*os.File, error) {
		if w == nil {
			return null, nil
		}
		if f, ok := w.(*os.File); ok {
			return f, nil
		}
		r, wf, e := Pipe()
		if e != nil {
			return nil, e
		}
		files = append(files, wf)
		p.platform.readers = append(p.platform.readers, r)
		done := make(chan error, 1)
		p.platform.copied = append(p.platform.copied, done)
		go func() {
			_, e := io.Copy(w, r)
			r.Close()
			done <- e
		}()
		return wf, nil
	}
	stdout, err := output(config.Stdout)
	if err != nil {
		return err
	}
	stderr := stdout
	if config.Stdout == nil || !reflect.TypeOf(config.Stdout).Comparable() || config.Stdout != config.Stderr {
		stderr, err = output(config.Stderr)
		if err != nil {
			return err
		}
	}
	duplicate := func(f *os.File) (windows.Handle, error) {
		var h windows.Handle
		e := windows.DuplicateHandle(windows.CurrentProcess(), windows.Handle(f.Fd()), windows.CurrentProcess(), &h, 0, true, windows.DUPLICATE_SAME_ACCESS)
		if e == nil {
			handles = append(handles, h)
		}
		return h, e
	}
	si := windows.StartupInfo{Flags: windows.STARTF_USESTDHANDLES}
	si.Cb = uint32(unsafe.Sizeof(si))
	si.Desktop, _ = windows.UTF16PtrFromString(desktopName)
	si.StdInput, err = duplicate(stdin)
	if err != nil {
		return err
	}
	si.StdOutput, err = duplicate(stdout)
	if err != nil {
		return err
	}
	si.StdErr, err = duplicate(stderr)
	if err != nil {
		return err
	}
	path, err := windows.UTF16PtrFromString(p.cmd.Path)
	if err != nil {
		return err
	}
	args := make([]string, len(p.cmd.Args))
	for i, a := range p.cmd.Args {
		args[i] = windows.EscapeArg(a)
	}
	command, err := windows.UTF16PtrFromString(strings.Join(args, " "))
	if err != nil {
		return err
	}
	var dir *uint16
	if p.cmd.Dir != "" {
		dir, err = windows.UTF16PtrFromString(p.cmd.Dir)
		if err != nil {
			return err
		}
	}
	var pi windows.ProcessInformation
	err = windowsBool(tokenAPI.NewProc("CreateProcessWithTokenW"), uintptr(tok), 1 /* LOGON_WITH_PROFILE */, uintptr(unsafe.Pointer(path)), uintptr(unsafe.Pointer(command)), windows.CREATE_SUSPENDED|windows.CREATE_UNICODE_ENVIRONMENT|windows.CREATE_NO_WINDOW, 0, uintptr(unsafe.Pointer(dir)), uintptr(unsafe.Pointer(&si)), uintptr(unsafe.Pointer(&pi)))
	if err != nil {
		return fmt.Errorf("create process with profile: %w", err)
	}
	p.platform.process, p.platform.native = pi.Process, true
	defer windows.CloseHandle(pi.Thread)
	defer func() {
		if err != nil {
			windows.TerminateProcess(pi.Process, 1)
			windows.WaitForSingleObject(pi.Process, windows.INFINITE)
			if p.cmd.Process != nil {
				p.cmd.Process.Release()
				p.cmd.Process = nil
			}
		}
	}()
	// os.FindProcess needs a query/synchronize handle. Grant the daemon access
	// before resuming the child; wait on os.Process directly, not on a Cmd
	// which was never started. Do not depend on Administrators or SeDebugPrivilege.
	current, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return err
	}
	sd, err := windows.GetSecurityInfo(pi.Process, windows.SE_KERNEL_OBJECT, windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		return err
	}
	old, _, err := sd.DACL()
	if err != nil {
		return err
	}
	acl, err := windows.ACLFromEntries([]windows.EXPLICIT_ACCESS{{
		AccessPermissions: windows.READ_CONTROL | windows.PROCESS_QUERY_INFORMATION | windows.SYNCHRONIZE,
		AccessMode:        windows.GRANT_ACCESS,
		Trustee:           windows.TRUSTEE{TrusteeForm: windows.TRUSTEE_IS_SID, TrusteeType: windows.TRUSTEE_IS_USER, TrusteeValue: windows.TrusteeValueFromSID(current.User.Sid)},
	}}, old)
	if err != nil {
		return err
	}
	if err = windows.SetSecurityInfo(pi.Process, windows.SE_KERNEL_OBJECT, windows.DACL_SECURITY_INFORMATION, nil, nil, acl, nil); err != nil {
		return err
	}
	goruntime.KeepAlive(sd)
	p.cmd.Process, err = os.FindProcess(int(pi.ProcessId))
	if err != nil {
		return err
	}
	_, err = windows.ResumeThread(pi.Thread)
	return err
}

func (p *Child) finishIO() error {
	// Bound stdout/stderr draining when a descendant has kept a handle open.
	readers := p.platform.readers
	copied := p.platform.copied
	p.platform.readers, p.platform.copied = nil, nil
	timer := time.AfterFunc(p.cmd.WaitDelay, func() {
		for _, f := range readers {
			f.Close()
		}
	})
	var err error
	for _, done := range copied {
		err = errors.Join(err, <-done)
	}
	if !timer.Stop() && len(copied) != 0 {
		err = errors.Join(err, exec.ErrWaitDelay)
	}
	for _, f := range readers {
		f.Close()
	}
	err = errors.Join(err, closeDesktop(p.platform.station, p.platform.desktop))
	p.platform.station, p.platform.desktop = 0, 0
	p.platform.mu.Lock()
	if p.platform.process != 0 {
		err = errors.Join(err, windows.CloseHandle(p.platform.process))
		p.platform.process = 0
	}
	p.platform.mu.Unlock()
	return err
}

func (p *Child) waitProcess() error {
	if !p.platform.native {
		return p.cmd.Wait()
	}
	state, err := p.cmd.Process.Wait()
	p.cmd.ProcessState = state
	if err == nil && !state.Success() {
		return &exec.ExitError{ProcessState: state}
	}
	return err
}

func (p *Child) kill(deadline time.Time) error {
	if !p.platform.native {
		if p.cmd.Process == nil {
			return nil
		}
		return killTrackedProcess(p.cmd.Process, deadline)
	}
	p.platform.mu.Lock()
	defer p.platform.mu.Unlock()
	handle := p.platform.process
	if handle == 0 {
		return nil
	}
	err := windows.TerminateProcess(handle, 1)
	if err != nil && !errors.Is(err, windows.ERROR_ACCESS_DENIED) {
		return err
	}
	remaining := max(time.Until(deadline), 0)
	timeout := uint32((remaining + time.Millisecond - 1) / time.Millisecond)
	state, waitErr := windows.WaitForSingleObject(handle, timeout)
	if waitErr != nil {
		return errors.Join(err, waitErr)
	}
	if state != windows.WAIT_OBJECT_0 {
		return errors.Join(err, errors.New("process termination timed out"))
	}
	return nil
}
