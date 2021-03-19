package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

type operKind int

const (
	operCreate operKind = iota
	operOpen
	operSeek
	operRead
	operWrite
	operClose
	operUnlink1
	operUnlink2

	operTruncate
	operFtruncate

	operMkdir
	operRmdir

	operRename1
	operRename2

	operChdir

	operMuscleFlush
	operMusclePush
	operMuscleRemount
	operMusclePruneCache
	operMuscleTrim

	operSwapClients

	operKindCount
)

func fromString(s string) operKind {
	switch s {
	case "create":
		return operCreate
	case "open":
		return operOpen
	case "seek":
		return operSeek
	case "read":
		return operRead
	case "write":
		return operWrite
	case "close":
		return operClose
	case "unlink1":
		return operUnlink1
	case "unlink2":
		return operUnlink2
	case "truncate":
		return operTruncate
	case "ftruncate":
		return operFtruncate
	case "mkdir":
		return operMkdir
	case "rmdir":
		return operRmdir
	case "rename1":
		return operRename1
	case "rename2":
		return operRename2
	case "chdir":
		return operChdir
	case "musclefsflush":
		return operMuscleFlush
	case "musclefspush":
		return operMusclePush
	case "musclefsremount":
		return operMuscleRemount
	case "musclefsprunecache":
		return operMusclePruneCache
	case "musclefstrim":
		return operMuscleTrim
	case "swapclients":
		return operSwapClients
	default:
		panic(s)
	}
}

func (code operKind) String() string {
	switch code {
	case operCreate:
		return "create"
	case operOpen:
		return "open"
	case operSeek:
		return "seek"
	case operRead:
		return "read"
	case operWrite:
		return "write"
	case operClose:
		return "close"
	case operUnlink1:
		return "unlink1"
	case operUnlink2:
		return "unlink2"
	case operTruncate:
		return "truncate"
	case operFtruncate:
		return "ftruncate"
	case operMkdir:
		return "mkdir"
	case operRmdir:
		return "rmdir"
	case operRename1:
		return "rename1"
	case operRename2:
		return "rename2"
	case operChdir:
		return "chdir"
	case operMuscleFlush:
		return "musclefsflush"
	case operMusclePush:
		return "musclefspush"
	case operMuscleRemount:
		return "musclefsremount"
	case operMusclePruneCache:
		return "musclefsprunecache"
	case operMuscleTrim:
		return "musclefstrim"
	case operSwapClients:
		return "swapclients"
	default:
		return fmt.Sprintf("unknown=%d", int(code))
	}
}

type openFlags int

var (
	linuxAccessModes   = openFlags(syscall.O_RDONLY | syscall.O_RDWR | syscall.O_WRONLY)
	linuxCreationFlags = openFlags(syscall.O_CLOEXEC | syscall.O_CREAT | syscall.O_DIRECTORY | syscall.O_EXCL | syscall.O_NOCTTY | syscall.O_NOFOLLOW | syscall.O_TRUNC | unix.O_TMPFILE)
	linuxStatusFlags   = openFlags(syscall.O_APPEND | syscall.O_ASYNC | syscall.O_DIRECT | syscall.O_DSYNC | syscall.O_LARGEFILE | syscall.O_NOATIME | syscall.O_NONBLOCK | syscall.O_SYNC | unix.O_PATH)
	// See open(2).
	linuxOpenFlags = linuxAccessModes | linuxCreationFlags | linuxStatusFlags

	// Initially I won't try to have musclefs match ext4 or tmpfs for all
	// open flags. Doing so would be very expensive in terms of study and
	// development, and would provide little benefit, because flags I don't
	// know well are unlikely to be used, at least by me. And I'm the only
	// user of musclefs.
	supportedOpenFlags = openFlags(syscall.O_APPEND | syscall.O_ASYNC | syscall.O_CLOEXEC | syscall.O_CREAT | syscall.O_DIRECTORY | syscall.O_EXCL | syscall.O_LARGEFILE | syscall.O_NOATIME | syscall.O_NOCTTY | syscall.O_NOFOLLOW | syscall.O_NONBLOCK | syscall.O_RDONLY | syscall.O_RDWR | syscall.O_TRUNC | syscall.O_WRONLY)

	// A call to creat() is equivalent to calling open() with flags equal to O_CREAT|O_WRONLY|O_TRUNC.
	createFlags = syscall.O_CREAT | syscall.O_WRONLY | syscall.O_TRUNC
)

func randomOpenFlags() openFlags {
	// Open in read-write mode 90% of the times, to properly exercise read and write.
	// Else, most reads/writes will give EBADF.
	// Leaving this probability inconfigurable.
	if rand.Intn(10) < 9 {
		return syscall.O_RDWR
	}
	return openFlags(rand.Int()) & supportedOpenFlags
}

func (flags openFlags) String() string {
	var b bytes.Buffer
	if flags&syscall.O_RDONLY != 0 {
		b.WriteString("|O_RDONLY")
	}
	if flags&syscall.O_WRONLY != 0 {
		b.WriteString("|O_WRONLY")
	}
	if flags&syscall.O_RDWR != 0 {
		b.WriteString("|O_RDWR")
	}
	if flags&syscall.O_CLOEXEC != 0 {
		b.WriteString("|O_CLOEXEC")
	}
	if flags&syscall.O_CREAT != 0 {
		b.WriteString("|O_CREAT")
	}
	if flags&syscall.O_DIRECTORY != 0 {
		b.WriteString("|O_DIRECTORY")
	}
	if flags&syscall.O_EXCL != 0 {
		b.WriteString("|O_EXCL")
	}
	if flags&syscall.O_NOCTTY != 0 {
		b.WriteString("|O_NOCTTY")
	}
	if flags&syscall.O_NOFOLLOW != 0 {
		b.WriteString("|O_NOFOLLOW")
	}
	if flags&syscall.O_TRUNC != 0 {
		b.WriteString("|O_TRUNC")
	}
	if flags&syscall.O_APPEND != 0 {
		b.WriteString("|O_APPEND")
	}
	if flags&syscall.O_ASYNC != 0 {
		b.WriteString("|O_ASYNC")
	}
	if flags&syscall.O_DIRECT != 0 {
		b.WriteString("|O_DIRECT")
	}
	if flags&syscall.O_DSYNC != 0 {
		b.WriteString("|O_DSYNC")
	}
	if flags&syscall.O_LARGEFILE != 0 {
		b.WriteString("|O_LARGEFILE")
	}
	if flags&syscall.O_NOATIME != 0 {
		b.WriteString("|O_NOATIME")
	}
	if flags&syscall.O_NONBLOCK != 0 {
		b.WriteString("|O_NONBLOCK")
	}
	if flags&syscall.O_SYNC != 0 {
		b.WriteString("|O_SYNC")
	}
	if g := flags &^ openFlags(linuxOpenFlags); g != 0 {
		_, _ = fmt.Fprintf(&b, "|%d", g)
	}
	if b.Len() > 0 {
		return b.String()[1:]
	}
	return "0"
}

type oper struct {
	id int

	code operKind

	// For operations that require file descriptors (seek, read, write,
	// close). It's the operation (create or open) that created the file
	// descriptors to be used. We don't just store the fds because the
	// parent operation may need to be replayed in musclefs after an
	// induced crash.
	parent *oper // seek, read, write, close, ftruncate.

	pathname    string    // creat, open, mkdir, rmdir, chdir, truncate, rename1, rename2, unlink1, unlink2.
	newpathname string    // rename1, rename2.
	flags       openFlags // open.
	mode        uint32    // creat, open, mkdir.

	rbuf int    // read, truncate, ftruncate.
	wbuf []byte // write.

	offset int64 // seek.
	whence int   // seek.

	// Output fields.

	sutn, refn     int    // read, write.
	sutbuf, refbuf []byte // read.
	sutfd, reffd   int    // create, open, chdir.
	sutoff, refoff int64  // seek.
	suterr, referr error  // create, open, seek, read, write, close, mkdir, rmdir.
}

// String implements fmt.Stringer.
func (oper *oper) String() string {
	b := bytes.NewBuffer(nil)
	_, _ = fmt.Fprintf(b,
		"[oper id=%d code=%v parent=%v pathname=%q newpathname=%q flags=%v mode=0%o len(wbuf)=%d rbuf=%d offset=%d whence=%d sutn=%d refn=%d len(sutbuf)=%d len(refbuf)=%d sutfd=%d reffd=%d sutoff=%d refoff=%d suterr=%v referr=%v]",
		oper.id, oper.code, oper.parent, oper.pathname, oper.newpathname, oper.flags, oper.mode, len(oper.wbuf), oper.rbuf, oper.offset, oper.whence, oper.sutn, oper.refn, len(oper.sutbuf), len(oper.refbuf), oper.sutfd, oper.reffd, oper.sutoff, oper.refoff, oper.suterr, oper.referr)
	return b.String()
}

func (oper *oper) run(s *operSeq) {
	sut := filesystems[suti]
	switch oper.code {
	case operCreate:
		p := s.relativize(oper.pathname)
		oper.sutfd, oper.suterr = syscall.Openat(s.sutcwd, p, createFlags, oper.mode)
		oper.reffd, oper.referr = syscall.Openat(s.refcwd, p, createFlags, oper.mode)
	case operOpen:
		p := s.relativize(oper.pathname)
		oper.sutfd, oper.suterr = syscall.Openat(s.sutcwd, p, int(oper.flags), oper.mode)
		oper.reffd, oper.referr = syscall.Openat(s.refcwd, p, int(oper.flags), oper.mode)
	case operSeek:
		oper.sutoff, oper.suterr = syscall.Seek(oper.parent.sutfd, oper.offset, oper.whence)
		oper.refoff, oper.referr = syscall.Seek(oper.parent.reffd, oper.offset, oper.whence)
	case operRead:
		oper.sutbuf = make([]byte, oper.rbuf)
		oper.refbuf = make([]byte, oper.rbuf)
		oper.sutn, oper.suterr = syscall.Read(oper.parent.sutfd, oper.sutbuf)
		oper.refn, oper.referr = syscall.Read(oper.parent.reffd, oper.refbuf)
	case operWrite:
		oper.sutn, oper.suterr = syscall.Write(oper.parent.sutfd, oper.wbuf)
		oper.refn, oper.referr = syscall.Write(oper.parent.reffd, oper.wbuf)
	case operClose:
		oper.suterr = syscall.Close(oper.parent.sutfd)
		oper.referr = syscall.Close(oper.parent.reffd)
	case operUnlink1:
		p := s.relativize(oper.pathname)
		oper.suterr = syscall.Unlinkat(s.sutcwd, p)
		oper.referr = syscall.Unlinkat(s.refcwd, p)
	case operUnlink2:
		_, oper.suterr = sut.runCommand("unlink " + oper.pathname)
		if oper.suterr != nil && oper.suterr.Error() == "device or resource busy" {
			// Musclefs can't unlink file trees if they have any fids pointing to them.
			// In that case, pretend the reference file system will also deny the operation.
			// Another approach would be to only generate “safe” unlink2 operations but that seems more work.
			oper.referr = oper.suterr
		} else {
			oper.referr = func() error {
				var pp []string
				collect := func(path string, info os.FileInfo, err error) error {
					if err != nil {
						return err
					}
					pp = append(pp, path)
					return nil
				}
				if err := filepath.Walk(filepath.Join(refDir, oper.pathname), collect); err != nil {
					return err
				}
				for i := len(pp) - 1; i >= 0; i-- {
					if err := os.Remove(pp[i]); err != nil {
						return err
					}
				}
				return nil
			}()
		}
		// Report only the basic error, for easier classification.
		e := errors.Unwrap(oper.referr)
		for e != nil {
			oper.referr = e
			e = errors.Unwrap(oper.referr)
		}
	case operTruncate:
		oper.suterr = syscall.Truncate(filepath.Join(sut.mnt, oper.pathname), int64(oper.rbuf))
		oper.referr = syscall.Truncate(filepath.Join(refDir, oper.pathname), int64(oper.rbuf))
	case operFtruncate:
		oper.suterr = syscall.Ftruncate(oper.parent.sutfd, int64(oper.rbuf))
		oper.referr = syscall.Ftruncate(oper.parent.reffd, int64(oper.rbuf))
	case operMkdir:
		p := s.relativize(oper.pathname)
		oper.suterr = syscall.Mkdirat(s.sutcwd, p, oper.mode)
		oper.referr = syscall.Mkdirat(s.refcwd, p, oper.mode)
	case operRmdir:
		p := s.relativize(oper.pathname)
		oper.suterr = unix.Unlinkat(s.sutcwd, p, unix.AT_REMOVEDIR)
		oper.referr = unix.Unlinkat(s.refcwd, p, unix.AT_REMOVEDIR)
	case operRename1:
		oper.suterr = syscall.Rename(filepath.Join(sut.mnt, oper.pathname), filepath.Join(sut.mnt, oper.newpathname))
		oper.referr = syscall.Rename(filepath.Join(refDir, oper.pathname), filepath.Join(refDir, oper.newpathname))
	case operRename2:
		_, oper.suterr = sut.runCommand(fmt.Sprintf("rename %s %s", oper.pathname, oper.newpathname))
		if oper.suterr != nil && oper.suterr.Error() == "device or resource busy" {
			// Musclefs can't rename files if they have any fids pointing to them.
			// In that case, pretend the reference file system will also deny the operation.
			// Another approach would be to only generate “safe” rename2 operations but that seems more work.
			oper.referr = oper.suterr
		} else {
			oper.referr = syscall.Rename(filepath.Join(refDir, oper.pathname), filepath.Join(refDir, oper.newpathname))
		}
	case operChdir:
		f := func(oldcwd int, newcwdpath string) (newcwd int, err error) {
			if oldcwd <= 0 {
				panic(fmt.Sprintf("bad fd %d", oldcwd))
			}
			if err := syscall.Close(oldcwd); err != nil {
				return -1, err
			}
			fd, err := syscall.Open(newcwdpath, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC, 0)
			if err != nil {
				return -1, err
			}
			return fd, nil
		}
		oper.sutfd, oper.suterr = f(s.sutcwd, filepath.Join(sut.mnt, oper.pathname))
		oper.reffd, oper.referr = f(s.refcwd, filepath.Join(refDir, oper.pathname))
	case operMuscleFlush:
		_, oper.suterr = sut.runCommand("flush\n")
	case operMusclePush:
		_, oper.suterr = sut.runCommand("push\n")
	case operMuscleRemount:
		oper.suterr = sut.restart(s)
	case operMusclePruneCache:
		oper.suterr = func() error {
			if _, err := sut.runCommand("push\n"); err != nil {
				return err
			}
			if err := sut.waitForSnapshot(); err != nil {
				return err
			}
			return sut.pruneCache()
		}()
	case operMuscleTrim:
		_, oper.suterr = sut.runCommand("trim\n")
	case operSwapClients:
		oper.suterr = func() error {
			if err := s.closeAll(); err != nil {
				return fmt.Errorf("oper.run: %v", err)
			}
			if _, err := sut.runCommand("push\n"); err != nil {
				return fmt.Errorf("oper.run: %v", err)
			}
			if err := sut.waitForSnapshot(); err != nil {
				return fmt.Errorf("oper.run: %v", err)
			}
			suti++
			suti %= 2
			sut = filesystems[suti]
			worklog, err := sut.runCommand("pull\n")
			if err != nil {
				return fmt.Errorf("oper.run: %v", err)
			}
			s := bufio.NewScanner(bytes.NewReader(worklog))
			for s.Scan() {
				command := s.Text()
				logDebug("oper.run: got pull command %q", command)
				switch {
				case command[0] == '#':
					// Ignore comment.
				case strings.HasPrefix(command, "graft2 "), strings.HasPrefix(command, "unlink "), command == "flush", command == "pull":
					if _, err := sut.runCommand(command + "\n"); err != nil {
						return fmt.Errorf("run.oper: error running command %q from pull worklog: %v", command, err)
					}
				default:
					return fmt.Errorf("run.oper: unexpected command from pull worklog: %q", command)
				}
			}
			// No error from scanning a bytes.Reader.
			_ = s.Err()
			return nil
		}()
	default:
		panic(fmt.Sprintf("unknown oper code: %v", oper.code))
	}
}

func (op *oper) errorsMatch() bool {
	// Exception: relaxed comparison for rename(2), because I've spent too many hours trying to make ext4 and musclefs match exactly.
	// Same exception for unlink2, which is a musclefs-specific operation, so there's no point matching errors exactly.
	if op.code == operRename2 || op.code == operUnlink2 {
		good := op.suterr == nil && op.referr == nil
		good = good || (op.suterr != nil && op.referr != nil)
		return good
	}
	// Exception: Relaxed comparison for seek(2).
	// This discrepancy seems to come from the Linux 9p driver, rather than musclefs.
	// The discrepancy doesn't seem to have ripple effects, hence ignoring it for now.
	// TODO(nicolagi): Investigate more.
	if op.code == operSeek && op.suterr == nil && op.referr != nil && op.referr.Error() == "invalid argument" {
		return true
	}
	if op.suterr != nil && op.referr != nil {
		return op.suterr.Error() == op.referr.Error()
	}
	return op.suterr == nil && op.referr == nil
}

// Checks the operation on musclefs matches the corresponding one on the reference fs.
// This also does post-condition checks for musclefs-only operations.
func (op *oper) outputsMatch(seq *operSeq) error {
	if !op.errorsMatch() {
		return errors.New("oper.outputsMatch: mismatching errors")
	}
	if op.referr != nil {
		// No point doing other checks.
		return nil
	}
	switch op.code {
	case operCreate, operOpen:
		if op.sutfd < 0 || op.reffd < 0 {
			return fmt.Errorf("%v: negative fd(s)", op.code)
		}
	case operSeek:
		if op.sutoff != op.refoff {
			// It's not a 9P operation, musclefs doesn't even see the call to seek(2).
			logWarn("oper.outputsMatch: different offets after seek")
		}
	case operRead:
		if op.sutn != op.refn {
			return errors.New("read: number of bytes mismatch")
		} else if !bytes.Equal(op.sutbuf, op.refbuf) {
			return fmt.Errorf("read: mismatch sut=%q ref=%q", op.sutbuf, op.refbuf)
		}
	case operWrite:
		if op.sutn != op.refn {
			return errors.New("write: number of bytes mismatch")
		}
	case operClose:
	case operUnlink1:
	case operUnlink2:
	case operTruncate:
	case operFtruncate:
	case operMkdir:
	case operRmdir:
	case operRename1:
	case operRename2:
	case operChdir:
	case operMuscleFlush:
	case operMusclePush:
		count := func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
				return fmt.Errorf("found file: %v", path)
			}
			return nil
		}
		sut := filesystems[suti]
		if err := filepath.Walk(sut.staging, count); err != nil {
			// If a file was removed but there's a fid still pointing to it,
			// it makes sense that the staging are is not empty. So let's
			// close all files, and do the outputsMatch again. It should then pass.
			if err := seq.closeAll(); err != nil {
				return err
			}
			if err := filepath.Walk(sut.staging, count); err != nil {
				return err
			}
		}
	case operMuscleRemount:
	case operMusclePruneCache:
	case operMuscleTrim:
	case operSwapClients:
	default:
		panic(fmt.Sprintf("unknown op code: %v", op.code))
	}
	return nil
}
