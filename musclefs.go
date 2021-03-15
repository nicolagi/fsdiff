package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/lionkov/go9p/p"
	"github.com/lionkov/go9p/p/clnt"
)

type musclefs struct {
	// The musclefs file system internal files are stored in a subtree
	// of the host file system, and is mounted in its mnt subdirectory.
	base string

	// Derived from base.
	cache          string
	ctl            string
	mnt            string
	propagationLog string
	socket         string
	staging        string

	// The musclefs command, and its input/output.
	cmd    *exec.Cmd
	stdout *os.File
	stderr *os.File
}

func newMuscleFS(testDir string, sutDir string, encryptionKey []byte) (*musclefs, error) {
	if err := os.Mkdir(sutDir, 0700); err != nil {
		return nil, err
	}
	if err := os.Mkdir(filepath.Join(sutDir, "mnt"), 0777); err != nil {
		return nil, err
	}
	sutout, err := os.Create(filepath.Join(sutDir, "stdout"))
	if err != nil {
		return nil, fmt.Errorf("newMuscleFS: %v", err)
	}
	suterr, err := os.Create(filepath.Join(sutDir, "stderr"))
	if err != nil {
		_ = sutout.Close()
		return nil, fmt.Errorf("newMuscleFS: %v", err)
	}
	f, err := os.OpenFile(filepath.Join(sutDir, "config"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		_ = sutout.Close()
		_ = suterr.Close()
		return nil, fmt.Errorf("newMuscleFS: %v", err)
	}
	socket := filepath.Join(sutDir, "muscle.sock")
	mountPoint := filepath.Join(sutDir, "mnt")
	_, err = fmt.Fprintf(f, `storage disk
disk-store-dir %s/s3surrogate
encryption-key %02x
listen-net unix
listen-addr %s
musclefs-mount %s
`, testDir, encryptionKey, socket, mountPoint)
	if err != nil {
		_ = sutout.Close()
		_ = suterr.Close()
		_ = f.Close()
		return nil, err
	}
	if err := f.Close(); err != nil {
		_ = sutout.Close()
		_ = suterr.Close()
		return nil, err
	}
	return &musclefs{
		base:           sutDir,
		cache:          filepath.Join(sutDir, "cache"),
		ctl:            filepath.Join(sutDir, "mnt", "ctl"),
		mnt:            filepath.Join(sutDir, "mnt"),
		propagationLog: filepath.Join(sutDir, "propagation.log"),
		socket:         socket,
		staging:        filepath.Join(sutDir, "staging"),
		stdout:         sutout,
		stderr:         suterr,
	}, nil
}

func (fs *musclefs) start() error {
	cmd := exec.Command("musclefs", "-D", "-fsdiff.blocksize=8192")
	cmd.Stdout = fs.stdout
	cmd.Stderr = fs.stderr
	cmd.Dir = fs.base
	cmd.Env = append(cmd.Env, fmt.Sprintf("MUSCLE_BASE=%s", fs.base))
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("musclefs.start: %v", err)
	}

	socket := filepath.Join(fs.base, "muscle.sock")
	user := p.OsUsers.Uid2User(os.Geteuid())
	c, err := clnt.Mount("unix", socket, user.Name(), 8192, user)
	for retries := 0; retries < 10 && err != nil; retries++ {
		time.Sleep(100 * time.Millisecond)
		c, err = clnt.Mount("unix", socket, user.Name(), 8192, user)
	}
	if err != nil {
		return fmt.Errorf("musclefs.start: %v", err)
	}
	c.Unmount()

	fs.cmd = cmd
	return nil
}

func (fs *musclefs) stop() error {
	if err := fs.cmd.Process.Signal(os.Interrupt); err != nil {
		return fmt.Errorf("musclefs.stop: could not interrupt %d: %v", fs.cmd.Process.Pid, err)
	}
	if err := fs.cmd.Wait(); err != nil {
		return fmt.Errorf("musclefs.stop: could not wait %d: %v", fs.cmd.Process.Pid, err)
	}
	return nil
}

func (fs *musclefs) mount() error {
	uid := os.Getuid()
	gid := os.Getgid()
	socket := filepath.Join(fs.base, "muscle.sock")
	mountPoint := filepath.Join(fs.base, "mnt")
	cmd := exec.Command("sudo", "mount", "-t", "9p", socket, mountPoint, "-o", fmt.Sprintf("trans=unix,dfltuid=%d,dfltgid=%d", uid, gid))
	combinedOutput, err := cmd.CombinedOutput()
	if err != nil {
		logInfo("musclefs.mount: %s", string(combinedOutput))
		return err
	}
	logInfo("musclefs.mount: mounted %s on %s", socket, mountPoint)
	return nil
}

func (fs *musclefs) unmount() error {
	umount := exec.Command("sudo", "umount", fs.mnt)
	umount.Stdout = os.Stdout
	umount.Stderr = os.Stderr
	if err := umount.Run(); err != nil {
		logError("musclefs.unmount: could not unmount (will run lsof to diagnose): %v", err)
		lsof := exec.Command("sudo", "lsof", fs.mnt)
		lsof.Stdout = os.Stdout
		lsof.Stderr = os.Stderr
		if err := lsof.Run(); err != nil {
			logWarn("musclefs.unmount: lsof failed: %v", err)
		}
		return err
	}
	return nil
}

func (fs *musclefs) restart(s *operSeq) error {
	if err := s.closeAll(); err != nil {
		return fmt.Errorf("musclefs.restart: %v", err)
	}
	if err := fs.unmount(); err != nil {
		return fmt.Errorf("musclefs.restart: %v", err)
	}
	if err := fs.stop(); err != nil {
		return fmt.Errorf("musclefs.restart: %v", err)
	}
	if err := fs.start(); err != nil {
		return fmt.Errorf("musclefs.restart: %v", err)
	}
	if err := fs.mount(); err != nil {
		return fmt.Errorf("musclefs.restart: %v", err)
	}
	return nil
}

func (fs *musclefs) runCommand(cmd string) ([]byte, error) {
	const maxResponseSize = 16384
	f, err := os.OpenFile(fs.ctl, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	if _, err := f.Write([]byte(cmd)); err != nil {
		_ = f.Close()
		s := err.Error()
		i := strings.Index(s, "ctl: ")
		if strings.HasPrefix(s, "write ") && i >= 0 {
			return nil, fmt.Errorf("%s", s[i+5:])
		}
		return nil, err
	}
	if _, err := f.Seek(0, 0); err != nil {
		return nil, err
	}
	b := make([]byte, maxResponseSize)
	n, err := f.Read(b)
	if err != nil && !errors.Is(err, io.EOF) {
		_ = f.Close()
		return nil, err
	}
	b = b[:n]
	logDebug("musclefs.runCommand: suti=%d cmd=%q output=%q", suti, cmd, string(b))
	if err := f.Close(); err != nil {
		return nil, err
	}
	return b, nil
}

func (fs *musclefs) pruneCache() error {
	rm := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		return os.Remove(path)
	}
	return filepath.Walk(fs.cache, rm)
}

func (fs *musclefs) isPropagating() (bool, error) {
	f, err := os.Open(fs.propagationLog)
	if err != nil {
		return false, fmt.Errorf("musclefs.isPropagating: %v", err)
	}
	s := bufio.NewScanner(f)
	for s.Scan() {
		if s.Bytes()[0] != 'd' {
			return true, f.Close()
		}
	}
	if err := s.Err(); err != nil {
		return false, fmt.Errorf("musclefs.isPropagating: %v", err)
	}
	return false, f.Close()
}

func (fs *musclefs) waitForSnapshot() error {
	for i := 0; i < 25; i++ {
		if yes, err := fs.isPropagating(); err != nil {
			return err
		} else if !yes {
			return nil
		}
		time.Sleep(250 * time.Millisecond)
	}
	return fmt.Errorf("musclefs.waitForSnapshot: timed out")
}
