package main

import (
	"fmt"
	"io"
	"math/rand"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"golang.org/x/sys/unix"
)

type operSeq struct {
	mu sync.Mutex

	opersDone int32
	maxOpers  int32
	ranges    probabilityRanges

	sutcwd  int
	refcwd  int
	cwdpath string

	existingDirs  map[string]struct{}
	existingFiles map[string]struct{}
	openOpers     []*oper
}

func (seq *operSeq) run(op *oper) error {
	op.run(seq)
	logInfo("operSeq.run: op=%v", op)
	if err := op.outputsMatch(seq); err != nil {
		return fmt.Errorf("operSeq.run %q: %v", op.code, err)
	}
	seq.mu.Lock()
	defer seq.mu.Unlock()
	// Bookkeeping:
	switch op.code {
	case operCreate, operOpen:
		if op.referr == nil {
			seq.existingFiles[op.pathname] = struct{}{}
			seq.openOpers = append(seq.openOpers, op)
		}
	case operSeek:
	case operRead:
	case operWrite:
	case operClose:
		if op.referr == nil {
			openOpers := make([]*oper, 0, len(seq.openOpers)-1)
			for _, o := range seq.openOpers {
				if o != op.parent {
					openOpers = append(openOpers, o)
				}
			}
			seq.openOpers = openOpers
		}
	case operUnlink1:
		if op.referr == nil {
			delete(seq.existingFiles, op.pathname)
		}
	case operUnlink2:
		if op.referr == nil {
			for d := range seq.existingDirs {
				if strings.HasPrefix(d, op.pathname) {
					delete(seq.existingDirs, d)
				}
			}
			for f := range seq.existingFiles {
				if strings.HasPrefix(f, op.pathname) {
					delete(seq.existingFiles, f)
				}
			}
		}
	case operTruncate:
	case operFtruncate:
	case operMkdir:
		if op.referr == nil {
			seq.existingDirs[op.pathname] = struct{}{}
		}
	case operRmdir:
		if op.referr == nil {
			delete(seq.existingDirs, op.pathname)
		}
	case operRename1:
		if op.referr == nil {
			if strings.HasPrefix(seq.cwdpath, op.pathname) {
				prev := seq.cwdpath
				seq.cwdpath = op.newpathname + seq.cwdpath[len(op.pathname):]
				logDebug("changed cwdpath from %q to %q after rename1", prev, seq.cwdpath)
			}
			if _, ok := seq.existingDirs[op.pathname]; ok {
				delete(seq.existingDirs, op.pathname)
				seq.existingDirs[op.newpathname] = struct{}{}
			}
			if _, ok := seq.existingFiles[op.pathname]; ok {
				delete(seq.existingFiles, op.pathname)
				seq.existingFiles[op.newpathname] = struct{}{}
			}
		}
	case operRename2:
		if op.referr == nil {
			if strings.HasPrefix(seq.cwdpath, op.pathname) {
				seq.cwdpath = op.newpathname + seq.cwdpath[len(op.pathname):]
			}
			var tomove []string
			for f := range seq.existingFiles {
				if strings.HasPrefix(f, op.pathname) {
					tomove = append(tomove, f)
				}
			}
			for _, f := range tomove {
				newf := op.newpathname + f[len(op.pathname):]
				delete(seq.existingFiles, f)
				seq.existingFiles[newf] = struct{}{}
			}
			tomove = nil
			for f := range seq.existingDirs {
				if strings.HasPrefix(f, op.pathname) {
					tomove = append(tomove, f)
				}
			}
			for _, f := range tomove {
				newf := op.newpathname + f[len(op.pathname):]
				delete(seq.existingDirs, f)
				seq.existingDirs[newf] = struct{}{}
			}
		}
	case operChdir:
		// -1 is okay as well, opencwds will be called later.
		seq.sutcwd = op.sutfd
		seq.refcwd = op.reffd
		if op.referr == nil {
			logDebug("updated cwdpath from %q to %q after chdir", seq.cwdpath, op.pathname)
			seq.cwdpath = op.pathname
		}
	case operMuscleFlush:
	case operMusclePush:
	case operMuscleRemount:
	case operMusclePruneCache:
	case operMuscleTrim:
	case operSwapClients:
	default:
		logFatal("operSeq.run: unknown op code: %v", op.code)
	}
	atomic.AddInt32(&seq.opersDone, 1)
	return nil
}

var natoAlphabet = []string{
	"alfa",
	"bravo",
	"charlie",
	"delta",
	"echo",
	"foxtrot",
	"golf",
	"hotel",
	"india",
	"juliett",
	"kilo",
	"lima",
	"mike",
	"november",
	"oscar",
	"papa",
	"quebec",
	"romeo",
	"sierra",
	"tango",
	"uniform",
	"victor",
	"whiskey",
	"x-ray",
	"yankee",
	"zulu",
}

func (seq *operSeq) relativize(p string) string {
	q, err := filepath.Rel("/"+seq.cwdpath, "/"+p)
	if err != nil {
		panic(err)
	}
	logDebug("operSeq.relativize: remapped %q to %q relative to %q", p, q, seq.cwdpath)
	return q
}

func (seq *operSeq) randomDir(maxElements int, existingProbability int) string {
	if rand.Intn(100) < existingProbability {
		if len(seq.existingDirs) == 0 {
			return ""
		}
		i := rand.Intn(len(seq.existingDirs))
		j := 0
		for f := range seq.existingDirs {
			if j < i {
				j++
				continue
			}
			if len(strings.Split(f, "/")) <= maxElements {
				return f
			}
		}
		return ""
	}
	attempts := 100
again:
	elements := make([]string, maxElements)
	for i := 0; i < maxElements; i++ {
		elements[i] = natoAlphabet[rand.Intn(len(natoAlphabet))]
	}
	candidate := strings.Join(elements, "/")
	_, ok1 := seq.existingFiles[candidate]
	_, ok2 := seq.existingDirs[candidate]
	if (ok1 || ok2) && attempts > 0 {
		attempts--
		goto again
	}
	return candidate
}

func (seq *operSeq) randomFile(maxElements int, existingProbability int) string {
	if rand.Intn(100) < existingProbability {
		if len(seq.existingFiles) == 0 {
			return ""
		}
		i := rand.Intn(len(seq.existingFiles))
		j := 0
		for f := range seq.existingFiles {
			if j < i {
				j++
				continue
			}
			if len(strings.Split(f, "/")) <= maxElements {
				return f
			}
		}
		return ""
	}
	attempts := 100
again:
	elements := make([]string, maxElements)
	for i := 0; i < maxElements; i++ {
		elements[i] = natoAlphabet[rand.Intn(len(natoAlphabet))]
	}
	candidate := strings.Join(elements, "/")
	_, ok1 := seq.existingFiles[candidate]
	_, ok2 := seq.existingDirs[candidate]
	if (ok1 || ok2) && attempts > 0 {
		attempts--
		goto again
	}
	return candidate
}

func (seq *operSeq) randomPathname(existingDirProbability, existingFileProbability, nestingProbability int) string {
	n := rand.Intn(100)
	var m map[string]struct{}
	switch {
	case 0 <= n && n < existingDirProbability && len(seq.existingDirs) > 0:
		m = seq.existingDirs
	case existingDirProbability <= n && n < existingDirProbability+existingFileProbability && len(seq.existingFiles) > 0:
		m = seq.existingFiles
	}
	if len(m) > 0 {
		i := rand.Int() % len(m)
		j := 0
		for p := range m {
			if i == j {
				return p
			}
			j++
		}
	}
	// If we're here, we want to generate a pathname that does not correspond to an existing file or directory.
	// We may want to nest the directory structure.
	if len(seq.existingDirs) > 0 && rand.Intn(100) < nestingProbability {
		// We want to nest. Pick a random existing dir first:
		i := rand.Intn(len(seq.existingDirs))
		j := 0
		var dir string
		for dir = range seq.existingDirs {
			if i == j {
				break
			}
			j++
		}
		return filepath.Join(dir, natoAlphabet[rand.Int()%len(natoAlphabet)])
	}
	return natoAlphabet[rand.Int()%len(natoAlphabet)]
}

func (seq *operSeq) randomOperKind() operKind {
	n := int(rand.Float64() * 100.0)
	for _, r := range seq.ranges {
		if n < r.upperBound {
			return r.oper
		}
	}
	logFatal("operSeq: randomOperKind: %d does not fit in %v", n, seq.ranges)
	panic("not reached")
}

func (seq *operSeq) opencwds() error {
	if seq.sutcwd != -1 || seq.refcwd != -1 {
		return fmt.Errorf("operSeq.opencwds: not both closed sutcwd=%d refcwd=%d", seq.sutcwd, seq.refcwd)
	}
	p := filepath.Join(filesystems[suti].mnt, seq.cwdpath)
	logDebug("operSeq.opencwds: opening %seq as sut cwd", p)
	// Cf. ../musl/src/dirent/opendir.c and ../musl/src/fcntl/open.c.
	sutcwd, err := syscall.Open(p, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("operSeq.opencwds: opening %q: %w", p, err)
	}
	p = filepath.Join(refDir, seq.cwdpath)
	logDebug("operSeq.opencwds: opening %seq as ref cwd", p)
	refcwd, err := syscall.Open(p, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC, 0)
	if err != nil {
		_ = syscall.Close(sutcwd)
		return fmt.Errorf("operSeq.opencwds: opening %q: %w", p, err)
	}
	seq.sutcwd = sutcwd
	seq.refcwd = refcwd
	logDebug("operSeq.opencwds: sutcwd=%d refcwd=%d", seq.sutcwd, seq.refcwd)
	return nil
}

func (seq *operSeq) closecwds() error {
	if seq.sutcwd != -1 {
		logDebug("operSeq.closecwds: closing suti=%d sutcwd=%d", suti, seq.sutcwd)
		fd := seq.sutcwd
		seq.sutcwd = -1
		if err := syscall.Close(fd); err != nil {
			return fmt.Errorf("operSeq.closecwds: %d: %v", fd, err)
		}
	}
	if seq.refcwd != -1 {
		logDebug("operSeq.closecwds: closing suti=%d refcwd=%d", suti, seq.refcwd)
		fd := seq.refcwd
		seq.refcwd = -1
		if err := syscall.Close(fd); err != nil {
			return fmt.Errorf("operSeq.closecwds: %d: %v", fd, err)
		}
	}
	return nil
}

func (seq *operSeq) nextOper() *oper {
	seq.mu.Lock()
	defer seq.mu.Unlock()
	if atomic.LoadInt32(&seq.opersDone) >= seq.maxOpers {
		return nil
	}
again:
	op := &oper{id: int(atomic.LoadInt32(&seq.opersDone)), code: seq.randomOperKind()}
	switch op.code {
	case operCreate:
		// op.mode = rand.Uint32() & supportedModeBits
		op.mode = 0777
		// 5% existing directory, 5% existing file, 90% new node, 20% chance of nesting in the latter case.
		op.pathname = seq.randomPathname(5, 5, 20)
	case operOpen:
		op.flags = randomOpenFlags()
		// Cf. ../musl/src/fcntl/open.c.
		if op.flags&syscall.O_CREAT != 0 || op.flags&unix.O_TMPFILE == unix.O_TMPFILE {
			// op.mode = rand.Uint32() & supportedModeBits
			op.mode = 0777
		}
		// 25% existing directory, 65% existing file, 10% new node, 20% chance of nesting in the latter case.
		op.pathname = seq.randomPathname(25, 65, 20)
	case operSeek:
		if len(seq.openOpers) == 0 {
			logDebug("again from seek")
			goto again
		}
		op.parent = seq.openOpers[rand.Intn(len(seq.openOpers))]
		op.offset = int64(rand.Intn(1024))
		switch rand.Intn(3) {
		case 0:
			op.whence = io.SeekStart
		case 1:
			op.whence = io.SeekCurrent
		case 2:
			op.whence = io.SeekEnd
		}
	case operRead:
		if len(seq.openOpers) == 0 {
			logDebug("again from read")
			goto again
		}
		op.parent = seq.openOpers[rand.Intn(len(seq.openOpers))]
		op.rbuf = rand.Intn(512)
	case operWrite:
		if len(seq.openOpers) == 0 {
			logDebug("again from write")
			goto again
		}
		op.parent = seq.openOpers[rand.Intn(len(seq.openOpers))]
		sz := rand.Intn(512)
		op.wbuf = make([]byte, sz)
		rand.Read(op.wbuf)
	case operClose:
		if len(seq.openOpers) == 0 {
			logDebug("again from close")
			goto again
		}
		op.parent = seq.openOpers[rand.Intn(len(seq.openOpers))]
	case operUnlink1:
		// 25% existing directory, 65% existing file, 10% new node, 20% chance of nesting in the latter case.
		op.pathname = seq.randomPathname(25, 65, 20)
	case operUnlink2:
		// 50% existing directory, 40% existing file, 10% new node, 20% chance of nesting in the latter case.
		op.pathname = seq.randomPathname(50, 40, 20)
		// Don't try removing the current directory.
		// It makes operSwapClients impossible.
		if strings.HasPrefix(seq.cwdpath, op.pathname) {
			logDebug("again from unlink2")
			goto again
		}
	case operTruncate:
		// 10% existing directory, 70% existing file, 20% new node, 50% chance of nesting in the latter case.
		op.pathname = seq.randomPathname(10, 70, 50)
		op.rbuf = rand.Intn(512)
	case operFtruncate:
		if len(seq.openOpers) == 0 {
			logDebug("again from ftruncate")
			goto again
		}
		op.parent = seq.openOpers[rand.Intn(len(seq.openOpers))]
		op.rbuf = rand.Intn(512)
	case operMkdir:
		// op.mode = rand.Uint32() & supportedmodebits
		op.mode = 0777
		// 10% existing directory, 10% existing file, 80% new node, 20% chance of nesting in the latter case.
		op.pathname = seq.randomPathname(10, 10, 20)
	case operRmdir:
		// 65% existing directory, 15% existing file, 20% new node, 20% chance of nesting in the latter case.
		op.pathname = seq.randomPathname(65, 15, 20)
	case operRename1:
		if rand.Intn(2) == 0 {
			op.pathname = seq.randomDir(5, 75) // max 4 levels deep, 75% existing directory
		} else {
			op.pathname = seq.randomFile(5, 75) // max 4 levels deep, 75% existing file
		}
		if op.pathname == "" {
			logDebug("again from rename1")
			goto again
		}
		newname := natoAlphabet[rand.Intn(len(natoAlphabet))]
		op.newpathname = filepath.Join(filepath.Dir(op.pathname), newname)
		logDebug("operSeq.nextOper: rename1 %q %q", op.pathname, op.newpathname)
	case operRename2:
		switch rand.Intn(4) {
		case 0:
			op.pathname = seq.randomFile(3, 75)    // at most 2 levels deep, 75% existing file
			op.newpathname = seq.randomFile(3, 75) // at most 2 levels deep, 75% existing file
			if op.pathname == "" || op.newpathname == "" {
				logDebug("again from rename2")
				goto again
			}
		case 1:
			op.pathname = seq.randomDir(3, 75)    // at most 2 levels deep, 75% existing directory
			op.newpathname = seq.randomDir(3, 75) // at most 2 levels deep, 75% existing directory
			if op.pathname == "" || op.newpathname == "" {
				logDebug("again from rename2")
				goto again
			}
		case 2:
			op.pathname = seq.randomFile(3, 75)   // at most 2 levels deep, 75% existing file
			op.newpathname = seq.randomDir(3, 75) // at most 2 levels deep, 75% existing directory
			if op.pathname == "" || op.newpathname == "" {
				logDebug("again from rename2")
				goto again
			}
		case 3:
			op.pathname = seq.randomDir(3, 75)     // at most 2 levels deep, 75% existing directory
			op.newpathname = seq.randomFile(3, 75) // at most 2 levels deep, 75% existing file
			if op.pathname == "" || op.newpathname == "" {
				logDebug("again from rename2")
				goto again
			}
		}
	case operChdir:
		dir := seq.randomDir(3, 100) // at most 2 levels deep, necessarily an existing directory
		if seq.cwdpath == dir {
			logDebug("again from chdir")
			goto again
		}
		op.pathname = dir
	case operMuscleFlush:
	case operMusclePush:
	case operMuscleRemount:
	case operMusclePruneCache:
	case operMuscleTrim:
	case operSwapClients:
	default:
		panic(fmt.Sprintf("unknown op code: %v", op.code))
	}
	return op
}

func (seq *operSeq) closeAll() error {
	seq.mu.Lock()
	defer seq.mu.Unlock()
	for _, f := range seq.openOpers {
		if f.sutfd != -1 {
			if err := syscall.Close(f.sutfd); err != nil {
				return fmt.Errorf("operSeq.closeAll: %v", err)
			}
		}
		if f.reffd != -1 {
			if err := syscall.Close(f.reffd); err != nil {
				return fmt.Errorf("operSeq.closeAll: %v", err)
			}
		}
		logDebug("operSeq.closeAll: closed %v", f)
	}
	seq.openOpers = nil
	if err := seq.closecwds(); err != nil {
		return fmt.Errorf("operSeq.closeAll: %v", err)
	}
	return nil
}
