// go:build linux
package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/gops/agent"
)

var (
	// Holds all files and directories for a run of fsdiff.
	testDir string

	// The reference file system is a subtree of the host file system,
	// namely, its subtree rooted at the temporary directory refDir.
	refDir string

	// The current system under test is filesystems[sut].
	// There are two instances to test push (from one) and pull (from the other).
	filesystems [2]*musclefs
	suti        int

	// Summary of the contents of the fs after the last successful comparison between
	// reference file system and file system under test.
	lastTreeDescription []byte
)

// Linux mode bits.
var _ uint32 = 0777 | syscall.S_ISGID | syscall.S_ISUID | syscall.S_ISVTX

func beforeAll() (err error) {
	testDir, err = ioutil.TempDir("", "fsdiff-*")
	if err != nil {
		return fmt.Errorf("beforeAll: %v", err)
	}
	refDir = filepath.Join(testDir, "ref")
	if err = os.Mkdir(refDir, 0700); err != nil {
		return fmt.Errorf("beforeAll: %v", err)
	}
	encryptionKey := make([]byte, 16)
	rand.Read(encryptionKey)
	if filesystems[0], err = newMuscleFS(testDir, filepath.Join(testDir, "sut0"), encryptionKey); err != nil {
		return fmt.Errorf("beforeAll: %v", err)
	}
	if filesystems[1], err = newMuscleFS(testDir, filepath.Join(testDir, "sut1"), encryptionKey); err != nil {
		return fmt.Errorf("beforeAll: %v", err)
	}

	for _, fs := range filesystems {
		if err := fs.start(); err != nil {
			return fmt.Errorf("beforeAll: %v", err)
		}
		if err := fs.mount(); err != nil {
			return fmt.Errorf("beforeAll: %v", err)
		}
	}

	suti = 0
	return nil
}

func afterAll() {
	for _, fs := range filesystems {
		if err := fs.unmount(); err != nil {
			logWarn("afterAll: %v", err)
		}
		if err := fs.stop(); err != nil {
			logWarn("afterAll: %v", err)
		}
		_ = fs.stdout.Close()
		_ = fs.stderr.Close()
	}
}

// Main loop for random sequential operation sequences.
func runOperations(max int, periods hashPeriods, cfg *config) error {
	seq := operSeq{
		maxOpers:      int32(max),
		ranges:        cfg.probabilityRanges(),
		existingDirs:  make(map[string]struct{}),
		existingFiles: make(map[string]struct{}),
		sutcwd:        -1,
		refcwd:        -1,
	}
	logInfo("ranges: %v", seq.ranges)
	defer func() {
		if err := seq.closeAll(); err != nil {
			logWarn("runOperations: %v", err)
		}
	}()
	for {
		if seq.sutcwd == -1 || seq.refcwd == -1 {
			if err := seq.opencwds(); err != nil {
				return fmt.Errorf("runOperations: %w", err)
			}
		}
		op := seq.nextOper()
		if op == nil {
			return nil
		}
		if err := seq.run(op); err != nil {
			return fmt.Errorf("runOperations: %v", err)
		}
		sutDesc, err := hashTree(filesystems[suti].mnt, op.id%periods.hashMetadata == 0, op.id%periods.hashContents == 0)
		if err != nil {
			return fmt.Errorf("runOperations: %v", err)
		}
		refDesc, err := hashTree(refDir, op.id%periods.hashMetadata == 0, op.id%periods.hashContents == 0)
		if err != nil {
			return fmt.Errorf("runOperations: %v", err)
		}
		if diff := cmp.Diff(sutDesc, refDesc); diff != "" {
			logError("Tree difference between fs under test and reference fs: %s", diff)
			logError("Tree difference between fs under test and previous description of fs under test: %s", cmp.Diff(sutDesc, lastTreeDescription))
			return fmt.Errorf("runOperations: hashes do not match")
		}
		lastTreeDescription = sutDesc
	}
}

func main() {
	_ = agent.Listen(agent.Options{})
	configPath := flag.String("c", "", "`path` to configuration")
	randomProbabilities := flag.Bool("r", false, "generate random probabilities")
	max := flag.Int("m", 100, "max number of operations")
	seed := flag.Int64("seed", time.Now().UnixNano(), "")
	periods := hashPeriods{hashMetadata: 1, hashContents: 250}
	flag.CommandLine.Var(&periods, "periods", "how often to compare fs hashes")
	shell := flag.Bool("shell", false, "run a shell instead of random operations")
	flag.Parse()
	if flag.NArg() != 0 {
		flag.Usage()
		os.Exit(1)
	}

	var cfg *config
	if *configPath != "" {
		if (*configPath)[0] != '/' {
			*configPath = filepath.Join("/exper/etc/fsdiff", *configPath)
		}
		f, err := os.Open(*configPath)
		if err != nil {
			logFatal("fsdiff: %v", err)
		}
		cfg, err = loadConfig(f)
		_ = f.Close()
		if err != nil {
			logFatal("fsdiff: %v", err)
		}
	} else {
		cfg, _ = loadConfig(strings.NewReader("{}"))
	}

	logInfo("Setting seed=%d", *seed)
	rand.Seed(*seed)

	if *randomProbabilities {
		cfg.randomizeProbabilities()
		logInfo(cfg.String())
	}

	if err := beforeAll(); err != nil {
		logFatal("fsdiff: %v", err)
	}
	if *shell {
		cmd := exec.Command(os.Getenv("SHELL"))
		cmd.Dir = testDir
		cmd.Stdin = os.Stdin
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			logError("fsdiff: %v", err)
		}
	} else {
		if err := runOperations(*max, periods, cfg); err != nil {
			logError("fsdiff: %v", err)
			afterAll()
			os.Exit(1)
		}
	}
	afterAll()
}
