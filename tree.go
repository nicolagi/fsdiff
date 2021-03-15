package main

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
)

func hashTree(path string, includeMeta, includeContent bool) ([]byte, error) {
	var b bytes.Buffer
	if err := hashAny(&b, path, "", includeMeta, includeContent); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func hashAny(buf *bytes.Buffer, base, rel string, includeMeta, includeContent bool) error {
	f, err := os.Stat(filepath.Join(base, rel))
	if err != nil {
		return fmt.Errorf("hashAny: %v", err)
	}
	if f.IsDir() {
		if includeMeta {
			_, _ = fmt.Fprintf(buf, "path=%q mode=0%o\n", rel, f.Mode())
		}
		children, err := os.ReadDir(filepath.Join(base, rel))
		if err != nil {
			return fmt.Errorf("hashAny: %w", err)
		}
		sort.Slice(children, func(i, j int) bool {
			return children[i].Name() < children[j].Name()
		})
		for _, child := range children {
			if err := hashAny(buf, base, filepath.Join(rel, child.Name()), includeMeta, includeContent); err != nil {
				return err
			}
		}
	} else {
		if includeMeta {
			_, _ = fmt.Fprintf(buf, "path=%q size=%d mode=0%o\n", rel, f.Size(), f.Mode())
		}
		if includeContent {
			b, err := ioutil.ReadFile(filepath.Join(base, rel))
			if err != nil {
				return fmt.Errorf("hashAny: %w", err)
			}
			_, _ = fmt.Fprintf(buf, "path=%q hash=%x\n", rel, sha256.Sum256(b))
		}
	}
	return nil
}
