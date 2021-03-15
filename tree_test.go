package main

import (
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/rogpeppe/go-internal/testscript"
)

func testscriptMain() int {
	hash, err := hashTree(os.Args[1], true, true)
	if err != nil {
		log.Print(err)
		return 1
	}
	fmt.Printf("%x\n", hash)
	return 0
}

func TestHashTree(t *testing.T) {
	testscript.Run(t, testscript.Params{
		Dir: "testdata",
	})
}

func TestMain(m *testing.M) {
	os.Exit(testscript.RunMain(m, map[string]func() int{
		"hash": testscriptMain,
	}))
}
