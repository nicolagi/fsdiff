package main

import (
	"fmt"
	"strconv"
	"strings"
)

// Represents how often hashing of fs metadata and fs contents
// happens, where the unit of time is the operation count.
type hashPeriods struct {
	hashMetadata int
	hashContents int
}

// String implements fmt.Stringer and flag.Value.
func (p *hashPeriods) String() string {
	return fmt.Sprintf("{metadata=%d,contents=%d}", p.hashMetadata, p.hashContents)
}

// Set implements flag.Value.
func (p *hashPeriods) Set(s string) error {
	parts := strings.Split(s, ",")
	if l := len(parts); l != 2 {
		return fmt.Errorf("too many tokens (%d), want 2 comma-separated ints", l)
	}
	metadata, err := strconv.Atoi(parts[0])
	if err != nil {
		return err
	}
	contents, err := strconv.Atoi(parts[1])
	if err != nil {
		return err
	}
	if metadata <= 0 || contents <= 0 {
		return fmt.Errorf("both periods must be positive integers")
	}
	if contents%metadata != 0 {
		return fmt.Errorf("the contents period must be a multiple of the metadata period")
	}
	p.hashMetadata = metadata
	p.hashContents = contents
	return nil
}
