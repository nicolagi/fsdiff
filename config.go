package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
)

// A random number falling between ranges[i-1].upperBound and
// ranges[i].upperBound corresponds to operation [i].oper, with a
// fictitious value of 0 for ranges[-1].upperBound.
type probabilityRanges []struct {
	upperBound int
	oper       operKind
}

func (rs probabilityRanges) String() string {
	var b bytes.Buffer
	r := rs[0]
	_, _ = fmt.Fprintf(&b, "{ %q: %d–%d", r.oper, 0, r.upperBound)
	prev := r.upperBound
	for _, r := range rs[1:] {
		_, _ = fmt.Fprintf(&b, ", %q: %d–%d", r.oper, prev, r.upperBound)
		prev = r.upperBound
	}
	b.WriteString(" }")
	return b.String()
}

type config struct {
	ProbabilitiesRaw map[string]int `json:"probabilities"`
	probabilities    map[operKind]int
}

func loadConfig(r io.Reader) (*config, error) {
	var c config
	if err := json.NewDecoder(r).Decode(&c); err != nil {
		return nil, fmt.Errorf("loadConfig: decoding JSON: %v", err)
	}
	c.probabilities = make(map[operKind]int)
	if c.ProbabilitiesRaw == nil {
		for oper := operKind(0); oper < operKindCount; oper++ {
			c.probabilities[oper] = 1
		}
		c.rescaleProbabilities()
	} else {
		for operName, p := range c.ProbabilitiesRaw {
			c.probabilities[fromString(operName)] = p
		}
		if l := len(c.probabilities); l != int(operKindCount) {
			return nil, fmt.Errorf("loadConfig: incomplete probabilities: %d/%d", l, operKindCount)
		}
		c.rescaleProbabilities()
	}
	return &c, nil
}

func (c *config) probabilityRanges() (ranges probabilityRanges) {
	prev := 0
	for oper, percentage := range c.probabilities {
		curr := prev + percentage
		ranges = append(ranges, struct {
			upperBound int
			oper       operKind
		}{
			upperBound: curr,
			oper:       oper,
		})
		prev = curr
	}
	return
}

func (c *config) randomizeProbabilities() {
	for oper := operKind(0); oper < operKindCount; oper++ {
		c.probabilities[oper] = rand.Intn(100)
	}
	c.rescaleProbabilities()
}

func (c *config) String() string {
	var b bytes.Buffer
	_, _ = fmt.Fprintf(&b, "{ %q: %d", operKind(0), c.probabilities[operKind(0)])
	for oper := operKind(1); oper < operKindCount; oper++ {
		_, _ = fmt.Fprintf(&b, ", %q: %d", oper, c.probabilities[oper])
	}
	b.WriteString(" }")
	return b.String()
}

// rescaleProbabilities arranges for the probabilities to add up to 100.
func (c *config) rescaleProbabilities() {
	sum := 0
	for oper := operKind(0); oper < operKindCount; oper++ {
		sum += c.probabilities[oper]
	}
	newSum := 0
	for oper := operKind(0); oper < operKindCount; oper++ {
		c.probabilities[oper] = c.probabilities[oper] * 100 / sum
		newSum += c.probabilities[oper]
	}
	c.probabilities[0] += 100 - newSum
}
