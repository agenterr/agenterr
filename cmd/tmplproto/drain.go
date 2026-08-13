// Drain-style online template extractor — prototype for the Step-0 gate.
// Deliberately simple: space tokenization, similarity grouping by
// (token count, first token), and APPEND-ONLY templates: generalizing an
// existing template mints a new template ID instead of mutating tokens
// in place, so any previously returned (id, vars) reconstructs forever.
// The production engine keeps this invariant (spec §2).
package main

import (
	"fmt"
	"strings"
)

const wild = "\x00" // wildcard slot marker; NUL cannot survive tokenization of sane logs

type tmpl struct {
	id     int
	tokens []string
}

type drain struct {
	groups    map[string][]*tmpl
	templates []*tmpl // index = id-1
	simThresh float64
}

func newDrain() *drain {
	return &drain{groups: map[string][]*tmpl{}, simThresh: 0.5}
}

func groupKey(tokens []string) string {
	first := tokens[0]
	if strings.ContainsAny(first, "0123456789") {
		first = wild // digit-bearing first tokens (timestamps, IPs) all group together
	}
	return fmt.Sprintf("%d|%s", len(tokens), first)
}

// similarity is the fraction of positions where the template token
// matches exactly or is already a wildcard.
func similarity(t *tmpl, tokens []string) float64 {
	same := 0
	for i, tok := range tokens {
		if t.tokens[i] == wild || t.tokens[i] == tok {
			same++
		}
	}
	return float64(same) / float64(len(tokens))
}

func (d *drain) newTemplate(tokens []string, key string) *tmpl {
	t := &tmpl{id: len(d.templates) + 1, tokens: append([]string(nil), tokens...)}
	d.templates = append(d.templates, t)
	d.groups[key] = append(d.groups[key], t)
	return t
}

func (d *drain) Extract(body string) (int, []string, bool) {
	if strings.ContainsRune(body, '\n') || strings.ContainsRune(body, '\x00') || body == "" {
		return 0, nil, false
	}
	tokens := strings.Split(body, " ")
	if len(tokens) > 200 {
		return 0, nil, false
	}
	key := groupKey(tokens)

	var best *tmpl
	bestSim := 0.0
	for _, t := range d.groups[key] {
		if s := similarity(t, tokens); s > bestSim {
			best, bestSim = t, s
		}
	}

	var target *tmpl
	switch {
	case best == nil || bestSim < d.simThresh:
		target = d.newTemplate(tokens, key) // exact template, zero vars
	default:
		mutate := false
		for i, tok := range tokens {
			if best.tokens[i] != wild && best.tokens[i] != tok {
				mutate = true
				break
			}
		}
		if !mutate {
			target = best
		} else {
			// Append-only: mint the generalized template as a NEW id.
			merged := append([]string(nil), best.tokens...)
			for i, tok := range tokens {
				if merged[i] != wild && merged[i] != tok {
					merged[i] = wild
				}
			}
			target = d.newTemplate(merged, key)
		}
	}

	var vars []string
	for i, tok := range target.tokens {
		if tok == wild {
			vars = append(vars, tokens[i])
		}
	}
	// Verify the invariant at extract time; a failed round trip means
	// tokenization lost information (e.g. double spaces) → raw fallback.
	if got, ok := d.Reconstruct(target.id, vars); !ok || got != body {
		return 0, nil, false
	}
	return target.id, vars, true
}

func (d *drain) Reconstruct(id int, vars []string) (string, bool) {
	if id < 1 || id > len(d.templates) {
		return "", false
	}
	t := d.templates[id-1]
	out := make([]string, len(t.tokens))
	vi := 0
	for i, tok := range t.tokens {
		if tok == wild {
			if vi >= len(vars) {
				return "", false
			}
			out[i] = vars[vi]
			vi++
		} else {
			out[i] = tok
		}
	}
	if vi != len(vars) {
		return "", false
	}
	return strings.Join(out, " "), true
}

func (d *drain) TemplateCount() int { return len(d.templates) }

func (d *drain) TemplateBytes() int {
	n := 0
	for _, t := range d.templates {
		n += len(strings.Join(t.tokens, " "))
	}
	return n
}
