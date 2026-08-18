// Package template implements lossless, append-only log template
// extraction (spec §2) — the storage engine's core primitive. A log body
// resolves to (templateID, vars) such that substituting vars back into
// the template reproduces the exact original bytes; that invariant is
// verified per extraction, and any failure (or any body that cannot
// tokenize: empty, multiline, NUL bytes, >200 tokens) falls back to
// RawID. Templates never mutate after creation: generalizing an existing
// template mints a NEW id, so any previously returned (id, vars) pair
// reconstructs forever. A per-project cap bounds the template table —
// high-entropy bodies (validated adversarially during Step-0) would
// otherwise mint one template per line.
package template

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

// Wild marks a variable slot inside a stored template text. NUL cannot
// occur in template-able bodies (they go raw), so it is unambiguous.
const Wild = "\x00"

// RawID is the reserved template id for bodies stored verbatim.
const RawID int64 = 0

// Row is a persisted template.
type Row struct {
	ID   int64
	Text string // tokens joined with " ", variable slots as Wild
}

// Store persists templates. Implementations must return ids that are
// unique per project and stable across restarts. Calls happen under the
// Extractor's lock — implementations should be fast.
type Store interface {
	InsertTemplate(ctx context.Context, projectID int64, text string) (int64, error)
	LoadTemplates(ctx context.Context, projectID int64) ([]Row, error)
}

type tmpl struct {
	id     int64
	tokens []string
}

type project struct {
	groups map[string][]*tmpl // groupKey → candidates
	byID   map[int64]*tmpl
}

// Extractor is safe for concurrent use.
type Extractor struct {
	mu        sync.Mutex
	store     Store
	cap       int
	simThresh float64
	projects  map[int64]*project
}

// NewExtractor returns an Extractor persisting through s. capPerProject
// ≤ 0 selects the default of 100_000 (spec §2).
func NewExtractor(s Store, capPerProject int) *Extractor {
	if capPerProject <= 0 {
		capPerProject = 100_000
	}
	return &Extractor{store: s, cap: capPerProject, simThresh: 0.5, projects: map[int64]*project{}}
}

// Extract resolves body to (id, vars, true) or reports a raw fallback
// (RawID, nil, false, nil). A non-nil error means template persistence
// failed and nothing was minted.
func (e *Extractor) Extract(ctx context.Context, projectID int64, body string) (int64, []string, bool, error) {
	if body == "" || strings.ContainsAny(body, "\n"+Wild) {
		return RawID, nil, false, nil
	}
	tokens := strings.Split(body, " ")
	if len(tokens) > 200 {
		return RawID, nil, false, nil
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	p, err := e.load(ctx, projectID)
	if err != nil {
		return 0, nil, false, err
	}

	key := groupKey(tokens)
	target, mintTokens := resolve(p.groups[key], tokens, e.simThresh)

	tks := mintTokens
	if target != nil {
		tks = target.tokens
	}
	vars := varsFor(tks, tokens)
	if substitute(tks, vars) != body { // invariant check BEFORE any mint
		return RawID, nil, false, nil
	}

	if target == nil {
		if len(p.byID) >= e.cap {
			return RawID, nil, false, nil
		}
		id, err := e.store.InsertTemplate(ctx, projectID, strings.Join(mintTokens, " "))
		if err != nil {
			return 0, nil, false, fmt.Errorf("template: persist: %w", err)
		}
		target = &tmpl{id: id, tokens: mintTokens}
		p.groups[key] = append(p.groups[key], target)
		p.byID[id] = target
	}
	return target.id, vars, true, nil
}

// Reconstruct rebuilds the original body from a (projectID, id, vars)
// triple previously returned by Extract on this or any prior process
// over the same Store.
//
// The three return states are distinct: ("", false, nil) means the
// template id is genuinely absent (never minted, or a corrupt/mismatched
// vars count); ("", false, err) means the lazy load of the project's
// templates from the store failed — a transient store error, NOT
// "missing" — and callers must not treat it as such; (body, true, nil)
// is success.
func (e *Extractor) Reconstruct(projectID, id int64, vars []string) (string, bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p, ok := e.projects[projectID]
	if !ok {
		// Not loaded this process; load lazily with a background ctx —
		// reads must not depend on the caller having extracted first.
		var err error
		p, err = e.load(context.Background(), projectID)
		if err != nil {
			return "", false, err
		}
	}
	t, ok := p.byID[id]
	if !ok {
		return "", false, nil
	}
	vi := 0
	for _, tok := range t.tokens {
		if tok == Wild {
			vi++
		}
	}
	if vi != len(vars) {
		return "", false, nil
	}
	return substitute(t.tokens, vars), true, nil
}

// Count reports the in-memory template count for a project (0 when the
// project has not been touched this process).
func (e *Extractor) Count(projectID int64) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	if p, ok := e.projects[projectID]; ok {
		return len(p.byID)
	}
	return 0
}

// load returns the project's in-memory state, loading persisted
// templates on first touch. Caller holds e.mu.
func (e *Extractor) load(ctx context.Context, projectID int64) (*project, error) {
	if p, ok := e.projects[projectID]; ok {
		return p, nil
	}
	rows, err := e.store.LoadTemplates(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("template: load project %d: %w", projectID, err)
	}
	p := &project{groups: map[string][]*tmpl{}, byID: map[int64]*tmpl{}}
	for _, r := range rows {
		t := &tmpl{id: r.ID, tokens: strings.Split(r.Text, " ")}
		k := groupKey(t.tokens)
		p.groups[k] = append(p.groups[k], t)
		p.byID[t.id] = t
	}
	e.projects[projectID] = p
	return p, nil
}

func groupKey(tokens []string) string {
	first := tokens[0]
	if strings.ContainsAny(first, "0123456789") {
		first = Wild // digit-bearing first tokens (timestamps, IPs) group together
	}
	return fmt.Sprintf("%d|%s", len(tokens), first)
}

// resolve picks the template for tokens: (existing, nil) when one
// already covers the line, (nil, tokensToMint) when a new template —
// exact or generalized under a NEW id (append-only) — is needed.
func resolve(candidates []*tmpl, tokens []string, simThresh float64) (*tmpl, []string) {
	var best *tmpl
	bestSim := 0.0
	for _, t := range candidates {
		if s := similarity(t.tokens, tokens); s > bestSim {
			best, bestSim = t, s
		}
	}
	if best == nil || bestSim < simThresh {
		return nil, append([]string(nil), tokens...)
	}
	if covers(best.tokens, tokens) {
		return best, nil
	}
	merged := append([]string(nil), best.tokens...)
	for i, tok := range tokens {
		if merged[i] != Wild && merged[i] != tok {
			merged[i] = Wild
		}
	}
	return nil, merged
}

// similarity is the fraction of positions where the template matches
// exactly or is already a wildcard. Callers guarantee equal lengths
// (groupKey buckets by token count).
func similarity(ttoks, tokens []string) float64 {
	same := 0
	for i, tok := range tokens {
		if ttoks[i] == Wild || ttoks[i] == tok {
			same++
		}
	}
	return float64(same) / float64(len(tokens))
}

func covers(ttoks, tokens []string) bool {
	for i, tok := range tokens {
		if ttoks[i] != Wild && ttoks[i] != tok {
			return false
		}
	}
	return true
}

func varsFor(ttoks, tokens []string) []string {
	var vars []string
	for i, tok := range ttoks {
		if tok == Wild {
			vars = append(vars, tokens[i])
		}
	}
	return vars
}

func substitute(ttoks, vars []string) string {
	out := make([]string, len(ttoks))
	vi := 0
	for i, tok := range ttoks {
		if tok == Wild {
			if vi >= len(vars) {
				return ""
			}
			out[i] = vars[vi]
			vi++
		} else {
			out[i] = tok
		}
	}
	return strings.Join(out, " ")
}
