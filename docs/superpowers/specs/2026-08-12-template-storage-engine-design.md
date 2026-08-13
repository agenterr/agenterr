# Template Storage Engine — Design Spec

**Date:** 2026-08-12
**Status:** Approved design, pre-implementation
**Target release:** v0.2.0

## Problem

The the-trial-customer production trial (see the trial runbook in the infra
repo) measured three defects against OpenObserve running on identical data:

1. **Search blind spot.** FTS5's unicode61 tokenizer fuses ANSI escape
   sequences onto the following word (`\x1b[35;1mrecord` → token
   `1mrecord`), so `search_logs("record")` returns 0 against 166 real
   matches. An agent-first tool returning a false 0 is the worst failure
   mode we have.
2. **Plain-text severity blind spot.** Bodies with no structured level
   (GORM, traefik) ingest as INFO and never create issues — 166
   `record not found` lines/24h invisible. Grew 18× during the trial.
3. **Storage and search performance.** ~481 B/record all-in
   (uncompressed row-store SQLite + FTS index) vs OpenObserve's
   ~106 B/record (columnar Parquet + zstd). agenterr's data volume
   crossed over o2's in absolute terms while holding 3.7× fewer records.
   One unscoped multi-word search timed out and pegged the container at
   its 0.5-core cap for minutes; o2 answers equivalent aggregates in
   ~46 ms.

**Success criterion (user-set): match or beat OpenObserve on storage
bytes/record and on lookup speed.**

## Core idea

agenterr already computes what a generic log store never has: the
*template* of every line (fingerprinting is the product). Store that
knowledge instead of re-deriving it:

- A log is stored as `(template_id, variables[], ts)` — the template
  text is stored once, ever.
- Search runs **template-first**: match the query against a few thousand
  template strings in microseconds, then decode only segments containing
  matching templates.
- This is the CLP (Compressed Log Processing) result: 2–4× better than
  zstd on raw text, searchable without full decompression. Expected
  ~20–50 B/record all-in vs o2's 106.

SQLite stops being the log store and becomes the **metadata store**
(projects, keys, issues, events, triage, noise/alert/severity rules,
settings, templates, rollups, segment manifest). Log data lives in an
append-only **segment engine**. The `logs` table and FTS5 are deleted.

## Non-goals

- **No migrator.** v0.2.0 starts with a fresh data store. The trial
  deployment keeps its old DB as a cold backup (the runbook's upgrade
  procedure already takes one); historical trial data is not carried
  forward. Issues/triage state also start fresh.
- No change to ingest wire formats, `agenterr-ship`, or the Vector
  contract. Everything is behind the API.
- No distributed/object-store support. Single node, local disk, as today.
- Fingerprinting (issue grouping) is **not** replaced by template
  extraction in this release. They are separate dimensions; unifying
  them is future work.
- Compression of the metadata SQLite DB (it is small).

## Architecture

```
edges (HTTP JSON / OTLP)
   → normalize        (ANSI strip, severity lift, severity rules)
   → template extract (Drain-style tree, lossless, raw fallback)
   → memtable + WAL   (acked after WAL append; fsync batched ~100 ms)
        │ flush at ~64k logs or ~5 min
        ▼
   segment files   data/segments/<project>/<ts>-<seq>.seg   (immutable)
   SQLite          issues, events, templates, rollups, manifest, rules, keys
```

### Component boundaries

| Unit | Does | Depends on |
|---|---|---|
| `internal/normalize` | ANSI stripping, severity lifting chain | core |
| `internal/template` | lossless extract/reconstruct, template tree | core |
| `internal/engine` (new store impl) | memtable, WAL, segment read/write, compaction/retention | template, store interfaces |
| `internal/store/sqlite` | metadata only (shrinks) | — |
| `internal/pipeline` | orchestration (largely survives) | engine, rules |
| query layer (api/mcp/web) | template-first search, context, rollups | engine, sqlite |

The existing `store` interface layering survives; the engine is a new
implementation behind it plus a widened search/aggregate surface.

## 1. Normalize stage

Runs on every log before anything else sees the body.

- **ANSI strip:** remove CSI/SGR escape sequences from the body. Record
  a boolean hint if red/bright-red (SGR 31/91) was present. The stripped
  body is *the* body from here on — escape codes are intentionally not
  preserved (the one deliberate loss in the system).
- **Severity lift**, first match wins:
  1. explicit severity from the wire
  2. structured-body lift (existing JSON/logfmt parsing)
  3. `panic:` / `fatal error:` prefix → FATAL (existing, now operating
     on clean bodies)
  4. **per-project severity rules**: user-defined `regex → severity`,
     same storage/CRUD/MCP pattern as noise rules. This is the
     user-controllable answer to the GORM gap
     (`record not found → ERROR`).
  5. red-ANSI heuristic → ERROR. **Off by default**, per-project toggle.

## 2. Template extraction

Drain-style online template tree. Contract:

- `Extract(body) → (templateID, vars []string)` and
  `Reconstruct(templateID, vars) → body` with the invariant
  `Reconstruct(Extract(b)) == b` **byte-for-byte, verified at ingest per
  line**. Any line failing the check — or not templating (multi-line
  panics, one-off blobs) — is stored under the reserved
  `templateID = 0` (raw): body kept verbatim in the segment's raw
  column, zstd'd. Correctness never depends on templating; only
  efficiency does.
- Templates are append-only rows in SQLite (`id, project_id, text,
  first_seen`) and cached in memory. Expected cardinality: thousands.
- **Guardrail:** per-project template cap (default 100k). At the cap,
  new shapes go to raw; a counter surfaces it in stats. Prevents
  hostile/high-entropy services from exploding the table.

## 3. Segment format

One segment = one project, one contiguous time span. Immutable once
written. Columnar, zstd per column block:

| Column | Encoding |
|---|---|
| log_id | delta varint (globally monotonic int64, assigned at ingest — `events.log_id` pointers stay valid) |
| ts | delta varint, epoch micros |
| template_id | dictionary + RLE |
| severity, service, environment | dictionary + RLE |
| variables | per-row length-prefixed concat in row order, zstd'd (regrouping by template inside the block is a possible later optimization, not part of this design) |
| attrs | interned per segment: unique attr-sets stored once, rows store a reference |
| raw bodies | separate zstd block for template_id=0 rows |

**Footer (the query planner's whole world):** min/max ts, min/max
log_id, set of template_ids present, service set, per-(severity,
service) counts, column offsets. CRC on every block.

**Manifest:** one SQLite row per segment (path, ranges, counts, sizes)
so startup never scans the directory. Manifest and file are reconciled
at startup; orphan files are quarantined, not deleted.

**Retention:** delete segment files whose max ts < cutoff, drop their
manifest rows. No b-tree churn. Hourly rollups (see §5) outlive their
segments intentionally — trend history stays after bodies expire.
Templates are never pruned in this release (they are tiny).

## 4. Memtable + WAL

- Logs are acked (HTTP 202) after WAL append. fsync is batched
  (default 100 ms, configurable). Crash loses at most the fsync window
  — same guarantee class as o2.
- Memtable holds decoded recent logs (at trial volume ~5 MB); flush to
  segment at ~64k logs or ~5 min, whichever first. WAL truncates after
  a successful flush + manifest commit.
- Recovery: replay WAL into memtable at startup. Replay is idempotent
  (log_ids are in the WAL records).

## 5. Query paths

- **`search_logs` (template-first, substring semantics):**
  1. *Plan:* substring-match the query against rendered template texts
     in memory (µs). Matching template_ids + time/service filters select
     candidate segments via footers.
  2. *Execute:* decode candidate segments' needed columns and
     substring-match. Because a query can also match **variable
     content**, phase 2 always scans the variables (and raw) columns of
     time/service-pruned segments even when no template matched.
  - No tokenizer exists anywhere. The `record`→0 bug class is
    structurally gone. Worst case (unscoped, 30 d, no template hit) is
    a linear decode of ~300 MB compressed ≈ ~1 s, vs today's
    multi-minute timeout at the CPU cap.
- **`get_log_context`:** memtable if recent; else footer ts-lookup →
  decode one segment window. **Log-by-id:** manifest log_id ranges.
- **Issues/triage:** SQLite, unchanged paths.
- **Aggregations (new):** hourly rollups
  `(project, service, severity, hour) → count`, upserted at flush time.
  New MCP tool `aggregate_logs` (group by service/severity/hour over a
  window). Reads thousands of rollup rows: sub-ms, beating o2's 46 ms.
  Closes the "aggregation is o2-only" trial shortcoming.

## 6. MCP / API surface changes

- `search_logs`: same signature, new semantics documented as substring.
  Remove the prefix-token caveat from docs when it lands.
- New: `aggregate_logs`, severity-rule CRUD (mirror noise-rule tools).
- `get_stats` gains: templating rate, raw fallback %, bytes/record,
  segment counts. (These numbers are also the README pitch.)

## 7. Performance targets (acceptance)

Measured on a replayed day of real trial traffic (~330k logs):

| Metric | Target | Baseline |
|---|---|---|
| Storage, all-in | ≤ 100 B/rec (goal ~20–50) | agenterr 481, o2 106 |
| Scoped search (service + 24 h) | ≤ 50 ms | FTS: variable, CPU-capped |
| Unscoped 30 d search, worst case | ≤ 2 s | timeout (minutes) |
| Aggregate by service, 30 d | ≤ 5 ms | o2 46 ms; agenterr impossible |
| Ingest throughput | ≥ current (no regression at 500-log batches) | — |
| Templating rate on trial corpus | ≥ 90% of lines (traefik must template) | — |

### Speed-test suite (proving the o2 claim)

Two layers, both replaying the same fixed corpus (a captured/generated
day of trial-shaped traffic, ~330k logs, committed as a generator not
raw data):

1. **CI benchmark gate (`test/bench`, runs on every PR):** Go benchmarks
   for the canonical operations — scoped search (service + 24 h),
   worst-case unscoped 30 d search, `aggregate_logs`, `get_log_context`,
   ingest throughput, and measured bytes/record after flush. Each
   asserts against the §7 targets as hard thresholds; a PR that pushes
   scoped search over 50 ms fails. Targets, not o2, are the CI baseline
   — o2 doesn't run in CI.
2. **Head-to-head harness (`make bench-vs-o2`, local/manual):**
   docker-composes a real OpenObserve, feeds both stores the identical
   corpus, runs the equivalent query on each side (agenterr MCP/API vs
   o2 `_search` SQL), and prints a side-by-side latency + on-disk-size
   table. This is the artifact behind any public "beats OpenObserve"
   claim, and re-runnable against future o2 versions. Run before
   tagging v0.2.0; the output table goes in the release notes.

## 8. Validation & testing

- **Step 0 — prototype before the build.** Throwaway Drain extractor
  over one day of real trial logs; report template count, templating
  rate, measured B/rec. Proves the make-or-break assumption (traefik's
  71% of volume templating well) in ~a day. If templating rate < 90%
  or B/rec > 100, stop and redesign before writing the engine.
- Round-trip property/fuzz tests: `Reconstruct(Extract(b)) == b` over
  fuzz corpus + real trial corpus.
- Crash-recovery: kill −9 mid-write under load; no acked log lost,
  no manifest/file divergence.
- Query oracle: every search result equals a naive
  decompress-everything-and-scan reference on the same data.
- `storetest` suite runs against the new engine.
- ANSI/severity: table tests over the trial's real GORM/traefik lines.

## 9. Risks & mitigations

| Risk | Mitigation |
|---|---|
| Templating rate too low on real traffic | Step 0 prototype gates the build |
| Template table explosion | per-project cap → raw fallback + stat |
| Variable-column scans dominate search | footers prune by time/service; measure in step 0 |
| WAL/flush bugs lose data | crash-recovery test in CI; acked-after-WAL contract |
| Memtable RAM at higher volumes | flush thresholds are size-based; 64k logs ≈ tens of MB max |
| Scope: this is a big build | phased plan (writing-plans); engine lands behind the existing `store` interface with the suite green at each phase |

## Decisions log

- Shape 2 (template-native end to end) over hybrid hot/cold or
  in-SQLite templates — user chose; lookup speed is the priority.
- No migrator — user chose; fresh store at v0.2.0.
- ANSI codes are stripped, not preserved (only deliberate data loss).
- Red-ANSI severity heuristic ships off by default.
- Rollups outlive segment retention.
- Fingerprinting and templates stay separate dimensions in v0.2.0.
