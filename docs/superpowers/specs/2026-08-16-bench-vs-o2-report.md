# Head-to-Head: agenterr engine vs OpenObserve (2026-08-16, updated 2026-08-20)

**Setup:** identical corpus — 317,229 real production logs (one full trial
day, ANSI-normalized identically on both sides). Same machine (macOS,
arm64). agenterr: the real storage engine (fast-reader branch: column-
selective scans, template-classified zero-alloc substring matching,
parallel sharded segments), measured through in-process store calls.
OpenObserve: `public.ecr.aws/zinclabs/openobserve:latest` in Docker,
measured through localhost HTTP with `use_cache=false` — now enforced in
the harness itself, not by hand; its result cache otherwise flatters
repeated queries ~4-10× (we re-verified: cached Q1 reads 9.8 ms vs the
honest 37 ms). Localhost HTTP overhead is <1 ms and negligible.
Harness: `cmd/benchvso2`.

## Storage

| | agenterr | OpenObserve |
|---|---|---|
| Log data at rest | 2.78 MB (4 shard segments) | 3.44 MB (24 parquet files) |
| Metadata | 0.20 MB | 5.3 MB `db/` (mostly fixed baseline) |
| All-in bytes/record | **9.4** | 10.9 marginal (≈28 counting db/) |

Sharding the compacted day into 4 segments (100k-row cap) cost +0.3%
storage vs the single merged segment — noise.

**Honest finding (unchanged from 2026-08-16):** the production trial's o2
figure (106 B/rec) does NOT reproduce on clean identical data — that
number reflected the box's o2 holding 23 heterogeneous streams with raw
(ANSI-laden) bodies on an older version. Apples-to-apples, o2's columnar
compression is excellent and agenterr's edge is real but modest: **~1.2×
smaller marginal, ~3× smaller all-in at this scale.** The fair public
claim is "matches or beats OpenObserve on storage," not "11× smaller."

## Speed (p50 of 20 runs; o2 uncached; identical results both sides)

| Query | agenterr | OpenObserve | winner |
|---|---|---|---|
| Q1 scoped substring search (service + 24 h, 16 hits) | **10.9 ms** | 37.0 ms | **agenterr 3.4×** |
| Q2 unscoped substring search (worst case, 0 hits) | **14.8 ms** | 34.4 ms | **agenterr 2.3×** |
| Q3 aggregate by service (10 groups) | **0.17 ms** | 20.3 ms | **agenterr 119×** |
| Ingest throughput | 79k logs/s (fsync-before-ack) | 213k logs/s (async ack) | different guarantees* |

\* o2 acks before durability and **silently discarded whole batches**
during setup (data older than its 5 h default window returned HTTP 200
with a per-record error body). agenterr's ack means fsync'd.

### History of the search gap (why it closed)

| Stage | Q1 scoped | Q2 unscoped |
|---|---|---|
| 2026-08-16 baseline (all-columns full decode) | 207 ms | 383 ms |
| + column-selective scans, zero-alloc template-classified matching, parallel chunking | 39.7 ms | 36.9 ms |
| + sharded compaction (parallel scans divide the decompress floor) | **10.9 ms** | **14.8 ms** |

The 2026-08-16 root-cause analysis held exactly: `segment.Read` decoded
all 17 columns of the whole segment before any filter ran. The fixes, in
order of impact: (1) decode only the cheap filter columns (ts, severity,
service refs) and materialize rows only for survivors; (2) never allocate
per-row strings during matching — reconstruct into a reusable buffer via
the template token lists and scan with `bytes.Contains`, with templates
whose static text contains the query matching with no byte scan at all;
(3) scan segments and 32k-row chunks in parallel (bounded, GOMAXPROCS);
(4) cap compacted segments at 100k rows so a day is 3-4 shards and the
one remaining serial cost — zstd-decompressing the vars column — divides
across cores too.

## Verdict vs spec §7 targets

| Target | Result |
|---|---|
| Storage ≤ 100 B/rec | **PASS** (9.4) |
| Aggregate ≤ 5 ms | **PASS** (0.17 ms) |
| Scoped search ≤ 50 ms | **PASS** (10.9 ms) |
| Unscoped ≤ 2 s at 30 d | **PASS** (14.8 ms/day; ~0.4 s extrapolated linearly, and shards scan in parallel across days) |

## Standing conclusions

- **agenterr now beats OpenObserve on every measured query metric** on
  identical clean data: 3.4× on scoped search, 2.3× on unscoped search,
  119× on aggregation — while remaining smaller at rest and durably
  acking every write.
- **Ingest throughput** is the one number where o2's figure is larger,
  and it is not comparable: o2 acks before fsync (and demonstrably
  drops data silently); agenterr's 79k logs/s is fsync-before-ack. A
  relaxed-ack mode could close the raw number if ever needed.
- **Cache honesty matters:** o2's result cache turns repeated identical
  queries into ~9 ms responses. Real investigative queries (new query
  each time) pay the uncached cost. The harness now pins
  `use_cache=false` so this can't silently skew future runs.

## Running the §7 gates

`make bench-gates` runs the spec §7 targets above as an automated,
repeatable test (`internal/store/enginestore/gates_test.go`, tag
`benchgates`) against a synthetic corpus that mirrors the real corpus's
shape — so the fast-reader win can't silently regress without a local
docker+corpus setup. It's local/manual only, not wired into CI. The
real-corpus, real-OpenObserve head-to-head above is `cmd/benchvso2`,
run by hand against the confidential local corpus.
