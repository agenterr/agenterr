# Head-to-Head: agenterr engine vs OpenObserve (2026-08-16)

**Setup:** identical corpus — 317,229 real production logs (one full trial
day, ANSI-normalized identically on both sides). Same machine (macOS,
arm64). agenterr: the real storage engine (branch `feat/query-layer`,
ingest → WAL fsync → flush → compaction), measured through in-process
store calls. OpenObserve: `public.ecr.aws/zinclabs/openobserve:latest`
in Docker, measured through localhost HTTP with `use_cache=false`
(its result cache otherwise flatters repeated queries ~10×; localhost
HTTP overhead is <1 ms and negligible). Harness: `cmd/benchvso2`.

## Storage

| | agenterr | OpenObserve |
|---|---|---|
| Log data at rest | 2.77 MB (1 compacted segment) | 3.44 MB (24 parquet files) |
| Metadata | 0.20 MB | 5.3 MB `db/` (mostly fixed baseline) |
| All-in bytes/record | **9.4** | 10.9 marginal (≈28 counting db/) |

**Honest finding:** the production trial's o2 figure (106 B/rec) does NOT
reproduce on clean identical data — that number reflected the box's o2
holding 23 heterogeneous streams with raw (ANSI-laden) bodies on an older
version. Apples-to-apples, o2's columnar compression is excellent and
agenterr's edge is real but modest: **~1.2× smaller marginal, ~3× smaller
all-in at this scale.** The fair public claim is "matches or beats
OpenObserve on storage," not "11× smaller."

## Speed (p50 of repeated runs; o2 uncached)

| Query (identical results both sides) | agenterr | OpenObserve | winner |
|---|---|---|---|
| Q1 scoped substring search (service + 24 h, 16 hits) | 207 ms | ~38 ms | o2 5.5× |
| Q2 unscoped substring search (worst case, 0 hits) | 383 ms | ~35 ms | o2 11× |
| Q3 aggregate by service (10 groups) | **0.16 ms** | ~22 ms | **agenterr 137×** |
| Ingest throughput | 70k logs/s (fsync-before-ack) | 213k logs/s (async ack) | different guarantees* |

\* o2 acks before durability and **silently discarded whole batches** during
setup (data older than its 5 h default window returned HTTP 200 with a
per-record error body). agenterr's ack means fsync'd.

## Verdict vs spec §7 targets

| Target | Result |
|---|---|
| Storage ≤ 100 B/rec | **PASS** (9.4) |
| Aggregate ≤ 5 ms | **PASS** (0.16 ms, beats o2's 46 ms trial / 22 ms local) |
| Scoped search ≤ 50 ms | **FAIL** (207 ms) |
| Unscoped ≤ 2 s | PASS at 1 day (383 ms) but would fail at 30 d (~11 s extrapolated) |

**Root cause of the search gap:** `segment.Read` decodes ALL 17 columns of
the whole segment before any filter runs; a compacted day = full-day decode
per query. o2 reads only the columns the query touches. The fix is
**column-selective segment reads** (decode ts/service/severity + template/
vars/raw only for rows surviving the cheap filters) — the segment format
already stores per-column offsets in the footer, so this is a reader
change, not a format change. Expected ~5–10×, which would put scoped
search at ~20–40 ms — competitive with o2 — and unscoped 30 d within the
2 s target.

## Standing conclusions

- **Aggregation:** agenterr wins outright (rollups vs columnar scan) — the
  trial's "aggregation is o2-only" shortcoming is now inverted.
- **Storage:** parity-to-better on clean data; the massive win vs the
  trial's 481 B/rec legacy agenterr (51×) stands.
- **Search latency:** o2's query engine currently wins; sub-second is fine
  for agent use, but the §7 scoped-search gate FAILS until
  column-selective reads land. That is the bench-suite plan's top item.
- **Durability:** agenterr's ingest ack is strictly stronger.
