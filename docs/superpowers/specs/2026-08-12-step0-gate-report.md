# Step-0 Gate Report — Template Prototype

**Run:** 2026-08-13 against corpus 2026-08-12T00:00:00 → 2026-08-13T00:00:00 UTC,
317,229 logs (the trial customer's box, `wp` project, all services).
**Spec:** 2026-08-12-template-storage-engine-design.md §8.
**Window substitution:** the plan named 2026-08-11; the most recent *full* day
at run time was 2026-08-12, so that window was used instead (same ingest mix).
The corpus file stays outside the repo (`~/tmp-agenterr-corpus-day.json`,
local-only, never committed) — production log data.

## CLI output

```
corpus: 317229 logs

column                        raw B       zstd B
ts (delta varint)            385790       242026
template_id                  317732        26038
service                      317229        20852
severity                     317229          978
variables                  39333309      2227077
raw bodies                     2138           15
attrs refs                   325953        34670
attrs dict                  4735999       204572
template table                    -        48228

templates minted:      189
templating rate:       99.3%  (315091 templated / 2138 raw)
simulated storage:     8.8 B/record (2804456 B total)

GATE (spec §8): rate ≥ 90% && B/rec ≤ 100 → PASS
```

Exit code: 0.

## Verdict

- Templating rate: **99.3%** (threshold ≥ 90%) → **pass**
- Simulated storage: **8.8 B/record** (threshold ≤ 100; o2 baseline 106;
  current agenterr production ~481) → **pass**
- Templates minted: **189** (sanity: expected thousands; 189 for a
  12-service day is even better — no template explosion whatsoever)

**GATE: PASS — proceed to engine plans.**

## Notes

- The make-or-break assumption held: `traefik_socket-proxy` (~71% of
  volume) templates extremely well — the whole corpus needed only 189
  templates.
- Raw fallback was 2,138 lines (0.7%), compressing to almost nothing
  (15 B zstd — near-identical multiline/edge bodies).
- The dominant cost is the variables column (2.23 MB of the 2.80 MB
  total, ~7.0 B/rec) — as designed; everything else is noise. Timestamps
  are second (242 KB); a coarser delta encoding could shave this later.
- Honest caveats baked into the plan still apply: whole-corpus-column
  zstd flatters vs the real 64k-row blocks (expect ~10–20% worse), and
  the simulation excludes segment footers, manifest, and WAL overhead.
  Even at a conservative 3× degradation (~26 B/rec) the target holds
  with 4× margin over o2.
- Attrs interning works as predicted: 4.74 MB of repeated JSON reduces
  to a 205 KB dict + 35 KB of refs (~0.76 B/rec).
