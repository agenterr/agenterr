.PHONY: test lint bench-gates bench-vs-o2

test:
	go test ./... -race

lint:
	$$(go env GOPATH)/bin/golangci-lint run

# bench-gates runs the spec §7 performance gates against a synthetic
# corpus (see internal/store/enginestore/gates_test.go). It's a local/
# manual command, not part of CI — latency gates on shared runners are
# flaky noise.
bench-gates:
	go test -tags benchgates -run TestSpecGates -count=1 -v ./internal/store/enginestore/

# bench-vs-o2 is the real-corpus, real-OpenObserve head-to-head behind
# the spec §7 numbers. It needs docker (for OpenObserve) and the
# confidential production-shaped corpus, which is local-only and never
# committed, so it can't be scripted here. See cmd/benchvso2's doc
# comment for the harness's modes and flags, and
# docs/superpowers/specs/2026-08-16-bench-vs-o2-report.md for the
# methodology and last-measured numbers.
bench-vs-o2:
	@echo "bench-vs-o2 is manual: needs docker + the confidential local corpus."
	@echo "See cmd/benchvso2's doc comment (go doc ./cmd/benchvso2) for usage,"
	@echo "and docs/superpowers/specs/2026-08-16-bench-vs-o2-report.md for the methodology."
