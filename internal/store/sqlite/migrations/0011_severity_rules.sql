-- 0011_severity_rules.sql
-- Per-project severity-lift rules (spec §1 lift-chain slot 4): a Go
-- regexp over the log body that lifts still-default-severity logs to
-- the rule's severity. Mirrors noise_rules' shape; lifted_count needs
-- atomic increments like dropped_count.
CREATE TABLE severity_rules (
    id           INTEGER PRIMARY KEY,
    project_id   INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    service      TEXT    NOT NULL DEFAULT '',
    pattern      TEXT    NOT NULL,
    severity     TEXT    NOT NULL,
    enabled      INTEGER NOT NULL DEFAULT 1,
    lifted_count INTEGER NOT NULL DEFAULT 0,
    created_at   TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
);
CREATE INDEX idx_severity_rules_project ON severity_rules(project_id);
