-- 0005_noise_rules.sql
-- Per-project ingest filtering rules. Typed columns instead of a params
-- blob: three fixed kinds, and dropped_count needs atomic increments.
CREATE TABLE noise_rules (
    id            INTEGER PRIMARY KEY,
    project_id    INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    kind          TEXT    NOT NULL CHECK (kind IN ('severity_floor','drop_match','sample')),
    service       TEXT    NOT NULL DEFAULT '',
    severity      TEXT    NOT NULL DEFAULT '',
    pattern       TEXT    NOT NULL DEFAULT '',
    n             INTEGER NOT NULL DEFAULT 0,
    enabled       INTEGER NOT NULL DEFAULT 1,
    dropped_count INTEGER NOT NULL DEFAULT 0,
    created_at    TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
);
CREATE INDEX idx_noise_rules_project ON noise_rules(project_id);

ALTER TABLE projects ADD COLUMN parse_bodies INTEGER NOT NULL DEFAULT 1;
