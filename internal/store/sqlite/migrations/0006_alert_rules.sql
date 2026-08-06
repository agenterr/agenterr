-- 0006_alert_rules.sql
-- Per-project alert rules: fire a webhook when a new issue, a regression, or
-- a threshold condition is matched. Typed columns mirroring noise_rules'
-- shape; headers is a JSON TEXT column (object of string->string).
CREATE TABLE alert_rules (
    id               INTEGER PRIMARY KEY,
    project_id       INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name             TEXT    NOT NULL DEFAULT '',
    kind             TEXT    NOT NULL CHECK (kind IN ('new_issue','regression','threshold')),
    service          TEXT    NOT NULL DEFAULT '',
    environment      TEXT    NOT NULL DEFAULT '',
    min_severity     TEXT    NOT NULL DEFAULT '',
    n                INTEGER NOT NULL DEFAULT 0,
    window_minutes   INTEGER NOT NULL DEFAULT 0,
    cooldown_seconds INTEGER NOT NULL DEFAULT 0,
    url              TEXT    NOT NULL DEFAULT '',
    headers          TEXT    NOT NULL DEFAULT '{}',
    enabled          INTEGER NOT NULL DEFAULT 1,
    last_fired       TEXT,
    last_error       TEXT    NOT NULL DEFAULT '',
    created_at       TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
);
CREATE INDEX idx_alert_rules_project ON alert_rules(project_id);
