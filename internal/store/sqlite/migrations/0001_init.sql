CREATE TABLE projects (
  id INTEGER PRIMARY KEY, name TEXT NOT NULL, slug TEXT NOT NULL UNIQUE,
  retention_days INTEGER NOT NULL DEFAULT 14, created_at TEXT NOT NULL);
CREATE TABLE keys (
  id INTEGER PRIMARY KEY, project_id INTEGER NOT NULL REFERENCES projects(id),
  kind TEXT NOT NULL CHECK (kind IN ('ingest','api')),
  hash BLOB NOT NULL, prefix TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE INDEX keys_prefix ON keys(prefix);
CREATE TABLE issues (
  id INTEGER PRIMARY KEY, project_id INTEGER NOT NULL REFERENCES projects(id),
  fingerprint TEXT NOT NULL, title TEXT NOT NULL, severity INTEGER NOT NULL,
  status TEXT NOT NULL DEFAULT 'open', first_seen TEXT NOT NULL,
  last_seen TEXT NOT NULL, count INTEGER NOT NULL DEFAULT 0,
  UNIQUE (project_id, fingerprint));
CREATE INDEX issues_last_seen ON issues(project_id, last_seen DESC);
CREATE TABLE logs (
  id INTEGER PRIMARY KEY, project_id INTEGER NOT NULL REFERENCES projects(id),
  ts TEXT NOT NULL, severity INTEGER NOT NULL, body TEXT NOT NULL,
  service TEXT NOT NULL DEFAULT '', environment TEXT NOT NULL DEFAULT '',
  release TEXT NOT NULL DEFAULT '', trace_id TEXT NOT NULL DEFAULT '',
  attrs TEXT NOT NULL DEFAULT '{}', issue_id INTEGER REFERENCES issues(id));
CREATE INDEX logs_ts ON logs(project_id, ts);
CREATE TABLE events (
  id INTEGER PRIMARY KEY, issue_id INTEGER NOT NULL REFERENCES issues(id),
  log_id INTEGER NOT NULL REFERENCES logs(id), ts TEXT NOT NULL);
CREATE INDEX events_issue ON events(issue_id, ts DESC);
