CREATE TABLE templates (
  id INTEGER PRIMARY KEY, project_id INTEGER NOT NULL REFERENCES projects(id),
  text BLOB NOT NULL, created_at TEXT NOT NULL);
CREATE INDEX templates_project ON templates(project_id, id);
CREATE TABLE segment_manifest (
  id INTEGER PRIMARY KEY, project_id INTEGER NOT NULL REFERENCES projects(id),
  path TEXT NOT NULL UNIQUE,
  min_ts INTEGER NOT NULL, max_ts INTEGER NOT NULL,
  min_log_id INTEGER NOT NULL, max_log_id INTEGER NOT NULL,
  count INTEGER NOT NULL, events INTEGER NOT NULL,
  services TEXT NOT NULL DEFAULT '[]', created_at TEXT NOT NULL);
CREATE INDEX segment_manifest_project ON segment_manifest(project_id, min_ts);
CREATE TABLE log_rollups (
  project_id INTEGER NOT NULL, service TEXT NOT NULL,
  severity INTEGER NOT NULL, hour TEXT NOT NULL,
  logs INTEGER NOT NULL DEFAULT 0, events INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (project_id, service, severity, hour));
CREATE TABLE issue_events (
  id INTEGER PRIMARY KEY, issue_id INTEGER NOT NULL REFERENCES issues(id),
  log_id INTEGER NOT NULL, project_id INTEGER NOT NULL,
  environment TEXT NOT NULL DEFAULT '', ts TEXT NOT NULL);
CREATE INDEX issue_events_issue ON issue_events(issue_id, ts DESC);
CREATE INDEX issue_events_project_ts ON issue_events(project_id, ts);
