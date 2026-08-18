CREATE TABLE segment_manifest_new (
  id INTEGER PRIMARY KEY AUTOINCREMENT, project_id INTEGER NOT NULL REFERENCES projects(id),
  path TEXT NOT NULL UNIQUE,
  min_ts INTEGER NOT NULL, max_ts INTEGER NOT NULL,
  min_log_id INTEGER NOT NULL, max_log_id INTEGER NOT NULL,
  count INTEGER NOT NULL, events INTEGER NOT NULL,
  services TEXT NOT NULL DEFAULT '[]', created_at TEXT NOT NULL,
  raw_rows INTEGER NOT NULL DEFAULT 0, size_bytes INTEGER NOT NULL DEFAULT 0);
INSERT INTO segment_manifest_new SELECT id, project_id, path, min_ts, max_ts, min_log_id, max_log_id, count, events, services, created_at, raw_rows, size_bytes FROM segment_manifest;
DROP TABLE segment_manifest;
ALTER TABLE segment_manifest_new RENAME TO segment_manifest;
CREATE INDEX segment_manifest_project ON segment_manifest(project_id, min_ts);
