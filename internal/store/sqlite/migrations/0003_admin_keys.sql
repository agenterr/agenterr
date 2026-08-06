-- Adds the instance-level "admin" key kind and makes project_id nullable
-- for it (an admin key is not scoped to any single project). SQLite can't
-- ALTER a CHECK constraint or drop NOT NULL in place, so this rebuilds the
-- table: create the new shape, copy rows, drop the old table, rename.
CREATE TABLE keys_new (
  id INTEGER PRIMARY KEY, project_id INTEGER REFERENCES projects(id),
  kind TEXT NOT NULL CHECK (kind IN ('ingest','api','admin')),
  hash BLOB NOT NULL, prefix TEXT NOT NULL, created_at TEXT NOT NULL);
INSERT INTO keys_new (id, project_id, kind, hash, prefix, created_at)
  SELECT id, project_id, kind, hash, prefix, created_at FROM keys;
DROP TABLE keys;
ALTER TABLE keys_new RENAME TO keys;
CREATE INDEX keys_prefix ON keys(prefix);
