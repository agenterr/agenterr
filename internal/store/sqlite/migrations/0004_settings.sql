-- Small internal key/value table for process bootstrap state (e.g. the
-- admin password hash) that isn't domain data and doesn't belong on the
-- store.Admin interface.
CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
