CREATE VIRTUAL TABLE logs_fts USING fts5(body, content=logs, content_rowid=id);
CREATE TRIGGER logs_ai AFTER INSERT ON logs BEGIN
  INSERT INTO logs_fts(rowid, body) VALUES (new.id, new.body); END;
CREATE TRIGGER logs_ad AFTER DELETE ON logs BEGIN
  INSERT INTO logs_fts(logs_fts, rowid, body) VALUES ('delete', old.id, old.body); END;
