# init_stechen_sqlite.py
#
# Initializes the SQLite schema using:
#   - pipeline            (one executed step)
#   - one table per script (payload tables)
#   - runtime logs
#   - checkpoints         (semantic summaries every N steps)
#
# Usage:
#   python init_stechen_sqlite.py
#   python init_stechen_sqlite.py stechen.db
#
# NOTE:
# - RunClass no longer stores classpath.
# - Classpath is auto-derived from the working directory at runtime.

import sys
import sqlite3
from pathlib import Path
from datetime import datetime

DEFAULT_DB = "stechen.db"


DDL = """
PRAGMA foreign_keys = ON;

-- =========================================================
-- CORE: ONE ROW = ONE PIPELINE STEP
-- =========================================================
CREATE TABLE IF NOT EXISTS pipeline (
  step_id     INTEGER PRIMARY KEY AUTOINCREMENT,
  script_name TEXT NOT NULL,
  created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
  run_id      TEXT,
  step_index  INTEGER
);

CREATE INDEX IF NOT EXISTS idx_pipeline_script_name ON pipeline(script_name);
CREATE INDEX IF NOT EXISTS idx_pipeline_created_at  ON pipeline(created_at);
CREATE INDEX IF NOT EXISTS idx_pipeline_run_id      ON pipeline(run_id);

-- =========================================================
-- SCRIPT TABLES (1:1 with pipeline.step_id)
-- =========================================================

CREATE TABLE IF NOT EXISTS DynamicJarLoader (
  step_id  INTEGER PRIMARY KEY,
  library  TEXT NOT NULL,
  FOREIGN KEY(step_id) REFERENCES pipeline(step_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS DynamicClassCreator (
  step_id     INTEGER PRIMARY KEY,
  class_name  TEXT NOT NULL,
  FOREIGN KEY(step_id) REFERENCES pipeline(step_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS CreateDirectory (
  step_id        INTEGER PRIMARY KEY,
  directory_name TEXT NOT NULL,
  target_path    TEXT,
  FOREIGN KEY(step_id) REFERENCES pipeline(step_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS CurrentDirUpdate (
  step_id   INTEGER PRIMARY KEY,
  dirname   TEXT NOT NULL,
  FOREIGN KEY(step_id) REFERENCES pipeline(step_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS CreateTextFile (
  step_id      INTEGER PRIMARY KEY,
  file_name    TEXT NOT NULL,
  target_path  TEXT,
  content_text TEXT,
  content_ref  TEXT,
  FOREIGN KEY(step_id) REFERENCES pipeline(step_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS CreateTextFileFromBase64 (
  step_id      INTEGER PRIMARY KEY,
  file_name    TEXT NOT NULL,
  target_path  TEXT,
  content_b64  TEXT NOT NULL,
  FOREIGN KEY(step_id) REFERENCES pipeline(step_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS DynamicDelegateCreator (
  step_id     INTEGER PRIMARY KEY,
  parent      TEXT NOT NULL,
  method_file TEXT,
  field_file  TEXT,
  output_dir  TEXT,
  FOREIGN KEY(step_id) REFERENCES pipeline(step_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS ClassMethodCloner (
  step_id              INTEGER PRIMARY KEY,
  class_name_to_modify TEXT NOT NULL,
  delegate_class       TEXT NOT NULL,
  method_name          TEXT NOT NULL,
  FOREIGN KEY(step_id) REFERENCES pipeline(step_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS ClassFieldCloner (
  step_id              INTEGER PRIMARY KEY,
  class_name_to_modify TEXT NOT NULL,
  delegate_class       TEXT NOT NULL,
  field_name           TEXT NOT NULL,
  FOREIGN KEY(step_id) REFERENCES pipeline(step_id) ON DELETE CASCADE
);

-- =========================================================
-- RunClass (NO classpath stored)
-- =========================================================
CREATE TABLE IF NOT EXISTS RunClass (
  step_id     INTEGER PRIMARY KEY,
  class_name  TEXT NOT NULL,
  args_text   TEXT,
  FOREIGN KEY(step_id) REFERENCES pipeline(step_id) ON DELETE CASCADE
);

-- =========================================================
-- PAYLOAD STORE (large text blobs, no base64 pain)
-- =========================================================
CREATE TABLE IF NOT EXISTS payload_store (
  payload_id   TEXT PRIMARY KEY,
  created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
  mime         TEXT NOT NULL DEFAULT 'text/plain',
  text_content TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_payload_store_created_at ON payload_store(created_at);

-- =========================================================
-- RUNTIME LOG TABLES
-- =========================================================
CREATE TABLE IF NOT EXISTS pipelineLong (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT NOT NULL,
  event_type TEXT NOT NULL,
  status TEXT NOT NULL,
  command TEXT,
  message TEXT
);

CREATE INDEX IF NOT EXISTS idx_pipelineLong_ts ON pipelineLong(ts);

CREATE TABLE IF NOT EXISTS plannerLong (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT NOT NULL,
  level TEXT NOT NULL,
  message TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_plannerLong_ts ON plannerLong(ts);

-- =========================================================
-- CHECKPOINTS (semantic summaries every N steps)
-- =========================================================
CREATE TABLE IF NOT EXISTS pipelineCheckpoints (
  checkpoint_id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
  run_id        TEXT,
  step_id_from  INTEGER NOT NULL,
  step_id_to    INTEGER NOT NULL,
  n_steps       INTEGER NOT NULL,
  summary       TEXT NOT NULL,
  state_json    TEXT,
  FOREIGN KEY(step_id_to) REFERENCES pipeline(step_id) ON DELETE NO ACTION
);

CREATE INDEX IF NOT EXISTS idx_pipelineCheckpoints_run_id  ON pipelineCheckpoints(run_id);
CREATE INDEX IF NOT EXISTS idx_pipelineCheckpoints_step_to ON pipelineCheckpoints(step_id_to);
"""


def init_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=30.0)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.executescript(DDL)
        conn.commit()
    finally:
        conn.close()


def main():
    db = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(DEFAULT_DB)
    init_db(db)
    print(f"[OK] Initialized SQLite schema at: {db.resolve()}")
    print(f"[TS] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
