# db_operator.py
#
# DBOperator for STECHEN "pipeline(step_id)" schema.
# - pipeline: one row per step (script_name + created_at + optional run_id, step_index)
# - per-script tables: 1:1 payload rows keyed by step_id (PRIMARY KEY + FK -> pipeline.step_id)
# - payload_store: store large blobs (Java source etc) and reference by content_ref
#
# PLUS (runtime + planner support):
# - pipelineLong: executor log (STEP/RESULT only)
# - pipelineCheckpoints: semantic summaries every N steps
#
# Usage:
#   from db_operator import DBOperator
#   op = DBOperator("stechen.db")
#   op.ensure_runtime_tables()
#   step_id = op.insert_step("DynamicJarLoader", {"library": "org.json:json:20240303"}, run_id="run1", step_index=1)
#   op.log_pipeline_long("STEP", "RUN", "java DynamicJarLoader --libraryB64 ...", "Starting execution")
#   op.close()

import time
import sqlite3
import hashlib
from typing import Any, Dict, List, Optional


class DBOperator:
    def __init__(self, db_path: str = "stechen.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path, timeout=30.0)
        self.conn.row_factory = sqlite3.Row

        # good concurrent behavior on Windows
        try:
            self.conn.execute("PRAGMA foreign_keys = ON;")
            self.conn.execute("PRAGMA journal_mode = WAL;")
            self.conn.execute("PRAGMA synchronous = NORMAL;")
        except Exception:
            pass

    # --------------------------------------------------
    # small utils
    # --------------------------------------------------
    @staticmethod
    def _ts() -> str:
        return time.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _cap(s: Optional[str], n: int) -> str:
        if s is None:
            return ""
        s = str(s)
        return s if len(s) <= n else s[:n] + " ... [truncated]"

    @staticmethod
    def _safe_table_name(name: str) -> str:
        # Very defensive: allow only [A-Za-z0-9_]
        out = []
        for ch in str(name):
            if ch.isalnum() or ch == "_":
                out.append(ch)
        return "".join(out)

    # --------------------------------------------------
    # PAYLOAD STORE (for huge text, no base64)
    # --------------------------------------------------
    def put_payload(self, text: str, mime: str = "text/plain", payload_id: Optional[str] = None) -> str:
        """
        Stores text in payload_store and returns payload_id.
        If payload_id is None, generates a deterministic hash id.
        """
        cur = self.conn.cursor()
        if payload_id is None:
            h = hashlib.sha256((text or "").encode("utf-8", errors="replace")).hexdigest()[:16]
            payload_id = f"payload:{h}"

        cur.execute(
            """
            INSERT OR REPLACE INTO payload_store(payload_id, mime, text_content)
            VALUES (?, ?, ?)
            """,
            (payload_id, mime, text or ""),
        )
        self.conn.commit()
        return payload_id

    def get_payload(self, payload_id: str) -> str:
        cur = self.conn.cursor()
        row = cur.execute(
            "SELECT text_content FROM payload_store WHERE payload_id=?",
            (payload_id,),
        ).fetchone()
        if not row:
            raise KeyError(f"No payload_store row for payload_id={payload_id}")
        return row["text_content"]

    # --------------------------------------------------
    # INSERT STEP
    # --------------------------------------------------
    def insert_step(
        self,
        script_name: str,
        params: Dict[str, Any],
        run_id: Optional[str] = None,
        step_index: Optional[int] = None,
    ) -> int:
        """
        Inserts:
          1) one row into pipeline
          2) one row into the per-script table named `script_name` (keyed by step_id)

        Returns:
          step_id
        """
        script_name = self._safe_table_name(script_name)
        cur = self.conn.cursor()

        # 1) pipeline row
        cur.execute(
            "INSERT INTO pipeline(script_name, run_id, step_index) VALUES (?,?,?)",
            (script_name, run_id, step_index),
        )
        step_id = int(cur.lastrowid)

        # 2) per-script payload row
        cols = [r["name"] for r in cur.execute(f"PRAGMA table_info({script_name})").fetchall()]
        if not cols:
            raise RuntimeError(f"Table '{script_name}' does not exist (schema not initialized?)")
        if "step_id" not in cols:
            raise RuntimeError(f"Table '{script_name}' must contain column step_id")

        payload_cols = [k for k in params.keys() if k in cols and k != "step_id"]

        sql_cols = ["step_id"] + payload_cols
        sql_vals = [step_id] + [params[c] for c in payload_cols]

        placeholders = ",".join("?" for _ in sql_cols)
        col_list = ",".join(sql_cols)

        cur.execute(
            f"INSERT INTO {script_name}({col_list}) VALUES ({placeholders})",
            sql_vals,
        )

        self.conn.commit()
        return step_id

    # --------------------------------------------------
    # GET ONE STEP (with payload)
    # --------------------------------------------------
    def get_step(self, step_id: int) -> Dict[str, Any]:
        cur = self.conn.cursor()

        row = cur.execute(
            "SELECT step_id, script_name, created_at, run_id, step_index FROM pipeline WHERE step_id=?",
            (int(step_id),),
        ).fetchone()

        if row is None:
            raise KeyError(f"No pipeline step {step_id}")

        script_name = self._safe_table_name(row["script_name"])

        payload = cur.execute(
            f"SELECT * FROM {script_name} WHERE step_id=?",
            (int(step_id),),
        ).fetchone()

        payload_dict = dict(payload) if payload else {}

        # Optional convenience: auto-resolve content_ref into _resolved_content
        if "content_ref" in payload_dict and payload_dict.get("content_ref"):
            ref = payload_dict["content_ref"]
            try:
                payload_dict["_resolved_content"] = self.get_payload(ref)
            except Exception:
                payload_dict["_resolved_content"] = None

        return {
            "step_id": int(step_id),
            "script_name": script_name,
            "created_at": row["created_at"],
            "run_id": row["run_id"],
            "step_index": row["step_index"],
            "payload": payload_dict,
        }

    # --------------------------------------------------
    # GET LAST N (newest first by default)
    # --------------------------------------------------
    def get_last_n(self, n: int, chronological: bool = False) -> List[Dict[str, Any]]:
        cur = self.conn.cursor()

        rows = cur.execute(
            "SELECT step_id FROM pipeline ORDER BY step_id DESC LIMIT ?",
            (int(n),),
        ).fetchall()

        steps = [self.get_step(int(r["step_id"])) for r in rows]
        if chronological:
            steps.reverse()
        return steps

    # --------------------------------------------------
    # GET ALL STEPS (chronological)
    # --------------------------------------------------
    def get_all_steps(self) -> List[Dict[str, Any]]:
        cur = self.conn.cursor()
        rows = cur.execute("SELECT step_id FROM pipeline ORDER BY step_id ASC").fetchall()
        return [self.get_step(int(r["step_id"])) for r in rows]

    # --------------------------------------------------
    # DELETE STEP (cascade will remove per-script row)
    # --------------------------------------------------
    def delete_step(self, step_id: int) -> None:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM pipeline WHERE step_id=?", (int(step_id),))
        self.conn.commit()

    # --------------------------------------------------
    # PIPELINE LONG (executor log: STEP / RESULT only)
    # --------------------------------------------------
    def ensure_pipeline_long(self) -> None:
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS pipelineLong (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                event_type TEXT NOT NULL,   -- STEP | RESULT
                status TEXT NOT NULL,       -- RUN | SUCCESS | FAIL
                command TEXT,
                message TEXT
            )
        """)
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_pipelineLong_ts ON pipelineLong(ts)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_pipelineLong_event ON pipelineLong(event_type)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_pipelineLong_status ON pipelineLong(status)")
        self.conn.commit()

    def log_pipeline_long(self, event_type: str, status: str, command: str, message: str) -> None:
        """
        Deterministic logger:
        - STEP: status should be RUN
        - RESULT: status should be SUCCESS or FAIL
        """
        event_type = (event_type or "").strip()
        status = (status or "").strip()

        cmd = self._cap(command, 50_000)
        msg = self._cap(message, 20_000)

        self.conn.execute(
            "INSERT INTO pipelineLong(ts, event_type, status, command, message) VALUES(?,?,?,?,?)",
            (self._ts(), event_type, status, cmd, msg),
        )
        self.conn.commit()

    # --------------------------------------------------
    # CHECKPOINTS (semantic summaries)
    # --------------------------------------------------
    def ensure_checkpoints(self) -> None:
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS pipelineCheckpoints (
                checkpoint_id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
                run_id        TEXT,
                step_id_from  INTEGER NOT NULL,
                step_id_to    INTEGER NOT NULL,
                n_steps       INTEGER NOT NULL,
                summary       TEXT NOT NULL,
                state_json    TEXT
            )
        """)
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_pipelineCheckpoints_run_id  ON pipelineCheckpoints(run_id)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_pipelineCheckpoints_step_to ON pipelineCheckpoints(step_id_to)")
        self.conn.commit()

    def get_last_checkpoint(self) -> Optional[Dict[str, Any]]:
        cur = self.conn.cursor()
        row = cur.execute(
            """
            SELECT checkpoint_id, created_at, run_id, step_id_from, step_id_to, n_steps, summary, state_json
            FROM pipelineCheckpoints
            ORDER BY checkpoint_id DESC
            LIMIT 1
            """
        ).fetchone()
        if not row:
            return None
        return {
            "checkpoint_id": int(row["checkpoint_id"]),
            "created_at": row["created_at"],
            "run_id": row["run_id"],
            "step_id_from": int(row["step_id_from"]),
            "step_id_to": int(row["step_id_to"]),
            "n_steps": int(row["n_steps"]),
            "summary": row["summary"],
            "state_json": row["state_json"],
        }

    def insert_checkpoint(
        self,
        run_id: Optional[str],
        step_id_from: int,
        step_id_to: int,
        n_steps: int,
        summary: str,
        state_json: Optional[str] = None,
    ) -> int:
        self.ensure_checkpoints()

        summary_c = self._cap(summary, 15_000)
        state_c = self._cap(state_json, 15_000) if state_json is not None else None

        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO pipelineCheckpoints(run_id, step_id_from, step_id_to, n_steps, summary, state_json)
            VALUES(?,?,?,?,?,?)
            """,
            (run_id, int(step_id_from), int(step_id_to), int(n_steps), summary_c, state_c),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    # --------------------------------------------------
    # Convenience: ensure runtime tables exist
    # --------------------------------------------------
    def ensure_runtime_tables(self) -> None:
        self.ensure_pipeline_long()
        self.ensure_checkpoints()

    # --------------------------------------------------
    # CLOSE
    # --------------------------------------------------
    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass
