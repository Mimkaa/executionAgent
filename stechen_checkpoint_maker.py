# stechen_checkpoint_maker.py
#
# Creates pipelineCheckpoints every K steps (default 50).
# Uses StechenGPTSummarizer to produce the "2) WHAT HAPPENED" summary.
#
# Usage:
#   python stechen_checkpoint_maker.py
#   python stechen_checkpoint_maker.py stechen.db RULES.txt 50
#
# Env overrides:
#   STECHEN_DB_PATH, STECHEN_RULES_FILE, STECHEN_MODEL, STECHEN_CHECKPOINT_K
#
# Requirements:
#   pip install openai
#   set OPENAI_API_KEY
#
import os
import sys
import sqlite3
import json
from pathlib import Path
from typing import Optional, Tuple, List

# Import your class-based summarizer (from the previous refactor)
# File name should match where you saved it.
from stechen_gpt_summarize import StechenGPTSummarizer


DEFAULT_DB = "stechen.db"
DEFAULT_RULES = "RULES.txt"
DEFAULT_MODEL = "gpt-5.2"
DEFAULT_K = 50


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def _get_last_checkpoint(conn: sqlite3.Connection, run_id: Optional[str]) -> Optional[sqlite3.Row]:
    cur = conn.cursor()
    if run_id is None:
        return cur.execute(
            "SELECT * FROM pipelineCheckpoints ORDER BY checkpoint_id DESC LIMIT 1"
        ).fetchone()
    return cur.execute(
        "SELECT * FROM pipelineCheckpoints WHERE run_id=? ORDER BY checkpoint_id DESC LIMIT 1",
        (run_id,),
    ).fetchone()


def _get_next_window_step_ids(
    conn: sqlite3.Connection,
    after_step_id: int,
    k: int,
    run_id: Optional[str],
) -> Optional[Tuple[int, int, int]]:
    """
    Returns (step_id_from, step_id_to, n_steps_in_window) if at least 1 step exists.
    Only returns a full window (n_steps_in_window == k) if enough steps exist.
    """
    cur = conn.cursor()
    if run_id is None:
        rows = cur.execute(
            """
            SELECT step_id
            FROM pipeline
            WHERE step_id > ?
            ORDER BY step_id ASC
            LIMIT ?
            """,
            (after_step_id, k),
        ).fetchall()
    else:
        rows = cur.execute(
            """
            SELECT step_id
            FROM pipeline
            WHERE step_id > ? AND run_id = ?
            ORDER BY step_id ASC
            LIMIT ?
            """,
            (after_step_id, run_id, k),
        ).fetchall()

    if not rows:
        return None

    step_ids = [int(r["step_id"]) for r in rows]
    return step_ids[0], step_ids[-1], len(step_ids)


def _infer_window_run_id(conn: sqlite3.Connection, step_id_from: int, step_id_to: int) -> Optional[str]:
    """
    If all steps in the window share the same non-null run_id, return it; else None.
    """
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT DISTINCT run_id
        FROM pipeline
        WHERE step_id BETWEEN ? AND ?
        """,
        (step_id_from, step_id_to),
    ).fetchall()

    distinct = [r["run_id"] for r in rows]
    distinct = [x for x in distinct if x is not None]
    if len(distinct) == 1:
        return distinct[0]
    return None


def _collect_state_json(conn: sqlite3.Connection, step_id_to: int) -> str:
    """
    Optional: store a tiny bit of state for debugging.
    Keeps it small and stable.
    """
    cur = conn.cursor()
    total_steps = cur.execute("SELECT COUNT(*) AS c FROM pipeline").fetchone()["c"]
    latest = cur.execute(
        "SELECT step_id, script_name, created_at, run_id, step_index FROM pipeline ORDER BY step_id DESC LIMIT 1"
    ).fetchone()

    state = {
        "total_steps": int(total_steps),
        "checkpoint_up_to_step_id": int(step_id_to),
        "latest_step": dict(latest) if latest else None,
    }
    return json.dumps(state, ensure_ascii=False)


def _insert_checkpoint(
    conn: sqlite3.Connection,
    run_id: Optional[str],
    step_id_from: int,
    step_id_to: int,
    n_steps: int,
    summary: str,
    state_json: Optional[str],
) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO pipelineCheckpoints (run_id, step_id_from, step_id_to, n_steps, summary, state_json)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (run_id, step_id_from, step_id_to, n_steps, summary, state_json),
    )
    conn.commit()
    return int(cur.lastrowid)


class RangeSummarizer(StechenGPTSummarizer):
    """
    Adds summarize_range(step_id_from, step_id_to, run_id=None)
    without changing your original class file.
    """

    def summarize_range(self, step_id_from: int, step_id_to: int, run_id: Optional[str] = None) -> str:
        rules_text = self._cap(self._read_text(self.rules_file), self.MAX_RULES_CHARS)

        if not self.db_path.exists():
            return f"2) WHAT HAPPENED\n- No database found at: {self.db_path}"

        # Load the steps in this range and format them using the same internal formatting
        conn = sqlite3.connect(str(self.db_path), timeout=30.0)
        conn.row_factory = sqlite3.Row
        try:
            cur = conn.cursor()
            if run_id is None:
                rows = cur.execute(
                    """
                    SELECT step_id, script_name, created_at, run_id, step_index
                    FROM pipeline
                    WHERE step_id BETWEEN ? AND ?
                    ORDER BY step_id ASC
                    """,
                    (step_id_from, step_id_to),
                ).fetchall()
            else:
                rows = cur.execute(
                    """
                    SELECT step_id, script_name, created_at, run_id, step_index
                    FROM pipeline
                    WHERE step_id BETWEEN ? AND ? AND run_id=?
                    ORDER BY step_id ASC
                    """,
                    (step_id_from, step_id_to, run_id),
                ).fetchall()

            if not rows:
                return "2) WHAT HAPPENED\n- No pipeline steps found in requested range."

            steps = []
            for r in rows:
                step_id = int(r["step_id"])
                script_name = str(r["script_name"])
                payload = self._fetch_payload(conn, script_name, step_id)
                steps.append(
                    {
                        "step_id": step_id,
                        "script_name": script_name,
                        "created_at": r["created_at"],
                        "run_id": r["run_id"],
                        "step_index": r["step_index"],
                        "payload": payload,
                    }
                )

            steps_text = self._format_steps(steps)

        finally:
            conn.close()

        input_text = (
            "STECHEN_SYSTEM_RULES:\n"
            f"{rules_text}\n\n"
            "LAST_STEPS:\n"
            f"{steps_text}\n"
        )

        resp = self._client.responses.create(
            model=self.model,
            instructions=self._build_instructions(),
            input=input_text,
            reasoning={"effort": "low"},
        )

        out = (resp.output_text or "").strip()
        if not out.startswith("2) WHAT HAPPENED"):
            out = "2) WHAT HAPPENED\n" + out
        return out


def make_checkpoints(db_path: Path, rules_file: Path, model: str, k: int, run_id: Optional[str]) -> None:
    summarizer = RangeSummarizer(db_path=str(db_path), rules_file=str(rules_file), model=model)

    conn = _connect(db_path)
    try:
        last_cp = _get_last_checkpoint(conn, run_id=run_id)
        last_to = int(last_cp["step_id_to"]) if last_cp else 0

        created = 0

        while True:
            window = _get_next_window_step_ids(conn, after_step_id=last_to, k=k, run_id=run_id)
            if window is None:
                break

            step_id_from, step_id_to, n_steps_in_window = window

            # Only create checkpoint when we have FULL k steps
            if n_steps_in_window < k:
                break

            # Decide checkpoint run_id:
            # - if user requested run_id, keep it
            # - else infer if stable across the range
            cp_run_id = run_id or _infer_window_run_id(conn, step_id_from, step_id_to)

            summary = summarizer.summarize_range(step_id_from, step_id_to, run_id=run_id)
            state_json = _collect_state_json(conn, step_id_to)

            checkpoint_id = _insert_checkpoint(
                conn,
                run_id=cp_run_id,
                step_id_from=step_id_from,
                step_id_to=step_id_to,
                n_steps=k,
                summary=summary,
                state_json=state_json,
            )

            print(f"[OK] Created checkpoint_id={checkpoint_id} for steps {step_id_from}..{step_id_to} (n={k})")
            created += 1
            last_to = step_id_to

        if created == 0:
            print(f"[INFO] No new full checkpoint window of {k} steps found after step_id={last_to}.")
        else:
            print(f"[DONE] Created {created} checkpoint(s).")

    finally:
        conn.close()


if __name__ == "__main__":
    db_path = Path(os.getenv("STECHEN_DB_PATH", DEFAULT_DB))
    rules_file = Path(os.getenv("STECHEN_RULES_FILE", DEFAULT_RULES))
    model = os.getenv("STECHEN_MODEL", DEFAULT_MODEL)
    k = int(os.getenv("STECHEN_CHECKPOINT_K", str(DEFAULT_K)))

    # CLI overrides
    # python stechen_checkpoint_maker.py stechen.db RULES.txt 50
    if len(sys.argv) >= 2:
        db_path = Path(sys.argv[1])
    if len(sys.argv) >= 3:
        rules_file = Path(sys.argv[2])
    if len(sys.argv) >= 4:
        k = int(sys.argv[3])

    # Optional: set STECHEN_RUN_ID_FILTER to restrict checkpoints to a run_id
    run_id_filter = os.getenv("STECHEN_RUN_ID_FILTER", None)

    make_checkpoints(
        db_path=db_path,
        rules_file=rules_file,
        model=model,
        k=k,
        run_id=run_id_filter,
    )
