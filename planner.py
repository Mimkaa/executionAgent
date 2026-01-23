# planner.py (FIXED)
#
# Fixes:
# 1) Keep RAW script_name for executor; only sanitize for table lookups.
# 2) NEVER include CreateTextFileFromBase64 in schema hint list.
# 3) Hard-enforce: CreateTextFile MUST include content_text OR content_ref (no empty files).
#
# Drop-in replacement: you can paste this over your current planner.py.
#
import os
import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

from openai import OpenAI
from db_operator import DBOperator
from stechen_gpt_summarize import StechenGPTSummarizer

# -----------------------------
# CONFIG
# -----------------------------
DB_PATH = os.getenv("STECHEN_DB_PATH", "stechen.db")
RULES_FILE = os.getenv("STECHEN_RULES_FILE", "RULES.txt")
GOAL_FILE = os.getenv("STECHEN_GOAL_FILE", "goal.txt")

N_STEPS = int(os.getenv("STECHEN_N_STEPS", "5"))
MODEL = os.getenv("STECHEN_MODEL", "gpt-5.2")
DEFAULT_RUN_ID = os.getenv("STECHEN_RUN_ID", None)

PIPELINE_LONG_TABLE = "pipelineLong"
CHECKPOINTS_TABLE = "pipelineCheckpoints"

# Force these per your request:
FRESH_SUMMARY_N = 50
EVIDENCE_N = 5

SEED_FIRST_STEP = {
    "script_name": "DynamicJarLoader",
    "params": {"library": "net.bytebuddy:byte-buddy:1.15.3"},
    "run_id": DEFAULT_RUN_ID,
    "step_index": None,
}


# -----------------------------
# small utils
# -----------------------------
def _read_text(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")


def _safe_table_name(name: str) -> str:
    out = []
    for ch in str(name):
        if ch.isalnum() or ch == "_":
            out.append(ch)
    return "".join(out)


def log(*args) -> None:
    try:
        print(*args)
    except Exception:
        pass


def row_to_dict(cur: sqlite3.Cursor, row: Any) -> Dict[str, Any]:
    if row is None:
        return {}
    if isinstance(row, sqlite3.Row):
        return dict(row)
    cols = [d[0] for d in (cur.description or [])]
    return dict(zip(cols, row))


# -----------------------------
# DB reads: pipelineLong + checkpoints
# -----------------------------
def ensure_schema_tables_exist(conn: sqlite3.Connection) -> None:
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {PIPELINE_LONG_TABLE} (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts TEXT NOT NULL,
          event_type TEXT NOT NULL,
          status TEXT NOT NULL,
          command TEXT,
          message TEXT
        )
    """)
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {CHECKPOINTS_TABLE} (
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
    conn.commit()


def get_last_pipeline_long(conn: sqlite3.Connection) -> Dict[str, Optional[Dict[str, Any]]]:
    out: Dict[str, Optional[Dict[str, Any]]] = {"last_step": None, "last_result": None}
    cur = conn.cursor()

    cur.execute(
        f"""
        SELECT id, ts, event_type, status, command, message
        FROM {PIPELINE_LONG_TABLE}
        WHERE event_type='STEP'
        ORDER BY id DESC
        LIMIT 1
        """
    )
    row_step = cur.fetchone()
    if row_step:
        out["last_step"] = row_to_dict(cur, row_step)

    cur.execute(
        f"""
        SELECT id, ts, event_type, status, command, message
        FROM {PIPELINE_LONG_TABLE}
        WHERE event_type='RESULT'
        ORDER BY id DESC
        LIMIT 1
        """
    )
    row_res = cur.fetchone()
    if row_res:
        out["last_result"] = row_to_dict(cur, row_res)

    return out


def get_last_fail_result(conn: sqlite3.Connection) -> Optional[Dict[str, Any]]:
    cur = conn.cursor()
    row = cur.execute(
        f"""
        SELECT id, ts, event_type, status, command, message
        FROM {PIPELINE_LONG_TABLE}
        WHERE event_type='RESULT' AND status='FAIL'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    return row_to_dict(cur, row) if row else None


def get_all_checkpoints(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT checkpoint_id, created_at, run_id, step_id_from, step_id_to, n_steps, summary, state_json
        FROM {CHECKPOINTS_TABLE}
        ORDER BY checkpoint_id ASC
        """
    )
    rows = cur.fetchall() or []
    return [row_to_dict(cur, r) for r in rows]


def format_all_checkpoints(checkpoints: List[Dict[str, Any]]) -> str:
    if not checkpoints:
        return "CHECKPOINTS: <none>\n"

    parts: List[str] = ["CHECKPOINTS (oldest -> newest):"]
    for cp in checkpoints:
        parts.append(
            f"- checkpoint_id={cp.get('checkpoint_id')} created_at={cp.get('created_at')} run_id={cp.get('run_id')} "
            f"step_id_from={cp.get('step_id_from')} step_id_to={cp.get('step_id_to')} n_steps={cp.get('n_steps')}"
        )
        parts.append("  SUMMARY:")
        parts.append("  " + str(cp.get("summary") or "").replace("\n", "\n  "))
        if cp.get("state_json"):
            parts.append("  STATE_JSON:")
            parts.append("  " + str(cp.get("state_json") or "").replace("\n", "\n  "))
        parts.append("")

    return ("\n".join(parts).strip() + "\n")


# -----------------------------
# Evidence formatting (last N)
# -----------------------------
def _fetch_payload(conn: sqlite3.Connection, script_name: str, step_id: int) -> Dict[str, Any]:
    cur = conn.cursor()
    t = _safe_table_name(script_name)

    cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (t,))
    if not cur.fetchone():
        return {"_payload_error": f"Missing table for script: {t}"}

    cur.execute(f"SELECT * FROM {t} WHERE step_id=?", (int(step_id),))
    row = cur.fetchone()
    if not row:
        return {"_payload_error": f"No payload row in {t} for step_id={step_id}"}

    payload = row_to_dict(cur, row)

    # Resolve payload_store reference if present
    if "content_ref" in payload and payload.get("content_ref"):
        ref = payload["content_ref"]
        cur.execute("SELECT text_content FROM payload_store WHERE payload_id=?", (ref,))
        r2 = cur.fetchone()
        if r2:
            r2d = row_to_dict(cur, r2)
            payload["_resolved_content"] = r2d.get("text_content") or ""

    # Make bytes printable
    for k, v in list(payload.items()):
        if isinstance(v, (bytes, bytearray)):
            payload[k] = f"<{len(v)} bytes>"

    return payload


def _format_steps_for_inference(steps: List[Dict[str, Any]]) -> str:
    chunks: List[str] = []
    for s in steps:
        step_id = s.get("step_id")
        script = s.get("script_name")
        created_at = s.get("created_at")
        run_id = s.get("run_id")
        step_index = s.get("step_index")
        payload = s.get("payload", {})

        chunks.append(
            f"STEP_ID={step_id}  TIME={created_at}  SCRIPT={script}"
            + (f"  RUN_ID={run_id}" if run_id else "")
            + (f"  STEP_INDEX={step_index}" if step_index is not None else "")
        )
        chunks.append("PAYLOAD:")
        chunks.append(str(payload) if payload is not None else "{}")
        chunks.append("----")

    return "\n".join(chunks)


# -----------------------------
# Planner prompt: produce ONE next DB step JSON
# -----------------------------
def planner_instructions(schema_hint: str) -> str:
    return (
        "You are the STECHEN planner.\n"
        "You will be given RULES, GOAL, checkpoints, last executor log, last FAIL traceback, a fresh pipeline summary, and step evidence.\n"
        "You must output EXACTLY ONE next pipeline DB step to insert.\n\n"
        "HARD OUTPUT RULE:\n"
        "- Output ONLY a single JSON object. No markdown. No extra text.\n\n"
        "JSON schema:\n"
        "{\n"
        "  \"script_name\": \"<tool name>\",\n"
        "  \"params\": {\"<db_column>\": \"<value>\", ...},\n"
        "  \"run_id\": \"<optional>\",\n"
        "  \"step_index\": null\n"
        "}\n\n"
        "CRITICAL: params keys MUST match the per-script table column names.\n"
        "Use these exact keys (schema hint):\n"
        f"{schema_hint}\n\n"
        "Constraints:\n"
        "- Choose the smallest safe next step that follows the rules.\n"
        "- Do NOT invent past actions; base decisions on evidence.\n"
        "- NEVER output CreateTextFileFromBase64 (FORBIDDEN).\n"
        "- If script_name is CreateTextFile, you MUST provide content_text OR content_ref (NO empty files).\n"
    )


def parse_plan_json(raw: str) -> Dict[str, Any]:
    s = (raw or "").strip()
    if not s:
        raise ValueError("Planner returned empty output.")

    try:
        obj = json.loads(s)
    except Exception as e:
        raise ValueError(f"Planner output is not valid JSON: {e}\nRAW:\n{s}")

    if not isinstance(obj, dict):
        raise ValueError("Planner JSON must be an object.")
    if "script_name" not in obj or "params" not in obj:
        raise ValueError("Planner JSON must contain 'script_name' and 'params'.")
    if not isinstance(obj["params"], dict):
        raise ValueError("'params' must be an object.")

    if not obj.get("run_id"):
        obj["run_id"] = DEFAULT_RUN_ID
    if "step_index" not in obj:
        obj["step_index"] = None

    return obj


def normalize_plan_params(raw_script_name: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize common alias keys -> canonical DB column keys.
    IMPORTANT: raw_script_name must be the real script name, not sanitized.
    """
    p = dict(params or {})

    if raw_script_name == "DynamicClassCreator" and "class_name" not in p and "name" in p:
        p["class_name"] = p.pop("name")

    if raw_script_name == "DynamicJarLoader" and "library" not in p and "lib" in p:
        p["library"] = p.pop("lib")

    if raw_script_name == "CurrentDirUpdate" and "dirname" not in p and "dir" in p:
        p["dirname"] = p.pop("dir")

    if raw_script_name == "CreateDirectory" and "directory_name" not in p and "name" in p:
        p["directory_name"] = p.pop("name")

    if raw_script_name == "RunClass" and "class_name" not in p and "class" in p:
        p["class_name"] = p.pop("class")

    return p


def get_schema_hint(op: DBOperator, script_names: List[str]) -> str:
    lines: List[str] = []
    cur = op.conn.cursor()
    for s in script_names:
        t = _safe_table_name(s)
        cur.execute(f"PRAGMA table_info({t})")
        cols = cur.fetchall() or []
        if not cols:
            continue

        required: List[str] = []
        optional: List[str] = []
        for r in cols:
            rdict = dict(r) if isinstance(r, sqlite3.Row) else {
                "name": r[1], "notnull": r[3], "dflt_value": r[4], "pk": r[5]
            }
            name = rdict["name"]
            if name == "step_id":
                continue
            notnull = int(rdict.get("notnull") or 0)
            dflt = rdict.get("dflt_value")
            if notnull == 1 and dflt is None:
                required.append(name)
            else:
                optional.append(name)

        if required:
            lines.append(f"- {t}: params MUST include {required}; optional {optional}")
        else:
            lines.append(f"- {t}: optional params {optional}")

    lines.append("")
    lines.append("Examples:")
    lines.append('- DynamicClassCreator → {"class_name": "..."}')
    lines.append('- DynamicJarLoader    → {"library": "net.bytebuddy:byte-buddy:1.15.3"}')
    return "\n".join(lines).strip()


def validate_params_against_schema(op: DBOperator, raw_script_name: str, params: Dict[str, Any]) -> None:
    cur = op.conn.cursor()
    t = _safe_table_name(raw_script_name)

    cur.execute(f"PRAGMA table_info({t})")
    cols = cur.fetchall() or []
    if not cols:
        raise ValueError(f"Schema error: table '{t}' does not exist.")

    required: List[str] = []
    for r in cols:
        rdict = dict(r) if isinstance(r, sqlite3.Row) else {
            "name": r[1], "notnull": r[3], "dflt_value": r[4], "pk": r[5]
        }
        name = rdict["name"]
        if name == "step_id":
            continue
        notnull = int(rdict.get("notnull") or 0)
        dflt = rdict.get("dflt_value")
        pk = int(rdict.get("pk") or 0)
        if pk == 1:
            continue
        if notnull == 1 and dflt is None:
            required.append(name)

    missing = [c for c in required if c not in params or str(params.get(c) or "").strip() == ""]
    if missing:
        raise ValueError(
            f"Planner produced invalid params for '{t}'. Missing required columns: {missing}. "
            f"Got keys: {list(params.keys())}"
        )


def enforce_no_empty_create_text_file(raw_script_name: str, params: Dict[str, Any]) -> None:
    """
    Prevents the exact failure you saw: empty CreateTextFile -> delegate missing method -> cloner fails.
    Your CreateTextFile payload schema uses (likely) content_text or content_ref.
    """
    if raw_script_name != "CreateTextFile":
        return
    content_text = (params.get("content_text") or "").strip()
    content_ref = params.get("content_ref")
    if not content_text and not content_ref:
        raise ValueError(
            "INVALID PLAN: CreateTextFile MUST include content_text or content_ref. "
            "Refusing to insert empty file step."
        )


def pipeline_is_empty(conn: sqlite3.Connection) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pipeline LIMIT 1")
    return cur.fetchone() is None


# -----------------------------
# Main
# -----------------------------
def main():
    base_dir = Path(__file__).resolve().parent

    db_p = Path(DB_PATH)
    if not db_p.is_absolute():
        db_p = (base_dir / db_p).resolve()

    rules_p = Path(RULES_FILE)
    if not rules_p.is_absolute():
        rules_p = (base_dir / rules_p).resolve()

    goal_p = Path(GOAL_FILE)
    if not goal_p.is_absolute():
        goal_p = (base_dir / goal_p).resolve()

    rules_text = _read_text(rules_p)
    goal_text = _read_text(goal_p)

    if not db_p.exists():
        raise FileNotFoundError(f"Database not found: {db_p}")

    conn = sqlite3.connect(str(db_p), timeout=30.0)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
    except Exception:
        pass

    ensure_schema_tables_exist(conn)

    # If pipeline is empty -> seed first mandatory step and exit successfully.
    if pipeline_is_empty(conn):
        log("[PLANNER] Pipeline is empty. Seeding first mandatory step:")
        log("          ", SEED_FIRST_STEP)

        op = DBOperator(str(db_p))
        try:
            op.ensure_runtime_tables()
            raw_script = SEED_FIRST_STEP["script_name"]
            params = SEED_FIRST_STEP["params"]
            run_id = SEED_FIRST_STEP.get("run_id")
            step_index = SEED_FIRST_STEP.get("step_index")

            params = normalize_plan_params(raw_script, params)
            enforce_no_empty_create_text_file(raw_script, params)
            validate_params_against_schema(op, raw_script, params)

            new_step_id = op.insert_step(
                script_name=raw_script,
                params=params,
                run_id=run_id,
                step_index=step_index,
            )
        finally:
            try:
                op.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

        log(f"[OK] Seeded step_id={new_step_id} ({raw_script})")
        return

    # -----------------------------
    # Normal planner flow (non-empty pipeline)
    # -----------------------------
    all_cps = get_all_checkpoints(conn)
    checkpoints_txt = format_all_checkpoints(all_cps)

    logs = get_last_pipeline_long(conn)
    last_fail = get_last_fail_result(conn)

    summarizer = StechenGPTSummarizer(
        db_path=str(db_p),
        rules_file=str(rules_p),
        model=MODEL,
        base_dir=base_dir,
    )
    fresh_summary = summarizer.summarize(n=FRESH_SUMMARY_N, run_id=DEFAULT_RUN_ID or None)

    # Evidence is last EVIDENCE_N pipeline steps
    cur = conn.cursor()
    cur.execute(
        "SELECT step_id, script_name, created_at, run_id, step_index "
        "FROM pipeline ORDER BY step_id DESC LIMIT ?",
        (int(EVIDENCE_N),),
    )
    rows = cur.fetchall() or []

    steps_newest_first: List[Dict[str, Any]] = []
    for r in rows:
        step_id = int(r["step_id"])
        script_name = str(r["script_name"])
        payload = _fetch_payload(conn, script_name, step_id)
        steps_newest_first.append(
            {
                "step_id": step_id,
                "script_name": script_name,
                "created_at": r["created_at"],
                "run_id": r["run_id"],
                "step_index": r["step_index"],
                "payload": payload,
            }
        )
    steps_chrono = list(reversed(steps_newest_first))
    evidence_text = _format_steps_for_inference(steps_chrono)

    # last step/result (existing behavior)
    last_exec_parts = ["EXECUTOR_LOGS:"]
    if logs.get("last_step"):
        last_exec_parts.append(
            f"LAST_STEP: ts={logs['last_step'].get('ts')} status={logs['last_step'].get('status')}"
        )
        last_exec_parts.append(f"COMMAND: {logs['last_step'].get('command')}")
        last_exec_parts.append("MESSAGE:")
        last_exec_parts.append(str(logs["last_step"].get("message") or ""))

    if logs.get("last_result"):
        last_exec_parts.append(
            f"LAST_RESULT: ts={logs['last_result'].get('ts')} status={logs['last_result'].get('status')}"
        )
        last_exec_parts.append(f"COMMAND: {logs['last_result'].get('command')}")
        last_exec_parts.append("MESSAGE:")
        last_exec_parts.append(str(logs["last_result"].get("message") or ""))

    last_exec_txt = "\n".join(last_exec_parts).strip() + "\n"

    if last_fail:
        last_fail_txt = (
            "LAST_FAIL_RESULT (FULL TRACEBACK):\n"
            f"ts={last_fail.get('ts')} id={last_fail.get('id')}\n"
            f"COMMAND: {last_fail.get('command')}\n"
            "MESSAGE:\n"
            f"{str(last_fail.get('message') or '')}\n"
        )
    else:
        last_fail_txt = "LAST_FAIL_RESULT (FULL TRACEBACK): <none>\n"

    # ✅ IMPORTANT: do NOT include CreateTextFileFromBase64 here.
    op_for_schema = DBOperator(str(db_p))
    try:
        schema_hint = get_schema_hint(
            op_for_schema,
            script_names=[
                "DynamicJarLoader",
                "DynamicClassCreator",
                "CreateDirectory",
                "CurrentDirUpdate",
                "CreateTextFile",
                "DynamicDelegateCreator",
                "ClassMethodCloner",
                "ClassFieldCloner",
                "RunClass",
            ],
        )
    finally:
        try:
            op_for_schema.close()
        except Exception:
            pass

    planner_input = (
        "STECHEN_RULES:\n"
        f"{rules_text}\n\n"
        "GOAL:\n"
        f"{goal_text}\n\n"
        f"{checkpoints_txt}\n"
        f"{last_exec_txt}\n"
        f"{last_fail_txt}\n"
        "FRESH_SUMMARY (via StechenGPTSummarizer):\n"
        f"{fresh_summary}\n\n"
        "LAST_PIPELINE_STEPS_EVIDENCE:\n"
        f"{evidence_text}\n"
    )

    client = OpenAI()
    resp = client.responses.create(
        model=MODEL,
        instructions=planner_instructions(schema_hint=schema_hint),
        input=planner_input,
        reasoning={"effort": "low"},
    )
    raw_plan = (resp.output_text or "").strip()
    plan = parse_plan_json(raw_plan)

    raw_script = str(plan["script_name"]).strip()
    params = plan["params"] or {}
    run_id = plan.get("run_id")
    step_index = plan.get("step_index")

    # Hard block forbidden tool even if GPT emits it.
    if raw_script == "CreateTextFileFromBase64":
        raise ValueError("FORBIDDEN: planner attempted to emit CreateTextFileFromBase64.")

    op = DBOperator(str(db_p))
    try:
        op.ensure_runtime_tables()

        params = normalize_plan_params(raw_script, params)
        enforce_no_empty_create_text_file(raw_script, params)
        validate_params_against_schema(op, raw_script, params)

        new_step_id = op.insert_step(
            script_name=raw_script,   # ✅ store REAL script name
            params=params,
            run_id=run_id,
            step_index=step_index,
        )
    finally:
        try:
            op.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

    log(f"[OK] Inserted next pipeline step_id={new_step_id} script={raw_script}")
    log("[OK] Note: planner does NOT write pipelineCheckpoints (handled by checkpoint writer).")


if __name__ == "__main__":
    main()
