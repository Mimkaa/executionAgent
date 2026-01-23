# run_all_pipeline_steps.py
#
# Run ALL steps currently stored in pipeline table (ordered by step_id ASC),
# using DBOperator to fetch payload for each step and executing the corresponding
# Java tool fetched from GitHub (cached locally).
#
# GUARANTEES:
# - Java stdout + stderr are always captured
# - Errors are printed immediately to console
# - Full error output + Python traceback stored in pipelineLong
# - No silent failures
# - NEW: RunClass can fail even with exit code 0 if output contains Java exceptions
#

import os
import sys
import time
import base64
import subprocess
import urllib.request
import sqlite3
import traceback
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

from db_operator import DBOperator
from stechen_checkpoint_maker import make_checkpoints


# =========================================================
# CONFIG
# =========================================================
DB_PATH = os.getenv("STECHEN_DB_PATH", "stechen.db")
WORK_DIR = Path(os.getenv("STECHEN_WORK_DIR", ".")).resolve()
GITHUB_BASE_RAW = os.getenv(
    "STECHEN_GITHUB_BASE_RAW",
    "https://raw.githubusercontent.com/Mimkaa/ObjectSchemas/main"
)

JAVA_CMD = "java"
JAVAC_CMD = "javac"
CLASSPATH_SEP = ";" if os.name == "nt" else ":"

PIPELINE_LONG_TABLE = "pipelineLong"
MAX_DB_MSG = 20_000
MAX_DB_CMD = 50_000

CHECKPOINTS_ENABLED = os.getenv("STECHEN_CHECKPOINTS_ENABLED", "1") == "1"
CHECKPOINT_K = int(os.getenv("STECHEN_CHECKPOINT_K", "50"))
RULES_FILE = Path(os.getenv("STECHEN_RULES_FILE", "RULES.txt")).resolve()
STECHEN_MODEL = os.getenv("STECHEN_MODEL", "gpt-5.2")
CHECKPOINT_RUN_ID_FILTER = os.getenv("STECHEN_RUN_ID_FILTER", None)


# =========================================================
# UTILITIES
# =========================================================
def log(*args):
    try:
        print(*args)
    except Exception:
        pass


def _ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def b64_utf8(s: str) -> str:
    return base64.b64encode((s or "").encode("utf-8", errors="replace")).decode("ascii")


def format_exc(e: BaseException) -> str:
    return "".join(traceback.format_exception(type(e), e, e.__traceback__))


def combine_out(res: subprocess.CompletedProcess) -> str:
    return (res.stdout or "") + ("\n" if (res.stdout and res.stderr) else "") + (res.stderr or "")


def build_classpath() -> str:
    jars = [str(p) for p in WORK_DIR.glob("*.jar")]
    return CLASSPATH_SEP.join([str(WORK_DIR)] + jars)


# =========================================================
# SEMANTIC FAILURE DETECTION (RunClass)
# =========================================================
FAIL_PATTERNS = [
    r"\bException in thread\b",
    r"\bNoClassDefFoundError\b",
    r"\bClassNotFoundException\b",
    r"\bLinkageError\b",
    r"\bUnsatisfiedLinkError\b",
    r"\bWebcamException\b",
    r"\bError: Unable to initialize main class\b",
]

def looks_like_java_failure(output: str) -> bool:
    if not output:
        return False

    strong = [
        "Exception in thread",
        "NoClassDefFoundError",
        "ClassNotFoundException",
        "UnsatisfiedLinkError",
        "LinkageError",
        "Error: Unable to initialize main class",
    ]
    if any(s in output for s in strong):
        return True

    for pat in FAIL_PATTERNS:
        if re.search(pat, output):
            return True

    return False


# =========================================================
# DB LOGGING
# =========================================================
def ensure_pipeline_long_table(conn: sqlite3.Connection) -> None:
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {PIPELINE_LONG_TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            event_type TEXT NOT NULL,   -- STEP | RESULT | CHECKPOINT
            status TEXT NOT NULL,       -- RUN | SUCCESS | FAIL
            command TEXT,
            message TEXT
        )
    """)
    conn.commit()


def db_event(conn, event_type, status, command, message):
    cmd = (command or "")
    msg = (message or "")

    if len(cmd) > MAX_DB_CMD:
        cmd = cmd[:MAX_DB_CMD] + " ... [truncated]"
    if len(msg) > MAX_DB_MSG:
        msg = msg[:MAX_DB_MSG] + " ... [truncated]"

    conn.execute(
        f"INSERT INTO {PIPELINE_LONG_TABLE}(ts,event_type,status,command,message)"
        f" VALUES(?,?,?,?,?)",
        (_ts(), event_type, status, cmd, msg),
    )
    conn.commit()


# =========================================================
# JAVA EXECUTION
# =========================================================
def download_java(script_name: str) -> Path:
    path = WORK_DIR / f"{script_name}.java"
    if path.exists():
        return path

    url = f"{GITHUB_BASE_RAW}/{script_name}.java"
    log("[DL]", url)

    with urllib.request.urlopen(url) as r, open(path, "wb") as f:
        f.write(r.read())

    return path


def compile_java(script_name: str) -> None:
    java_file = f"{script_name}.java"
    cp = build_classpath()
    cmd = [JAVAC_CMD, "-cp", cp, java_file]

    res = subprocess.run(
        cmd,
        cwd=str(WORK_DIR),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    if res.returncode != 0:
        combined = combine_out(res)
        log("❌ [JAVAC FAILED]", " ".join(cmd))
        log(combined)
        raise RuntimeError(f"[JAVAC] failed for {java_file}\n{combined}")


def run_java(script_name: str, argv: List[str]) -> str:
    cp = build_classpath()
    cmd = [JAVA_CMD, "-cp", cp, script_name] + argv

    log("[JAVA]", " ".join(cmd))

    res = subprocess.run(
        cmd,
        cwd=str(WORK_DIR),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    combined = combine_out(res)

    # 1) Hard failure if JVM returned non-zero
    if res.returncode != 0:
        log("❌ [JAVA FAILED]")
        log(combined)
        raise RuntimeError(f"[JAVA] exited with {res.returncode}\n{combined}")

    # 2) Semantic failure for RunClass: child printed exceptions but exited 0
    if script_name == "RunClass" and looks_like_java_failure(combined):
        log("❌ [JAVA SEMANTIC FAIL] RunClass output indicates failure (exit code was 0)")
        log(combined)
        raise RuntimeError(
            "[JAVA] RunClass child program printed exception/error but exited with 0.\n"
            "Treating as FAIL so planner can react.\n\n" + combined
        )

    return combined


# =========================================================
# STEP -> ARGV
# =========================================================
def build_argv_for_step(step: Dict[str, Any]) -> Tuple[str, List[str], str]:
    script = step["script_name"]
    payload = step.get("payload") or {}

    mapping = {
        "DynamicJarLoader": [("library", "--libraryB64")],
        "DynamicClassCreator": [("class_name", "--nameB64")],
        "CreateDirectory": [("directory_name", "--nameB64"), ("target_path", "--pathB64")],
        "CurrentDirUpdate": [("dirname", "--dirnameB64")],
        "CreateTextFile": [("file_name", "--nameB64"), ("target_path", "--pathB64")],
        "CreateTextFileFromBase64": [("file_name", "--nameB64"), ("target_path", "--pathB64")],
        "DynamicDelegateCreator": [
            ("parent", "--parentB64"),
            ("field_file", "--fieldFileB64"),
            ("method_file", "--methodFileB64"),
            ("output_dir", "--outputDirB64"),
        ],
        "ClassMethodCloner": [
            ("class_name_to_modify", "--classNameToModifyB64"),
            ("delegate_class", "--delegateclassB64"),
            ("method_name", "--methodB64"),
        ],
        "ClassFieldCloner": [
            ("class_name_to_modify", "--classNameToModifyB64"),
            ("delegate_class", "--delegateclassB64"),
            ("field_name", "--fieldB64"),
        ],
        "RunClass": [
            ("class_name", "--classB64"),
            ("args_text", "--argsB64"),
        ],
    }

    argv = []
    for col, flag in mapping.get(script, []):
        v = payload.get(col)
        if v:
            argv += [flag, b64_utf8(str(v))]

    if script == "CreateTextFile":
        text = payload.get("_resolved_content") or payload.get("content_text")
        if text is not None:
            argv += ["--contentB64", b64_utf8(str(text))]

    if script == "CreateTextFileFromBase64":
        if payload.get("content_b64"):
            argv += ["--contentB64", payload["content_b64"]]

    human_cmd = f"java {script} " + " ".join(argv)
    return script, argv, human_cmd


# =========================================================
# SAFE STEP EXECUTION
# =========================================================
def run_one_step(conn, step) -> bool:
    script, argv, human_cmd = build_argv_for_step(step)

    db_event(conn, "STEP", "RUN", human_cmd, f"Executing {script}")

    try:
        download_java(script)
        compile_java(script)
        out = run_java(script, argv)

        db_event(conn, "RESULT", "SUCCESS", human_cmd, out or "OK")
        return True

    except Exception as e:
        msg = f"{e}\n\n--- TRACEBACK ---\n{format_exc(e)}"
        db_event(conn, "RESULT", "FAIL", human_cmd, msg)
        log("❌ STEP FAILED:", human_cmd)
        log(msg)
        return False


# =========================================================
# MAIN
# =========================================================
def main():
    log("[RUN] Replaying ALL pipeline steps")

    WORK_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)

    try:
        ensure_pipeline_long_table(conn)
        db = DBOperator(DB_PATH)

        steps = db.fetch_pipeline_steps_ordered()
        if not steps:
            log("[RUN] No pipeline steps found.")
            return

        ok_count = 0

        for step in steps:
            if not run_one_step(conn, step):
                log("[RUN] STOPPED due to failure.")
                return

            ok_count += 1

            if CHECKPOINTS_ENABLED and ok_count % CHECKPOINT_K == 0:
                try:
                    make_checkpoints(
                        db_path=DB_PATH,
                        rules_file=str(RULES_FILE),
                        model=STECHEN_MODEL,
                        run_id_filter=CHECKPOINT_RUN_ID_FILTER,
                    )
                    db_event(conn, "CHECKPOINT", "SUCCESS", "", f"Checkpoint at step {ok_count}")
                except Exception as ce:
                    db_event(
                        conn,
                        "CHECKPOINT",
                        "FAIL",
                        "",
                        f"{ce}\n\n--- TRACEBACK ---\n{format_exc(ce)}",
                    )
                    log("⚠️ Checkpoint failed (non-fatal)")

        log(f"[RUN] Completed successfully ({ok_count} steps).")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
