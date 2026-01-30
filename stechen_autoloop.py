# stechen_autorun.py
#
# Runs init_stechen_sqlight.py FIRST (always), then bootstraps ByteBuddy,
# then enters the planner/executor loop until a ".ready" file appears
# in the CURRENT working directory.
#
# After EACH run (init / planner / executor), writes a JSON record into ./conductedWork/
#
# NEW:
# - At the VERY BEGINNING:
#     reads ../executorInput/goal.txt and copies its content to ./goal.txt
#
# When the loop halts because .ready exists:
#   1) run work_recorder.py ONCE (produces created_outputs.json)
#   2) read created_outputs.json
#   3) ensure ../executorOutput exists
#   4) copy each listed file from CURRENT DIR -> ../executorOutput/
#   5) create a small python runner script in ../executorOutput that:
#        - reads created_outputs.json
#        - copies created_outputs from its directory into a temp run dir
#        - runs the .class listed under dynamic_class_creator_class using java -cp

import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import sqlite3

from db_operator import DBOperator

# --------------------------------------------------
# Commands (run from CURRENT working directory)
# --------------------------------------------------
DB_PATH = os.getenv("STECHEN_DB_PATH", "stechen.db")
BOOTSTRAP_RUN_ID = os.getenv("STECHEN_RUN_ID", "bootstrap")

INIT_DB_CMD = [sys.executable, "init_stechen_sqlight.py", DB_PATH]
PLANNER_CMD = [sys.executable, "planner.py"]
EXECUTOR_CMD = [sys.executable, "single_step_pipeline_executor.py"]

# Run AFTER .ready is found
WORK_RECORDER_CMD = [sys.executable, "work_recorder.py"]

SLEEP_BETWEEN_ITERATIONS_SEC = 0.2

# --------------------------------------------------
# Paths
# --------------------------------------------------
CWD = Path(".").resolve()
READY_FILE = (CWD / ".ready").resolve()

CONDUCTED_DIR = (CWD / "conductedWork").resolve()
CONDUCTED_DIR.mkdir(parents=True, exist_ok=True)

WORK_DIR = Path(os.getenv("STECHEN_WORK_DIR", ".")).resolve()

CREATED_OUTPUTS_JSON = Path(os.getenv("CREATED_OUTPUTS_JSON", "./created_outputs.json")).resolve()

PARENT_DIR = CWD.parent
EXECUTOR_OUTPUT_DIR = (PARENT_DIR / "executorOutput").resolve()

EXECUTOR_INPUT_DIR = (PARENT_DIR / "executorInput").resolve()
EXECUTOR_INPUT_GOAL = (EXECUTOR_INPUT_DIR / "goal.txt").resolve()
LOCAL_GOAL = (CWD / "goal.txt").resolve()

# Skip noisy folders/files in produced-output diff
SKIP_DIR_NAMES = {
    "__pycache__",
    ".git",
    ".idea",
    ".vscode",
    "node_modules",
    "conductedWork",
}
SKIP_FILE_SUFFIXES = {
    ".pyc",
    ".log",
}


# ==========================================================
# Begin hook: copy goal from ../executorInput/goal.txt -> ./goal.txt
# ==========================================================

def sync_goal_from_executor_input() -> None:
    """
    Reads ../executorInput/goal.txt and copies its CONTENT to ./goal.txt.
    If the source doesn't exist, does nothing (but prints a warning).
    """
    if not EXECUTOR_INPUT_GOAL.exists():
        print(f"[goal] WARN: missing source goal: {EXECUTOR_INPUT_GOAL}")
        return

    try:
        text = EXECUTOR_INPUT_GOAL.read_text(encoding="utf-8", errors="replace")
        LOCAL_GOAL.write_text(text, encoding="utf-8")
        print(f"[goal] Synced: {EXECUTOR_INPUT_GOAL} -> {LOCAL_GOAL}")
    except Exception as e:
        print(f"[goal] WARN: failed to sync goal.txt: {e}")


# ==========================================================
# Helpers: file snapshot + diff
# ==========================================================

def _should_skip_path(p: Path) -> bool:
    parts = set(p.parts)
    if any(name in parts for name in SKIP_DIR_NAMES):
        return True
    if p.suffix.lower() in SKIP_FILE_SUFFIXES:
        return True
    return False


def snapshot_tree(root: Path) -> Dict[str, float]:
    out: Dict[str, float] = {}
    if not root.exists():
        return out
    for fp in root.rglob("*"):
        if not fp.is_file():
            continue
        if _should_skip_path(fp):
            continue
        try:
            rel = fp.relative_to(root).as_posix()
        except Exception:
            rel = str(fp)
        try:
            out[rel] = fp.stat().st_mtime
        except Exception:
            continue
    return out


def diff_snapshots(before: Dict[str, float], after: Dict[str, float]) -> Dict[str, List[str]]:
    created: List[str] = []
    modified: List[str] = []
    deleted: List[str] = []

    for k in after.keys():
        if k not in before:
            created.append(k)
        else:
            if after[k] != before[k]:
                modified.append(k)

    for k in before.keys():
        if k not in after:
            deleted.append(k)

    created.sort()
    modified.sort()
    deleted.sort()

    return {"created": created, "modified": modified, "deleted": deleted}


# ==========================================================
# Helpers: read latest executed step info from stechen.db
# ==========================================================

def connect_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def get_latest_pipeline_step(conn: sqlite3.Connection) -> Optional[Dict[str, Any]]:
    try:
        row = conn.execute(
            "SELECT step_id, script_name, created_at, run_id, step_index "
            "FROM pipeline ORDER BY step_id DESC LIMIT 1"
        ).fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def get_latest_pipelineLong_step_and_result(conn: sqlite3.Connection) -> Dict[str, Any]:
    out: Dict[str, Any] = {"step_log": None, "result_log": None}

    try:
        step_row = conn.execute(
            """
            SELECT id, ts, event_type, status, command, message
            FROM pipelineLong
            WHERE event_type = 'STEP'
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        if not step_row:
            return out
        out["step_log"] = dict(step_row)

        res_row = conn.execute(
            """
            SELECT id, ts, event_type, status, command, message
            FROM pipelineLong
            WHERE event_type = 'RESULT' AND id > ?
            ORDER BY id ASC
            LIMIT 1
            """,
            (step_row["id"],),
        ).fetchone()
        if res_row:
            out["result_log"] = dict(res_row)
        return out
    except Exception:
        return out


def short(s: Optional[str], n: int = 300) -> Optional[str]:
    if s is None:
        return None
    if len(s) <= n:
        return s
    return s[:n] + "...<TRUNCATED>..."


# ==========================================================
# Runner + conductedWork writer
# ==========================================================

def write_conducted_record(record: Dict[str, Any]) -> Path:
    ts = time.strftime("%Y%m%d_%H%M%S")
    fname = f"work_{ts}_{int(time.time() * 1000)}.json"
    out_path = CONDUCTED_DIR / fname
    out_path.write_text(json.dumps(record, indent=2, ensure_ascii=False), encoding="utf-8")
    return out_path


def run_and_record(cmd, label: str) -> bool:
    before = snapshot_tree(WORK_DIR)

    print(f"\n==== {label} ====")
    started = time.time()
    res = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    ended = time.time()

    if res.stdout:
        print(res.stdout)
    if res.stderr:
        print(res.stderr, file=sys.stderr)

    after = snapshot_tree(WORK_DIR)
    produced = diff_snapshots(before, after)

    step_info = None
    logs_info = None
    try:
        with connect_db(DB_PATH) as conn:
            step_info = get_latest_pipeline_step(conn)
            logs_info = get_latest_pipelineLong_step_and_result(conn)
    except Exception:
        pass

    record = {
        "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
        "label": label,
        "cmd": cmd,
        "returncode": res.returncode,
        "duration_sec": round(ended - started, 3),
        "db_path": str(Path(DB_PATH).resolve()),
        "work_dir": str(WORK_DIR),
        "latest_pipeline_row": step_info,
        "pipelineLong": {
            "step": logs_info.get("step_log") if isinstance(logs_info, dict) else None,
            "result": logs_info.get("result_log") if isinstance(logs_info, dict) else None,
        },
        "stdout_head": short(res.stdout, 2000),
        "stderr_head": short(res.stderr, 2000),
        "produced_files": produced,
    }

    out_path = write_conducted_record(record)
    print(f"[conductedWork] Wrote: {out_path}")

    if res.returncode != 0:
        print(f"[STOP] {label} failed with exit code {res.returncode}")
        return False

    return True


# ==========================================================
# Bootstrap logic
# ==========================================================

def bootstrap_bytebuddy() -> bool:
    inserted = False

    op = DBOperator(DB_PATH)
    try:
        op.ensure_runtime_tables()

        cur = op.conn.cursor()
        exists = cur.execute(
            """
            SELECT 1
            FROM pipeline p
            JOIN DynamicJarLoader d ON d.step_id = p.step_id
            WHERE p.script_name = 'DynamicJarLoader'
              AND d.library = ?
            LIMIT 1
            """,
            ("net.bytebuddy:byte-buddy:1.15.3",),
        ).fetchone()

        if exists:
            print("[BOOTSTRAP] ByteBuddy already present in pipeline. Skipping insert.")
        else:
            step_id = op.insert_step(
                script_name="DynamicJarLoader",
                params={"library": "net.bytebuddy:byte-buddy:1.15.3"},
                run_id=BOOTSTRAP_RUN_ID,
                step_index=None,
            )
            inserted = True
            print(f"[BOOTSTRAP] Inserted DynamicJarLoader(byte-buddy) as step_id={step_id}")

    except Exception as e:
        print("[BOOTSTRAP] Failed to insert bootstrap step:", e)
        return False
    finally:
        try:
            op.close()
        except Exception:
            pass

    label = "BOOTSTRAP EXECUTOR (ByteBuddy)" if inserted else "BOOTSTRAP EXECUTOR (already present)"
    return run_and_record(EXECUTOR_CMD, label)


# ==========================================================
# Post-halt: copy created outputs to ../executorOutput
# ==========================================================

def load_created_outputs_payload(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


def copy_outputs_to_executor_output(created_outputs: List[str]) -> None:
    EXECUTOR_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    copied = 0
    missing = 0

    for rel in created_outputs:
        src = (CWD / rel).resolve()
        if not src.exists() or not src.is_file():
            src2 = (CWD / Path(rel).name).resolve()
            if src2.exists() and src2.is_file():
                src = src2
            else:
                missing += 1
                continue

        dst = EXECUTOR_OUTPUT_DIR / src.name
        try:
            shutil.copy2(src, dst)
            copied += 1
        except Exception:
            missing += 1

    print(f"[executorOutput] Dir: {EXECUTOR_OUTPUT_DIR}")
    print(f"[executorOutput] Copied: {copied}")
    if missing:
        print(f"[executorOutput] Missing/failed: {missing}")


def write_executor_runner_script(executor_dir: Path, created_outputs_json_name: str = "created_outputs.json") -> Path:
    """
    Creates ../executorOutput/run_RunClass.py

    The generated runner (run_RunClass.py), when executed inside executorOutput:
      - reads created_outputs_json_name to get "dynamic_class_creator_class"
      - copies RunClass.class from the executionAgent folder (parent of executorOutput) into executorOutput (if missing)
      - runs: java -cp ".:./*" RunClass --class <targetClass>
        (Windows uses ';' separator automatically)
    """
    runner_path = executor_dir / "run_RunClass.py"

    content = f'''\
# run_RunClass.py
#
# Reads "{created_outputs_json_name}" in this folder to find "dynamic_class_creator_class",
# ensures RunClass.class exists here (copied from the executionAgent folder = parent of executorOutput),
# then runs:
#
#   java -cp <THIS_DIR and all jars> RunClass --class <dynamic_class_creator_class>
#
# Usage:
#   python run_RunClass.py
#
# Optional env:
#   JAVA_BIN=java

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
CREATED_JSON = HERE / "{created_outputs_json_name}"

# Assumption: executorOutput is a sibling of executionAgent directory,
# so HERE.parent points to the executionAgent folder.
SOURCE_RUNCLASS = (HERE.parent / "RunClass.class").resolve()
TARGET_RUNCLASS = HERE / "RunClass.class"


def read_payload():
    if not CREATED_JSON.exists():
        raise SystemExit(f"[ERR] missing {{CREATED_JSON}}")
    try:
        obj = json.loads(CREATED_JSON.read_text(encoding="utf-8"))
    except Exception as e:
        raise SystemExit(f"[ERR] invalid JSON in {{CREATED_JSON}}: {{e}}")
    if not isinstance(obj, dict):
        raise SystemExit(f"[ERR] expected dict JSON in {{CREATED_JSON}}")
    return obj


def ensure_runclass():
    if TARGET_RUNCLASS.exists():
        return
    if not SOURCE_RUNCLASS.exists():
        raise SystemExit(f"[ERR] RunClass.class not found at {{SOURCE_RUNCLASS}}")
    shutil.copy2(SOURCE_RUNCLASS, TARGET_RUNCLASS)
    print(f"[OK] Copied RunClass.class -> {{TARGET_RUNCLASS}}")


def normalize_class_arg(s: str) -> str:
    # Accept "WebcamAscii.class" or "WebcamAscii"
    s = s.strip()
    if s.lower().endswith(".class"):
        s = s[:-6]
    return s


def build_classpath() -> str:
    # Include THIS DIR plus all jars in this dir via wildcard.
    # Windows uses ';' separator, Unix uses ':'.
    sep = ";" if os.name == "nt" else ":"
    return sep.join([".", "./*"])


def main():
    ensure_runclass()

    payload = read_payload()
    dyn = payload.get("dynamic_class_creator_class")
    if not isinstance(dyn, str) or not dyn.strip():
        raise SystemExit("[ERR] dynamic_class_creator_class missing/null in created_outputs.json")

    target = normalize_class_arg(dyn)

    java_bin = os.getenv("JAVA_BIN", "java")
    cp = build_classpath()

    cmd = [java_bin, "-cp", cp, "RunClass", "--class", target]

    print("[RUN]", " ".join(cmd))
    res = subprocess.run(
        cmd,
        cwd=str(HERE),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    if res.stdout:
        print(res.stdout)
    if res.stderr:
        print(res.stderr, file=sys.stderr)

    raise SystemExit(res.returncode)


if __name__ == "__main__":
    main()
'''
    runner_path.write_text(content, encoding="utf-8")
    return runner_path



# ==========================================================
# Main
# ==========================================================

def main():
    # VERY BEGINNING: sync goal.txt from ../executorInput/goal.txt
    sync_goal_from_executor_input()

    # 0) INIT SQLITE SCHEMA FIRST
    if not run_and_record(INIT_DB_CMD, "INIT SQLITE SCHEMA"):
        print("\n[HALT] Database initialization failed.")
        return

    # 1) BOOTSTRAP
    if not bootstrap_bytebuddy():
        print("\n[HALT] Bootstrap failed.")
        return

    # 2) PRE-LOOP executor warm-up
    if not run_and_record(EXECUTOR_CMD, "PRE-LOOP SINGLE STEP EXECUTOR"):
        print("\n[HALT] Pre-loop executor run failed.")
        return

    iteration = 0
    halted_by_ready = False

    while True:
        if READY_FILE.exists():
            print(f"\n[HALT] Found .ready file: {READY_FILE}")
            halted_by_ready = True
            break

        iteration += 1
        print("\n" + "#" * 70)
        print(f"# STECHEN AUTORUN — ITERATION {iteration}")
        print("#" * 70)

        if not run_and_record(PLANNER_CMD, "PLANNER"):
            break

        if not run_and_record(EXECUTOR_CMD, "SINGLE STEP EXECUTOR"):
            break

        if READY_FILE.exists():
            print(f"\n[HALT] Found .ready file after execution: {READY_FILE}")
            halted_by_ready = True
            break

        time.sleep(SLEEP_BETWEEN_ITERATIONS_SEC)

    print("\n[HALT] STECHEN loop finished.")

    if not halted_by_ready:
        return

    # POST-HALT: run work_recorder.py
    print("\n==== POST-HALT: WORK RECORDER ====")
    res = subprocess.run(
        WORK_RECORDER_CMD,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if res.stdout:
        print(res.stdout)
    if res.stderr:
        print(res.stderr, file=sys.stderr)

    if res.returncode != 0:
        print(f"[WARN] work_recorder.py failed with exit code {res.returncode}")
        return

    print("[OK] work_recorder.py completed.")

    # COPY created outputs to ../executorOutput
    payload = load_created_outputs_payload(CREATED_OUTPUTS_JSON)
    created_outputs = payload.get("created_outputs", [])
    if not isinstance(created_outputs, list):
        created_outputs = []

    if not created_outputs:
        print(f"[executorOutput] No created outputs found in: {CREATED_OUTPUTS_JSON}")
        return

    print(f"[executorOutput] Found {len(created_outputs)} outputs in {CREATED_OUTPUTS_JSON}")
    copy_outputs_to_executor_output([x for x in created_outputs if isinstance(x, str)])

    # copy created_outputs.json into executorOutput for runner
    try:
        EXECUTOR_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        shutil.copy2(CREATED_OUTPUTS_JSON, EXECUTOR_OUTPUT_DIR / CREATED_OUTPUTS_JSON.name)
        print(f"[executorOutput] Copied: {CREATED_OUTPUTS_JSON.name}")
    except Exception as e:
        print(f"[executorOutput] WARN: could not copy created_outputs.json: {e}")

    # CREATE runner script in executorOutput
    runner_path = write_executor_runner_script(EXECUTOR_OUTPUT_DIR, created_outputs_json_name=CREATED_OUTPUTS_JSON.name)
    print(f"[executorOutput] Wrote runner: {runner_path}")


if __name__ == "__main__":
    main()
