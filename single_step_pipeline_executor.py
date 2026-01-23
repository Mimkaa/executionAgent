# single_step_pipeline_executor.py
#
# Runs EXACTLY ONE pending pipeline step.
#
# FIXED BEHAVIOR:
# - "pending step" = earliest step_id that has NO RESULT row yet (SUCCESS OR FAIL)
#   => prevents infinite retry loop on a failing step
#
# FIX (CRITICAL):
# - CreateTextFile MUST actually receive content:
#     - if content_text exists -> pass --contentB64
#     - if content_ref exists  -> load from payload_store and pass --contentB64
#   Otherwise your .txt files are empty and downstream cloning fails.
#
# EXTRA:
# - Hard forbid CreateTextFileFromBase64 in executor (per your RULES).
#
# NEW (ANTI-DEADLOCK):
# - Each Java tool runs in its own process with a hard timeout (default 3s).
# - If it exceeds timeout:
#     -> create .ready file in WORK_DIR
#     -> kill the process
#     -> treat as SUCCESS (so autoloop can stop cleanly when it sees .ready)
#

import os
import sys
import base64
import subprocess
import urllib.request
import traceback
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

from db_operator import DBOperator
from stechen_checkpoint_maker import make_checkpoints


# =========================================================
# Encoding safety
# =========================================================
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


# =========================================================
# CONFIG
# =========================================================
DB_PATH = os.getenv("STECHEN_DB_PATH", "stechen.db")
WORK_DIR = Path(os.getenv("STECHEN_WORK_DIR", ".")).resolve()
GITHUB_BASE_RAW = os.getenv(
    "STECHEN_GITHUB_BASE_RAW",
    "https://raw.githubusercontent.com/Mimkaa/ObjectSchemas/main"
)

RULES_FILE = Path(os.getenv("STECHEN_RULES_FILE", "RULES.txt")).resolve()
MODEL = os.getenv("STECHEN_MODEL", "gpt-5.2")

CHECKPOINT_EVERY = int(os.getenv("STECHEN_CHECKPOINT_EVERY", "50"))
CHECKPOINT_K = int(os.getenv("STECHEN_CHECKPOINT_K", "50"))
RUN_ID_FILTER = os.getenv("STECHEN_RUN_ID_FILTER", None)

JAVA_CMD = "java"
JAVAC_CMD = "javac"
CLASSPATH_SEP = ";" if os.name == "nt" else ":"

PRINT_SUCCESS = os.getenv("STECHEN_PRINT_SUCCESS", "0").strip() in ("1", "true", "True", "YES", "yes")

# NEW: step timeout (seconds). If exceeded -> create .ready + kill.
STEP_TIMEOUT_SEC = float(os.getenv("STECHEN_STEP_TIMEOUT_SEC", "7"))


# =========================================================
# UTIL
# =========================================================
def log(*args):
    print(*args)


def resolve_path_like_planner(p: str) -> Path:
    """
    Resolve relative paths relative to the directory of THIS script.
    (Matches planner.py behavior.)
    """
    base_dir = Path(__file__).resolve().parent
    pp = Path(p)
    if not pp.is_absolute():
        pp = (base_dir / pp).resolve()
    return pp


def b64_utf8(s: str) -> str:
    return base64.b64encode((s or "").encode("utf-8", errors="replace")).decode("ascii")


def combine_out(res: subprocess.CompletedProcess) -> str:
    return (res.stdout or "") + ("\n" if res.stdout and res.stderr else "") + (res.stderr or "")


def format_exc(e: BaseException) -> str:
    return "".join(traceback.format_exception(type(e), e, e.__traceback__))


def build_classpath() -> str:
    jars = [str(p) for p in WORK_DIR.glob("*.jar")]
    return CLASSPATH_SEP.join([str(WORK_DIR)] + jars)


def print_block(title: str, text: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("-" * 80)
    print(text if text else "<empty>")
    print("=" * 80 + "\n")


def create_ready_file() -> Path:
    """
    Create .ready in WORK_DIR to signal the supervisor (autoloop) to halt.
    """
    p = WORK_DIR / ".ready"
    try:
        p.write_text("READY\n", encoding="utf-8", errors="replace")
    except Exception:
        try:
            with open(p, "w", encoding="utf-8", errors="replace") as f:
                f.write("READY\n")
        except Exception:
            pass
    return p


# =========================================================
# SEMANTIC FAILURE DETECTION (ALL TOOLS)
# =========================================================
FAIL_PATTERNS = [
    r"Exception in thread",
    r"^\s*at\s+\S+\.\S+\(",
    r"\bjava\.lang\.[A-Za-z]+Exception\b",
    r"\bjava\.lang\.[A-Za-z]+Error\b",
    r"\bIllegalStateException\b",
    r"\bNoSuchFileException\b",
    r"\bNoClassDefFoundError\b",
    r"\bClassNotFoundException\b",
    r"\bUnsatisfiedLinkError\b",
    r"\bLinkageError\b",
    r"Error: Unable to initialize main class",
]

SEMANTIC_ALLOWLIST = {
    # add here only if you *know* a tool prints exception-like text on success
}


def looks_like_java_failure(output: str) -> bool:
    if not output:
        return False
    for p in FAIL_PATTERNS:
        if re.search(p, output, flags=re.MULTILINE):
            return True
    return False


# =========================================================
# JAVA TOOL HANDLING
# =========================================================
def download_java(script_name: str) -> None:
    java_file = WORK_DIR / f"{script_name}.java"
    if java_file.exists():
        return
    url = f"{GITHUB_BASE_RAW}/{script_name}.java"
    log(f"[DL] {url}")
    with urllib.request.urlopen(url) as r, open(java_file, "wb") as f:
        f.write(r.read())


def compile_java(script_name: str) -> None:
    cp = build_classpath()
    cmd = [JAVAC_CMD, "-cp", cp, f"{script_name}.java"]
    res = subprocess.run(
        cmd,
        cwd=str(WORK_DIR),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if res.returncode != 0:
        raise RuntimeError(combine_out(res))


def run_java(script_name: str, argv: List[str]) -> Tuple[str, bool]:
    """
    Runs a Java tool in its own process.
    Waits up to STEP_TIMEOUT_SEC. If timeout:
      - writes .ready in WORK_DIR
      - kills the process
      - returns a SUCCESS-style message (semantic_fail=False)
    """
    cp = build_classpath()
    cmd = [JAVA_CMD, "-cp", cp, script_name] + argv
    log("[JAVA]", " ".join(cmd))

    p = subprocess.Popen(
        cmd,
        cwd=str(WORK_DIR),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    try:
        stdout, stderr = p.communicate(timeout=STEP_TIMEOUT_SEC)
        combined = (stdout or "") + ("\n" if stdout and stderr else "") + (stderr or "")

        if p.returncode != 0:
            raise RuntimeError(combined)

        if script_name not in SEMANTIC_ALLOWLIST and looks_like_java_failure(combined):
            return combined, True

        return combined, False

    except subprocess.TimeoutExpired:
        ready_path = create_ready_file()

        # Try to terminate gracefully
        try:
            p.terminate()
        except Exception:
            pass

        # Small grace window, then hard kill if needed
        try:
            stdout, stderr = p.communicate(timeout=0.5)
        except Exception:
            stdout, stderr = "", ""

        if p.poll() is None:
            try:
                p.kill()
            except Exception:
                pass

        msg = (
            f"[TIMEOUT] Step exceeded {STEP_TIMEOUT_SEC:.1f}s. "
            f"Created ready signal: {ready_path}. "
            f"Killed pid={p.pid}.\n"
        )
        combined = (stdout or "") + ("\n" if stdout and stderr else "") + (stderr or "")
        if combined.strip():
            msg += "\n=== PARTIAL CHILD OUTPUT ===\n" + combined

        return msg, False


# =========================================================
# CreateTextFile content resolution (FIX)
# =========================================================
def load_payload_store_text(op: DBOperator, payload_id: str) -> str:
    row = op.conn.execute(
        "SELECT text_content FROM payload_store WHERE payload_id=?",
        (payload_id,),
    ).fetchone()
    if not row:
        raise RuntimeError(f"payload_store missing payload_id={payload_id!r}")
    return str(row["text_content"] or "")


# =========================================================
# STEP -> ARGV (FIXED)
# =========================================================
def build_argv_for_step(op: DBOperator, step: Dict[str, Any]) -> Tuple[str, List[str], str, Dict[str, str]]:
    """
    Returns:
      script, argv, human_cmd, debug_decoded (useful decoded args)
    """
    script = step["script_name"]
    payload = step.get("payload") or {}

    prefix = f"[step_id={step['step_id']} run_id={step.get('run_id')} step_index={step.get('step_index')}]"

    # Hard forbid here too (matches your RULES)
    if script == "CreateTextFileFromBase64":
        raise RuntimeError("FORBIDDEN STEP: CreateTextFileFromBase64 must not be executed.")

    mapping = {
        "DynamicJarLoader": [("library", "--libraryB64")],
        "DynamicClassCreator": [("class_name", "--nameB64")],
        "CreateDirectory": [("directory_name", "--nameB64"), ("target_path", "--pathB64")],
        "CurrentDirUpdate": [("dirname", "--dirnameB64")],
        "DynamicDelegateCreator": [
            ("parent", "--parentB64"),
            ("method_file", "--methodFileB64"),
            ("field_file", "--fieldFileB64"),
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
        "RunClass": [("class_name", "--classB64"), ("args_text", "--argsB64")],
        # If you later add CreateReadyFile tool with no args, it will just have no mapping and run with empty argv.
        # "CreateReadyFile": [],
    }

    argv: List[str] = []
    decoded: Dict[str, str] = {}

    # Normal mapped args
    for col, flag in mapping.get(script, []):
        if payload.get(col) is None:
            continue
        v = str(payload[col])
        argv += [flag, b64_utf8(v)]
        if flag in ("--methodFileB64", "--fieldFileB64", "--nameB64", "--pathB64", "--outputDirB64"):
            decoded[flag] = v

    # SPECIAL CASE: CreateTextFile (FIX: forward content)
    if script == "CreateTextFile":
        file_name = str(payload.get("file_name") or "")
        target_path = payload.get("target_path")
        content_text = payload.get("content_text")
        content_ref = payload.get("content_ref")

        if not file_name.strip():
            raise RuntimeError("CreateTextFile payload missing file_name")

        argv += ["--nameB64", b64_utf8(file_name)]

        if target_path is not None and str(target_path).strip():
            argv += ["--pathB64", b64_utf8(str(target_path))]

        # Resolve content
        content_final = ""
        source = ""
        if content_text is not None and str(content_text).strip():
            content_final = str(content_text)
            source = "content_text"
        elif content_ref is not None and str(content_ref).strip():
            content_final = load_payload_store_text(op, str(content_ref))
            source = f"payload_store:{content_ref}"
        else:
            # This is exactly the bug you hit. Don't allow empty files silently.
            raise RuntimeError("CreateTextFile has NO content_text and NO content_ref (would create empty file).")

        argv += ["--contentB64", b64_utf8(content_final)]
        decoded["CreateTextFile.content_source"] = source
        decoded["CreateTextFile.content_len"] = str(len(content_final))

    human_cmd = f"{prefix} java {script} " + " ".join(argv)
    return script, argv, human_cmd, decoded


# =========================================================
# PIPELINE SELECTION
# =========================================================
def step_has_any_result(op: DBOperator, sid: int) -> bool:
    row = op.conn.execute(
        "SELECT 1 FROM pipelineLong "
        "WHERE event_type='RESULT' AND command LIKE ? "
        "LIMIT 1",
        (f"%[step_id={sid} %",),
    ).fetchone()
    return bool(row)


def get_next_pending_step_id(op: DBOperator) -> Optional[int]:
    step_rows = op.conn.execute("SELECT step_id FROM pipeline ORDER BY step_id").fetchall()
    for r in step_rows:
        sid = int(r["step_id"])
        if not step_has_any_result(op, sid):
            return sid
    return None


def count_total_success_results(op: DBOperator) -> int:
    row = op.conn.execute(
        "SELECT COUNT(*) AS c FROM pipelineLong WHERE event_type='RESULT' AND status='SUCCESS'"
    ).fetchone()
    return int(row["c"] or 0)


# =========================================================
# EXECUTE ONE STEP
# =========================================================
def _exists_variants(name: str) -> Dict[str, bool]:
    if not name:
        return {}
    p1 = WORK_DIR / name
    p2 = WORK_DIR / (name + ".txt") if not name.lower().endswith(".txt") else None
    out = {str(p1): p1.exists()}
    if p2 is not None:
        out[str(p2)] = p2.exists()
    return out


def execute_one_step_and_log(op: DBOperator, step: Dict[str, Any]) -> bool:
    script, argv, human_cmd, decoded = build_argv_for_step(op, step)

    op.log_pipeline_long("STEP", "RUN", human_cmd, "Starting execution")

    try:
        download_java(script)
        compile_java(script)

        out, semantic_fail = run_java(script, argv)

        if semantic_fail:
            msg = "[JAVA SEMANTIC FAIL]\n\n=== FULL CHILD OUTPUT ===\n" + (out or "")
            op.log_pipeline_long("RESULT", "FAIL", human_cmd, msg)
            print_block("SEMANTIC FAIL (printed + logged to DB)", msg)
            return False

        op.log_pipeline_long("RESULT", "SUCCESS", human_cmd, out or "OK")

        if PRINT_SUCCESS and (out or "").strip():
            print_block("SUCCESS OUTPUT", out)

        return True

    except Exception as e:
        extra = ""

        if script == "DynamicDelegateCreator":
            mf = decoded.get("--methodFileB64", "")
            ff = decoded.get("--fieldFileB64", "")
            extra += (
                "\n\n--- DEBUG (DynamicDelegateCreator) ---\n"
                f"WORK_DIR: {WORK_DIR}\n"
                f"method_file(raw): {mf!r}\n"
                f"field_file(raw) : {ff!r}\n"
                f"method_exists_variants: { _exists_variants(mf) if mf else '<none>' }\n"
                f"field_exists_variants : { _exists_variants(ff) if ff else '<none>' }\n"
            )

        if script == "CreateTextFile":
            extra += (
                "\n\n--- DEBUG (CreateTextFile) ---\n"
                f"content_source: {decoded.get('CreateTextFile.content_source')}\n"
                f"content_len   : {decoded.get('CreateTextFile.content_len')}\n"
            )

        msg = (
            "[HARD FAIL]\n\n"
            + str(e)
            + extra
            + "\n\n--- PYTHON TRACEBACK ---\n"
            + format_exc(e)
        )

        op.log_pipeline_long("RESULT", "FAIL", human_cmd, msg)
        print_block("HARD FAIL (printed + logged to DB)", msg)
        return False


# =========================================================
# MAIN
# =========================================================
def main():
    log("[RUN] single-step executor starting")

    db_abs = resolve_path_like_planner(DB_PATH)
    log(f"[RUN] DB_PATH (resolved): {db_abs}")

    WORK_DIR.mkdir(parents=True, exist_ok=True)

    op = DBOperator(str(db_abs))
    op.ensure_runtime_tables()

    try:
        row = op.conn.execute("SELECT COUNT(*) AS c FROM pipeline").fetchone()
        row2 = op.conn.execute("SELECT MAX(step_id) AS m FROM pipeline").fetchone()
        log(f"[RUN] pipeline rows: {int(row['c'] or 0)}  max(step_id): {row2['m']}")

        step_id = get_next_pending_step_id(op)
        if step_id is None:
            log("[RUN] No pending steps.")
            return

        step = op.get_step(step_id)
        execute_one_step_and_log(op, step)

        if CHECKPOINT_EVERY > 0:
            total = count_total_success_results(op)
            if total > 0 and (total % CHECKPOINT_EVERY == 0):
                log(f"[RUN] CHECKPOINT trigger: total_success={total} (every {CHECKPOINT_EVERY})")
                make_checkpoints(
                    db_path=db_abs,
                    rules_file=RULES_FILE,
                    model=MODEL,
                    k=CHECKPOINT_K,
                    run_id=RUN_ID_FILTER,
                )

    finally:
        op.close()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
    sys.exit(0)
