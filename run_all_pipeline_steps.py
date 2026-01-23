# run_all_pipeline_steps.py
#
# Run ALL steps currently stored in pipeline table (ordered by step_id ASC),
# using DBOperator to fetch payload for each step and executing the corresponding
# Java tool fetched from GitHub (cached locally).
#
# ✅ Behavior:
# - Iterates through ALL pipeline records (oldest -> newest by step_id)
# - For each step:
#     - Build argv using B64 flags (Python encodes values, Java decodes)
#     - Download <Script>.java from GitHub if not cached
#     - Compile with jars in WORK_DIR on classpath
#     - Run java tool
#     - Log ONLY STEP + RESULT into pipelineLong using DBOperator.log_pipeline_long()
# - Every CHECKPOINT_EVERY steps:
#     - call the "current summarizer style" (2) WHAT HAPPENED using GPT)
#     - write to pipelineCheckpoints using DBOperator.insert_checkpoint()
# - DOES NOT delete or modify pipeline table (pure "replay")
#
# Usage:
#   python run_all_pipeline_steps.py
#
# Required:
#   pip install openai
#   set OPENAI_API_KEY
#
# Optional env:
#   STECHEN_DB_PATH, STECHEN_WORK_DIR, STECHEN_GITHUB_BASE_RAW
#   STECHEN_RULES_FILE (default RULES.txt)
#   STECHEN_MODEL (default gpt-5.2)
#   STECHEN_CHECKPOINT_EVERY (default 50)
#
import os
import sys
import time
import base64
import subprocess
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

from openai import OpenAI

from db_operator import DBOperator  # must include: log_pipeline_long, insert_checkpoint, ensure_runtime_tables

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


# -----------------------------
# CONFIG
# -----------------------------
DB_PATH = os.getenv("STECHEN_DB_PATH", "stechen.db")
WORK_DIR = Path(os.getenv("STECHEN_WORK_DIR", ".")).resolve()
GITHUB_BASE_RAW = os.getenv(
    "STECHEN_GITHUB_BASE_RAW",
    "https://raw.githubusercontent.com/Mimkaa/ObjectSchemas/main"
)

RULES_FILE = os.getenv("STECHEN_RULES_FILE", "RULES.txt")
MODEL = os.getenv("STECHEN_MODEL", "gpt-5.2")
CHECKPOINT_EVERY = int(os.getenv("STECHEN_CHECKPOINT_EVERY", "50"))

JAVA_CMD = "java"
JAVAC_CMD = "javac"
CLASSPATH_SEP = ";" if os.name == "nt" else ":"


# -----------------------------
# Console
# -----------------------------
def log(*args):
    try:
        print(*args)
    except Exception:
        pass


def _read_text(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")


def _cap(s: Optional[str], n: int) -> str:
    if s is None:
        return ""
    s = str(s)
    return s if len(s) <= n else s[:n] + "\n... [truncated]"


# =========================================================
# Runner helpers
# =========================================================
def b64_utf8(s: str) -> str:
    return base64.b64encode((s or "").encode("utf-8", errors="replace")).decode("ascii")


def build_classpath() -> str:
    jars = [str(p) for p in WORK_DIR.glob("*.jar")]
    return CLASSPATH_SEP.join([str(WORK_DIR)] + jars)


def download_java(script_name: str) -> Path:
    java_filename = f"{script_name}.java"
    local_path = WORK_DIR / java_filename
    if local_path.exists():
        return local_path

    url = f"{GITHUB_BASE_RAW}/{java_filename}"
    log(f"[DL] {java_filename} <- {url}")
    try:
        with urllib.request.urlopen(url) as resp, open(local_path, "wb") as out:
            out.write(resp.read())
    except Exception as e:
        raise FileNotFoundError(f"Failed to download {java_filename}: {e}")

    return local_path


def compile_java(script_name: str) -> None:
    java_filename = f"{script_name}.java"
    src_path = WORK_DIR / java_filename
    if not src_path.exists():
        raise FileNotFoundError(f"{java_filename} not found in {WORK_DIR}")

    cp = build_classpath()
    cmd = [JAVAC_CMD, "-cp", cp, java_filename]

    res = subprocess.run(
        cmd,
        cwd=str(WORK_DIR),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if res.returncode != 0:
        combined = (res.stdout or "") + ("\n" if res.stdout and res.stderr else "") + (res.stderr or "")
        raise RuntimeError(f"[JAVAC] failed for {java_filename}\n{combined}")


def run_java(script_name: str, argv: List[str]) -> str:
    cp = build_classpath()
    cmd = [JAVA_CMD, "-cp", cp, script_name] + argv

    log("[JAVA] Running:", " ".join(cmd))
    res = subprocess.run(
        cmd,
        cwd=str(WORK_DIR),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    combined = (res.stdout or "") + ("\n" if res.stdout and res.stderr else "") + (res.stderr or "")
    if res.returncode != 0:
        raise RuntimeError(f"[JAVA] {script_name} exited with {res.returncode}\n{combined}")

    return combined


# =========================================================
# Convert DB step -> CLI argv (B64 flags)
# =========================================================
def build_argv_for_step(step: Dict[str, Any]) -> Tuple[str, List[str], str]:
    script = step["script_name"]
    payload = step.get("payload") or {}

    step_id = step["step_id"]
    run_id = step.get("run_id")
    step_index = step.get("step_index")
    prefix = f"[step_id={step_id} run_id={run_id} step_index={step_index}]"

    mapping: Dict[str, List[Tuple[str, str]]] = {
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

    argv: List[str] = []

    if script in mapping:
        for col, flag in mapping[script]:
            v = payload.get(col)
            if v is None or str(v).strip() == "":
                continue
            argv += [flag, b64_utf8(str(v))]
    else:
        # fallback: --<col>B64 for every payload key (except step_id)
        for k, v in payload.items():
            if k in ("step_id",) or v is None:
                continue
            argv += [f"--{k}B64", b64_utf8(str(v))]

    # CreateTextFileFromBase64: already base64 -> pass raw as --contentB64
    if script == "CreateTextFileFromBase64":
        v = payload.get("content_b64")
        if v:
            argv += ["--contentB64", str(v)]

    # CreateTextFile: resolve content_ref -> _resolved_content -> content_text
    if script == "CreateTextFile":
        text = None
        if payload.get("_resolved_content") is not None:
            text = payload.get("_resolved_content")
        elif payload.get("content_text"):
            text = payload.get("content_text")

        if text is not None:
            argv += ["--contentB64", b64_utf8(str(text))]

    human_cmd = f"{prefix} java {script} " + " ".join(argv)
    return script, argv, human_cmd.strip()


# =========================================================
# GPT summarizer (CURRENT summarizer style: ONLY "2) WHAT HAPPENED")
# This summarizes the *last N pipeline steps* from the DB (new schema).
# =========================================================
MAX_RULES_CHARS = 80_000
MAX_EVIDENCE_CHARS = 25_000
MAX_PAYLOAD_CHARS = 10_000
MAX_VALUE_CHARS = 4_000
MAX_SUMMARY_CHARS = 15_000


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

        sig = ""
        try:
            if script == "DynamicJarLoader":
                sig = f"--library {payload.get('library','')}"
            elif script == "DynamicClassCreator":
                sig = f"--name {payload.get('class_name','')}"
            elif script == "CreateDirectory":
                sig = f"--name {payload.get('directory_name','')} --path {payload.get('target_path','')}"
            elif script == "CurrentDirUpdate":
                sig = f"--dirname {payload.get('dirname','')}"
            elif script == "CreateTextFile":
                sig = f"--name {payload.get('file_name','')} (content_ref={payload.get('content_ref')})"
            elif script == "CreateTextFileFromBase64":
                sig = f"--name {payload.get('file_name','')} (content_b64_len={len(payload.get('content_b64') or '')})"
            elif script == "DynamicDelegateCreator":
                sig = (
                    f"--parent {payload.get('parent','')} "
                    f"--methodFile {payload.get('method_file','')} "
                    f"--fieldFile {payload.get('field_file','')} "
                    f"--outputDir {payload.get('output_dir','')}"
                )
            elif script == "ClassMethodCloner":
                sig = (
                    f"--classNameToModify {payload.get('class_name_to_modify','')} "
                    f"--delegateclass {payload.get('delegate_class','')} "
                    f"--method {payload.get('method_name','')}"
                )
            elif script == "ClassFieldCloner":
                sig = (
                    f"--classNameToModify {payload.get('class_name_to_modify','')} "
                    f"--delegateclass {payload.get('delegate_class','')} "
                    f"--field {payload.get('field_name','')}"
                )
            elif script == "RunClass":
                sig = f"--class {payload.get('class_name','')} --args {payload.get('args_text','')}"
        except Exception:
            sig = ""

        if sig:
            chunks.append(f"SIG: {sig}")

        chunks.append("PAYLOAD:")
        chunks.append(_cap(str(payload), MAX_PAYLOAD_CHARS) or "{}")
        chunks.append("----")

    return "\n".join(chunks)


def _summarizer_instructions() -> str:
    return (
        "You are a STECHEN pipeline summarizer.\n"
        "You will be given:\n"
        "(A) STECHEN_SYSTEM_RULES (authoritative)\n"
        "(B) LAST_STEPS (the last N recorded steps)\n\n"
        "Your job is to infer what is happening RIGHT NOW.\n"
        "Important: This system is rule-driven. Use the rules to interpret intent.\n\n"
        "Inference rules you MUST apply:\n"
        "- Recognize STANDARD METHOD CONSTRUCTION: CreateTextFile -> DynamicDelegateCreator -> ClassMethodCloner.\n"
        "  If you see that pattern, assume we are 'adding method <methodName>' to the base class.\n"
        "- Recognize STANDARD FIELD CONSTRUCTION: (delegate creation + ClassFieldCloner).\n"
        "- If RunClass appears, assume we are executing the accumulated base class now.\n"
        "- If method/field source is missing (content stored by reference), still infer purpose from filenames and method_name.\n"
        "- You MAY reasonably assume earlier prerequisite steps were done if the current steps depend on them.\n"
        "  Only say 'unknown' if it changes what the next action should be.\n\n"
        "Output rules (HARD):\n"
        "- Output ONLY one heading: exactly '2) WHAT HAPPENED'\n"
        "- Then bullet points.\n"
        "- No other headings, no extra commentary.\n"
        "- Prefer confident, rule-backed interpretation over 'unknown'.\n"
    )


def summarize_last_steps_like_current(
    op: DBOperator,
    rules_text: str,
    n_steps: int,
    model: str,
) -> str:
    # pull last n_steps from pipeline via SQL to keep op minimal
    cur = op.conn.cursor()
    rows = cur.execute(
        "SELECT step_id FROM pipeline ORDER BY step_id DESC LIMIT ?",
        (int(max(1, n_steps)),),
    ).fetchall()
    if not rows:
        return "2) WHAT HAPPENED\n- No pipeline steps found."

    steps = [op.get_step(int(r["step_id"])) for r in rows]
    steps.reverse()  # chronological
    evidence = _cap(_format_steps_for_inference(steps), MAX_EVIDENCE_CHARS)

    inp = (
        "STECHEN_SYSTEM_RULES:\n"
        f"{_cap(rules_text, MAX_RULES_CHARS)}\n\n"
        "LAST_STEPS:\n"
        f"{evidence}\n"
    )

    client = OpenAI()
    resp = client.responses.create(
        model=model,
        instructions=_summarizer_instructions(),
        input=inp,
        reasoning={"effort": "low"},
    )
    out = (resp.output_text or "").strip()
    if not out.startswith("2) WHAT HAPPENED"):
        out = "2) WHAT HAPPENED\n" + out
    return _cap(out, MAX_SUMMARY_CHARS)


# =========================================================
# Main (run all + operator logging + checkpoints)
# =========================================================
def main():
    log("[RUN] Running ALL pipeline steps (one-shot replay), ordered by step_id ASC.")
    log(f"[RUN] DB_PATH:  {Path(DB_PATH).resolve()}")
    log(f"[RUN] WORK_DIR: {WORK_DIR}")
    log(f"[RUN] GITHUB:   {GITHUB_BASE_RAW}")
    log("[RUN] NOTE: pipeline table is NOT modified (nothing deleted).")
    log(f"[RUN] CHECKPOINT_EVERY = {CHECKPOINT_EVERY}")

    rules_text = _read_text(Path(RULES_FILE).resolve())

    op = DBOperator(DB_PATH)
    op.ensure_runtime_tables()  # pipelineLong + checkpoints exist

    try:
        # Get all step_ids (oldest -> newest)
        cur = op.conn.cursor()
        rows = cur.execute("SELECT step_id FROM pipeline ORDER BY step_id ASC").fetchall()
        if not rows:
            log("[RUN] No pipeline steps found.")
            return

        step_ids = [int(r["step_id"]) for r in rows]
        log(f"[RUN] Found {len(step_ids)} steps.")

        first_step_id_overall = step_ids[0]
        executed_since_checkpoint = 0
        last_executed_step_id = None
        last_run_id = None

        for idx, step_id in enumerate(step_ids, start=1):
            human_cmd = "<unknown>"
            try:
                step = op.get_step(step_id)
                script, argv, human_cmd = build_argv_for_step(step)

                last_run_id = step.get("run_id") or last_run_id
                last_executed_step_id = step_id

                log("")
                log("=" * 60)
                log(f"[RUN] ({idx}/{len(step_ids)}) step_id={step_id} script={script}")
                log("=" * 60)

                # STEP log (DB)
                op.log_pipeline_long("STEP", "RUN", human_cmd, "Starting execution")

                # Download + compile + run
                download_java(script)
                compile_java(script)
                out = run_java(script, argv)

                # RESULT log (DB)
                msg = out.strip() if out.strip() else "Executed successfully"
                op.log_pipeline_long("RESULT", "SUCCESS", human_cmd, msg)

                executed_since_checkpoint += 1

                # --- CHECKPOINT every N executed steps ---
                if CHECKPOINT_EVERY > 0 and executed_since_checkpoint >= CHECKPOINT_EVERY:
                    # summarize last CHECKPOINT_EVERY pipeline steps (or fewer if DB smaller)
                    summary = summarize_last_steps_like_current(
                        op=op,
                        rules_text=rules_text,
                        n_steps=CHECKPOINT_EVERY,
                        model=MODEL,
                    )

                    # Choose range: last CHECKPOINT_EVERY steps by step_id among executed
                    # (approx: step_id_to = current step_id, step_id_from = step_ids[idx-CHECKPOINT_EVERY])
                    from_idx = max(0, idx - CHECKPOINT_EVERY)
                    step_id_from = int(step_ids[from_idx])
                    step_id_to = int(step_id)

                    op.insert_checkpoint(
                        run_id=last_run_id,
                        step_id_from=step_id_from,
                        step_id_to=step_id_to,
                        n_steps=CHECKPOINT_EVERY,
                        summary=summary,
                        state_json=None,
                    )

                    log("[CHECKPOINT] wrote pipelineCheckpoints:",
                        f"step_id_from={step_id_from}",
                        f"step_id_to={step_id_to}",
                        f"n_steps={CHECKPOINT_EVERY}")

                    executed_since_checkpoint = 0

            except Exception as e:
                # RESULT fail (DB)
                try:
                    op.log_pipeline_long("RESULT", "FAIL", human_cmd, str(e))
                except Exception:
                    pass
                log("[FAIL]", e)
                # Stop at first failure (safer for STECHEN)
                raise

        # Optional: final checkpoint for leftover (<50) at end
        if CHECKPOINT_EVERY > 0 and executed_since_checkpoint > 0 and last_executed_step_id is not None:
            n = executed_since_checkpoint
            summary = summarize_last_steps_like_current(
                op=op,
                rules_text=rules_text,
                n_steps=n,
                model=MODEL,
            )
            # range for leftover
            end_idx = len(step_ids) - 1
            start_idx = max(0, len(step_ids) - n)
            step_id_from = int(step_ids[start_idx])
            step_id_to = int(step_ids[end_idx])

            op.insert_checkpoint(
                run_id=last_run_id,
                step_id_from=step_id_from,
                step_id_to=step_id_to,
                n_steps=n,
                summary=summary,
                state_json=None,
            )
            log("[CHECKPOINT] wrote final pipelineCheckpoints:",
                f"step_id_from={step_id_from}",
                f"step_id_to={step_id_to}",
                f"n_steps={n}")

        log("")
        log("[OK] Finished running all pipeline steps.")

    finally:
        op.close()


if __name__ == "__main__":
    main()
