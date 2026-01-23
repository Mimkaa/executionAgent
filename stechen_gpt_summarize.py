# stechen_gpt_summarizer.py
#
# Class-based wrapper around your summarizer.
# Main entrypoint:
#   StechenGPTSummarizer(...).summarize(n=10, run_id=None)

import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

from openai import OpenAI


class StechenGPTSummarizer:
    DEFAULT_DB = "stechen.db"
    DEFAULT_RULES = "RULES.txt"
    DEFAULT_N = 10
    DEFAULT_MODEL = "gpt-5.2"

    MAX_RULES_CHARS = 80_000
    MAX_PAYLOAD_CHARS = 10_000
    MAX_VALUE_CHARS = 4_000

    def __init__(
        self,
        db_path: str = DEFAULT_DB,
        rules_file: str = DEFAULT_RULES,
        model: str = DEFAULT_MODEL,
        base_dir: Optional[Path] = None,
    ):
        self.base_dir = base_dir or Path(__file__).resolve().parent
        self.db_path = self._resolve_path(db_path)
        self.rules_file = self._resolve_path(rules_file)
        self.model = model

        self._client = OpenAI()

    # -------------------------
    # Public API
    # -------------------------
    def summarize(self, n: int, run_id: Optional[str] = None) -> str:
        """
        Summarize the last n pipeline steps.
        If run_id is provided, only summarize steps for that run_id.
        """
        rules_text = self._cap(self._read_text(self.rules_file), self.MAX_RULES_CHARS)

        if not self.db_path.exists():
            return f"2) WHAT HAPPENED\n- No database found at: {self.db_path}"

        steps_text = self._load_last_steps_text(db_path=self.db_path, n=n, run_id=run_id)

        # If steps_text already contains a properly formatted "no steps" message
        # we return it directly.
        if steps_text.startswith("2) WHAT HAPPENED\n- "):
            return steps_text

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

    # -------------------------
    # Internals
    # -------------------------
    def _resolve_path(self, p: str) -> Path:
        path = Path(p)
        if not path.is_absolute():
            path = (self.base_dir / path).resolve()
        return path

    def _cap(self, s: Optional[str], n: int) -> str:
        if s is None:
            return ""
        s = str(s)
        if len(s) <= n:
            return s
        return s[:n] + "\n... [truncated]"

    def _read_text(self, path: Path) -> str:
        if not path.exists():
            raise FileNotFoundError(f"Rules file not found: {path.resolve()}")
        return path.read_text(encoding="utf-8", errors="replace")

    def _safe_table_name(self, name: str) -> str:
        out = []
        for ch in name:
            if ch.isalnum() or ch == "_":
                out.append(ch)
        return "".join(out)

    def _fetch_tables(self, conn: sqlite3.Connection) -> List[str]:
        cur = conn.cursor()
        rows = cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        return [r[0] for r in rows]

    def _fetch_payload(self, conn: sqlite3.Connection, script_name: str, step_id: int) -> Dict[str, Any]:
        cur = conn.cursor()
        t = self._safe_table_name(script_name)

        exists = cur.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (t,),
        ).fetchone()
        if not exists:
            return {"_payload_error": f"Missing table for script: {t}"}

        row = cur.execute(f"SELECT * FROM {t} WHERE step_id=?", (step_id,)).fetchone()
        if not row:
            return {"_payload_error": f"No payload row in {t} for step_id={step_id}"}

        payload = dict(row)

        for k, v in list(payload.items()):
            if isinstance(v, (bytes, bytearray)):
                payload[k] = f"<{len(v)} bytes>"
            else:
                payload[k] = self._cap(v, self.MAX_VALUE_CHARS)

        return payload

    def _format_steps(self, steps: List[Dict[str, Any]]) -> str:
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
                    sig = (
                        f"--name {payload.get('file_name','')} "
                        f"(content_text_len={len(payload.get('content_text') or '')}, content_ref={payload.get('content_ref')})"
                    )

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
            chunks.append(self._cap(str(payload), self.MAX_PAYLOAD_CHARS) or "{}")
            chunks.append("----")

        return "\n".join(chunks)

    def _build_instructions(self) -> str:
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
            "- Prefer confident, rule-backed interpretation over 'unknown'.\n\n"
            "After the bullet-point summary, append the raw commands exactly as executed:\n"
            "- Print a literal line: 'RAN COMMANDS (VERBATIM):'\n"
            "- Then print each command from LAST_STEPS on its own line.\n"
            "- Preserve original ordering, spacing, flags, and base64 values.\n"
            "- Do NOT paraphrase, decode, or summarize the commands.\n"
            "- Do NOT add explanations between commands.\n"
        )


    def _load_last_steps_text(self, db_path: Path, n: int, run_id: Optional[str]) -> str:
        conn = sqlite3.connect(str(db_path), timeout=30.0)
        conn.row_factory = sqlite3.Row
        try:
            tables = self._fetch_tables(conn)
            if "pipeline" not in tables:
                return "2) WHAT HAPPENED\n- No 'pipeline' table found (schema not initialized)."

            cur = conn.cursor()

            if run_id is None:
                rows = cur.execute(
                    "SELECT step_id, script_name, created_at, run_id, step_index "
                    "FROM pipeline ORDER BY step_id DESC LIMIT ?",
                    (int(n),),
                ).fetchall()
            else:
                rows = cur.execute(
                    "SELECT step_id, script_name, created_at, run_id, step_index "
                    "FROM pipeline WHERE run_id=? ORDER BY step_id DESC LIMIT ?",
                    (run_id, int(n)),
                ).fetchall()

            if not rows:
                if run_id is None:
                    return "2) WHAT HAPPENED\n- No pipeline steps found."
                return f"2) WHAT HAPPENED\n- No pipeline steps found for run_id={run_id!r}."

            steps_newest_first: List[Dict[str, Any]] = []
            for r in rows:
                step_id = int(r["step_id"])
                script_name = str(r["script_name"])
                payload = self._fetch_payload(conn, script_name, step_id)

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
            return self._format_steps(steps_chrono)

        finally:
            conn.close()


# -------------------------
# Example usage
# -------------------------
if __name__ == "__main__":
    db_path = os.getenv("STECHEN_DB_PATH", "stechen.db")
    rules_file = os.getenv("STECHEN_RULES_FILE", "RULES.txt")
    model = os.getenv("STECHEN_MODEL", "gpt-5.2")

    summarizer = StechenGPTSummarizer(db_path=db_path, rules_file=rules_file, model=model)

    # summarize last N (default 10 if env not used)
    n = int(os.getenv("STECHEN_N", "10"))
    print(summarizer.summarize(n=n))
