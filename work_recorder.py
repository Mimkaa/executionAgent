import json
import os
from pathlib import Path
from typing import Any, Dict, Optional, Set

ALLOWED_EXTS = {".jar", ".txt", ".class"}


def load_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def norm(s: str) -> str:
    return (s or "").strip().lower()


def stem_no_ext(p: str) -> str:
    return Path(p).name.rsplit(".", 1)[0].lower()


def allowed_ext(p: str) -> bool:
    return Path(p).suffix.lower() in ALLOWED_EXTS


def main() -> None:
    conducted_dir = Path(os.getenv("CONDUCTED_DIR", "./conductedWork")).resolve()
    out_json = Path(os.getenv("OUT_JSON", "./created_outputs.json")).resolve()

    if not conducted_dir.exists():
        raise SystemExit(f"[ERR] conductedWork dir not found: {conducted_dir}")

    created_outputs: Set[str] = set()

    for wf in sorted(conducted_dir.glob("work_*.json")):
        rec = load_json(wf)
        if not rec:
            continue

        script_name = ""
        lp = rec.get("latest_pipeline_row")
        if isinstance(lp, dict):
            script_name = lp.get("script_name") or ""

        script_norm = norm(script_name)

        created = (
            rec.get("produced_files", {})
               .get("created", [])
        )

        if not isinstance(created, list):
            continue

        for rel_path in created:
            if not isinstance(rel_path, str):
                continue

            if not allowed_ext(rel_path):
                continue

            base = stem_no_ext(rel_path)

            if script_norm and base == script_norm:
                continue

            created_outputs.add(rel_path)

    out = {
        "created_outputs": sorted(created_outputs),
        "dynamic_class_creator_class": "Main.class",  # <- deterministic entry point
    }

    out_json.write_text(
        json.dumps(out, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print(f"[OK] Wrote summary to {out_json}")


if __name__ == "__main__":
    main()
