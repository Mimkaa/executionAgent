import sqlite3
import textwrap
from pathlib import Path
import sys

DB = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("stechen.db")

def fetch_rows(conn, limit=80, only_fail=False, run_id=None):
    where = []
    params = []

    if only_fail:
        where.append("status != 'SUCCESS'")
    if run_id:
        where.append("command LIKE ? OR message LIKE ?")
        params += [f"%{run_id}%", f"%{run_id}%"]

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f"""
    SELECT id, ts, event_type, status, command, message
    FROM pipelineLong
    {where_sql}
    ORDER BY id DESC
    LIMIT ?
    """
    params.append(limit)
    return conn.execute(sql, params).fetchall()

def print_table(rows, wrap=60):
    headers = ["id", "ts", "event_type", "status", "command", "message"]
    # fixed column widths (adjust as you like)
    widths = [6, 19, 12, 10, 40, wrap]

    def fmt_cell(s, w):
        if s is None:
            s = ""
        s = str(s).replace("\n", "\\n")
        return textwrap.wrap(s, w) or [""]

    # header
    line = " | ".join(h.ljust(w) for h, w in zip(headers, widths))
    sep = "-+-".join("-" * w for w in widths)
    print(line)
    print(sep)

    for r in rows:
        wrapped_cols = [fmt_cell(c, w) for c, w in zip(r, widths)]
        max_lines = max(len(c) for c in wrapped_cols)
        for i in range(max_lines):
            parts = []
            for col_lines, w in zip(wrapped_cols, widths):
                parts.append((col_lines[i] if i < len(col_lines) else "").ljust(w))
            print(" | ".join(parts))
        print(sep)

def main():
    if not DB.exists():
        print(f"[ERR] DB not found: {DB.resolve()}")
        sys.exit(1)

    limit = 80
    only_fail = "--fail" in sys.argv
    # optional: `--limit 200`
    if "--limit" in sys.argv:
        i = sys.argv.index("--limit")
        limit = int(sys.argv[i + 1])

    with sqlite3.connect(str(DB), timeout=30.0) as conn:
        rows = fetch_rows(conn, limit=limit, only_fail=only_fail)
        print_table(rows, wrap=80)

if __name__ == "__main__":
    main()
