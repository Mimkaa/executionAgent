import sqlite3

conn = sqlite3.connect("stechen.db")
cur = conn.cursor()

cur.execute("""
    SELECT name
    FROM sqlite_master
    WHERE type='table'
      AND name NOT LIKE 'sqlite_%'
""")

for (table,) in cur.fetchall():
    print(f"\n=== {table} ===")

    cur.execute(f"PRAGMA table_info({table})")
    cols = [c[1] for c in cur.fetchall()]
    print("Columns:", cols)

    cur.execute(f"SELECT * FROM {table}")
    for row in cur.fetchall():
        print(dict(zip(cols, row)))

conn.close()
