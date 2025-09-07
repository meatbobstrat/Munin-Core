import sqlite3
from pathlib import Path

db_path = Path(r"D:\Dev\Munin-Core\Munin-Core\db\munin.sqlite")

con = sqlite3.connect(db_path)
cur = con.cursor()

print("\n== file_manifest (last 10) ==")
for row in cur.execute(
    "SELECT id, rel_path, status, started_at_utc, ingested_at_utc "
    "FROM file_manifest ORDER BY id DESC LIMIT 10;"
):
    print(row)

print("\n== event counts by file ==")
for row in cur.execute(
    "SELECT file_id, COUNT(*) "
    "FROM event_occurrence GROUP BY file_id "
    "ORDER BY file_id DESC LIMIT 10;"
):
    print(row)

con.close()
