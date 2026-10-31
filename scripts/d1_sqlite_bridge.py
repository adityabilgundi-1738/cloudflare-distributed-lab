from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path


def main() -> None:
    root = Path(__file__).resolve().parent.parent
    db_path = root / "local_d1.db"
    schema_path = root / "schema.sql"

    conn = sqlite3.connect(str(db_path), isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.executescript(schema_path.read_text(encoding="utf-8"))

    for raw_line in sys.stdin:
        line = raw_line.strip()
        if not line:
            continue
        req = json.loads(line)
        req_id = req["id"]
        op = req["op"]
        sql = req["sql"]
        params = req.get("params", [])

        try:
            cur = conn.execute(sql, params)
            if op == "all":
                rows = [dict(r) for r in cur.fetchall()]
                resp = {"id": req_id, "ok": True, "result": {"results": rows}}
            elif op == "first":
                row = cur.fetchone()
                resp = {
                    "id": req_id,
                    "ok": True,
                    "result": dict(row) if row is not None else None,
                }
            else:
                resp = {
                    "id": req_id,
                    "ok": True,
                    "result": {
                        "success": True,
                        "meta": {
                            "changes": cur.rowcount,
                            "last_row_id": cur.lastrowid,
                        },
                    },
                }
        except Exception as exc:
            resp = {"id": req_id, "ok": False, "error": str(exc)}

        sys.stdout.write(json.dumps(resp) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()
