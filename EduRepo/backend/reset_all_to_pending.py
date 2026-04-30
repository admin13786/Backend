from __future__ import annotations

import sqlite3
from pathlib import Path


def main() -> None:
    db_path = Path(__file__).resolve().parent / "data" / "edurepo.db"
    con = sqlite3.connect(db_path)
    cur = con.cursor()

    cur.execute("SELECT status, COUNT(1) FROM edurepo_articles GROUP BY status")
    print("before:", dict(cur.fetchall()))

    cur.execute(
        """
        UPDATE edurepo_articles SET
            status='pending',
            llm_attempts=0,
            llm_error='',
            ps_title='',
            ps_summary='',
            ps_markdown='',
            keywords_json='[]',
            highlights_json='[]',
            glossary_json='[]',
            processed_at=''
        WHERE raw_json IS NOT NULL AND LENGTH(TRIM(raw_json)) > 2
        """
    )
    print("updated rows:", cur.rowcount)
    con.commit()

    cur.execute("SELECT status, COUNT(1) FROM edurepo_articles GROUP BY status")
    print("after:", dict(cur.fetchall()))
    con.close()


if __name__ == "__main__":
    main()

