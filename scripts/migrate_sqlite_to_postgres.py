from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

try:
    import psycopg
except ImportError as exc:  # pragma: no cover
    raise SystemExit("psycopg is required. Install dependencies from requirements.txt") from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate analyses from SQLite to PostgreSQL")
    parser.add_argument("--sqlite-path", default="analyses.db", help="Path to the source SQLite database")
    parser.add_argument("--database-url", default=None, help="PostgreSQL DATABASE_URL. Defaults to env DATABASE_URL")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    sqlite_path = Path(args.sqlite_path).resolve()
    if not sqlite_path.exists():
        raise SystemExit(f"SQLite file not found: {sqlite_path}")

    database_url = args.database_url
    if not database_url:
        import os

        database_url = os.getenv("DATABASE_URL", "").strip()
    if not database_url:
        raise SystemExit("DATABASE_URL is required")
    if database_url.startswith("postgres://"):
        database_url = "postgresql://" + database_url[len("postgres://") :]

    sqlite_conn = sqlite3.connect(sqlite_path)
    sqlite_conn.row_factory = sqlite3.Row

    with sqlite_conn, psycopg.connect(database_url) as pg_conn:
        pg_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS analyses (
                id TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )

        rows = sqlite_conn.execute(
            "SELECT id, payload, created_at, updated_at FROM analyses ORDER BY created_at ASC"
        ).fetchall()

        migrated = 0
        for row in rows:
            pg_conn.execute(
                """
                INSERT INTO analyses (id, payload, created_at, updated_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE
                SET payload = EXCLUDED.payload,
                    updated_at = EXCLUDED.updated_at
                """,
                (row["id"], row["payload"], row["created_at"], row["updated_at"]),
            )
            migrated += 1

    print(f"Migrated {migrated} analyses from {sqlite_path}")


if __name__ == "__main__":
    main()
