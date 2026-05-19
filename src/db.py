"""
SQLite persistence for the bot.
Minimal: only tracks the active session (session_id + directory).
All project/session data comes from the OpenCode API natively.
"""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "bot.db"


def _conn() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def init():
    with _conn() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS active_session (
                id          INTEGER PRIMARY KEY CHECK (id = 1),
                session_id  TEXT NOT NULL,
                directory   TEXT NOT NULL
            )
        """)
        # Migrate old schema: add directory column if missing
        try:
            con.execute("ALTER TABLE active_session ADD COLUMN directory TEXT NOT NULL DEFAULT ''")
        except Exception:
            pass


def get_active() -> dict | None:
    """Return {session_id, directory} or None."""
    with _conn() as con:
        row = con.execute("SELECT session_id, directory FROM active_session WHERE id = 1").fetchone()
        return dict(row) if row else None


def set_active(session_id: str, directory: str):
    with _conn() as con:
        con.execute("""
            INSERT INTO active_session (id, session_id, directory)
            VALUES (1, ?, ?)
            ON CONFLICT(id) DO UPDATE SET session_id = excluded.session_id, directory = excluded.directory
        """, (session_id, directory))


def clear_active():
    with _conn() as con:
        con.execute("DELETE FROM active_session WHERE id = 1")
