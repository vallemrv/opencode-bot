"""
SQLite persistence for the bot.
Stores only one thing: which session_id is currently active.
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
                id      INTEGER PRIMARY KEY CHECK (id = 1),
                session_id  TEXT NOT NULL,
                cwd         TEXT NOT NULL,
                model       TEXT         -- "providerID/modelID" or NULL
            )
        """)


def get_active() -> dict | None:
    with _conn() as con:
        row = con.execute("SELECT * FROM active_session WHERE id = 1").fetchone()
        return dict(row) if row else None


def set_active(session_id: str, cwd: str, model: str | None):
    with _conn() as con:
        con.execute("""
            INSERT INTO active_session (id, session_id, cwd, model)
            VALUES (1, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                session_id = excluded.session_id,
                cwd        = excluded.cwd,
                model      = excluded.model
        """, (session_id, cwd, model))


def clear_active():
    with _conn() as con:
        con.execute("DELETE FROM active_session WHERE id = 1")
