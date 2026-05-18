"""
SQLite persistence for the bot.
Tracks: open projects (cwds), their sessions, and the active session.
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
            CREATE TABLE IF NOT EXISTS open_cwds (
                cwd TEXT PRIMARY KEY
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                session_id   TEXT PRIMARY KEY,
                cwd          TEXT NOT NULL,
                title        TEXT,
                model        TEXT,
                status       TEXT DEFAULT 'idle',
                created_at   INTEGER,
                updated_at   INTEGER,
                FOREIGN KEY (cwd) REFERENCES open_cwds(cwd)
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS active_session (
                id           INTEGER PRIMARY KEY CHECK (id = 1),
                session_id   TEXT NOT NULL
            )
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_sessions_cwd ON sessions(cwd)")


def is_cwd_open(cwd: str) -> bool:
    with _conn() as con:
        row = con.execute("SELECT 1 FROM open_cwds WHERE cwd = ?", (cwd,)).fetchone()
        return row is not None


def open_cwd(cwd: str):
    with _conn() as con:
        con.execute("INSERT OR IGNORE INTO open_cwds (cwd) VALUES (?)", (cwd,))


def close_cwd(cwd: str):
    with _conn() as con:
        con.execute("DELETE FROM active_session WHERE session_id IN (SELECT session_id FROM sessions WHERE cwd = ?)", (cwd,))
        con.execute("DELETE FROM sessions WHERE cwd = ?", (cwd,))
        con.execute("DELETE FROM open_cwds WHERE cwd = ?", (cwd,))


def get_all_open_cwds() -> list[str]:
    with _conn() as con:
        rows = con.execute("SELECT cwd FROM open_cwds").fetchall()
        return [r["cwd"] for r in rows]


def add_session(session_id: str, cwd: str, title: str | None = None, model: str | None = None, created_at: int | None = None):
    if not is_cwd_open(cwd):
        open_cwd(cwd)
    with _conn() as con:
        con.execute("""
            INSERT INTO sessions (session_id, cwd, title, model, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, 'idle', ?, ?)
            ON CONFLICT(session_id) DO UPDATE SET
                title = COALESCE(excluded.title, title),
                model = COALESCE(excluded.model, model),
                updated_at = COALESCE(excluded.updated_at, updated_at)
        """, (session_id, cwd, title, model, created_at, created_at))


def update_session(session_id: str, **kwargs):
    valid_fields = ['title', 'cwd', 'model', 'status', 'updated_at']
    updates = {k: v for k, v in kwargs.items() if k in valid_fields and v is not None}
    if not updates:
        return
    sql = "UPDATE sessions SET " + ", ".join(f"{k} = ?" for k in updates.keys()) + " WHERE session_id = ?"
    with _conn() as con:
        con.execute(sql, list(updates.values()) + [session_id])


def delete_session(session_id: str):
    with _conn() as con:
        con.execute("DELETE FROM sessions WHERE session_id = ?", (session_id,))
        con.execute("DELETE FROM active_session WHERE session_id = ?", (session_id,))


def get_session(session_id: str) -> dict | None:
    with _conn() as con:
        row = con.execute("SELECT * FROM sessions WHERE session_id = ?", (session_id,)).fetchone()
        return dict(row) if row else None


def get_sessions_by_cwd(cwd: str) -> list[dict]:
    with _conn() as con:
        rows = con.execute("SELECT * FROM sessions WHERE cwd = ? ORDER BY updated_at DESC", (cwd,)).fetchall()
        return [dict(r) for r in rows]


def get_active() -> dict | None:
    with _conn() as con:
        row = con.execute("""
            SELECT s.* FROM sessions s
            JOIN active_session a ON s.session_id = a.session_id
            WHERE a.id = 1
        """).fetchone()
        return dict(row) if row else None


def set_active(session_id: str):
    with _conn() as con:
        con.execute("""
            INSERT INTO active_session (id, session_id)
            VALUES (1, ?)
            ON CONFLICT(id) DO UPDATE SET session_id = excluded.session_id
        """, (session_id,))


def clear_active():
    with _conn() as con:
        con.execute("DELETE FROM active_session WHERE id = 1")


def sync_sessions_from_opencode(cwd: str, opencode_sessions: list[dict]):
    """Sync sessions for a specific cwd from OpenCode server."""
    open_cwd(cwd)
    
    existing_ids = set(s["session_id"] for s in get_sessions_by_cwd(cwd))
    opencode_ids = set()
    
    for sess in opencode_sessions:
        sess_cwd = sess.get("directory", "")
        if sess_cwd != cwd:
            continue
        
        sid = sess.get("id", "")
        opencode_ids.add(sid)
        
        title = sess.get("title", "")
        model_obj = sess.get("model", {})
        model = f"{model_obj.get('providerID', '')}/{model_obj.get('id', '')}" if model_obj else None
        created = sess.get("time", {}).get("created")
        updated = sess.get("time", {}).get("updated")
        
        add_session(sid, cwd, title, model, created)
        if updated:
            update_session(sid, updated_at=updated)
    
    for sid in existing_ids - opencode_ids:
        delete_session(sid)


def prune_sessions(cwd: str):
    """Delete sessions not in OpenCode for this cwd."""
    from opencode_client import OpenCodeClient
    import os
    
    host = os.getenv("OPENCODE_HOST", "localhost")
    port = int(os.getenv("OPENCODE_PORT", "4096"))
    oc = OpenCodeClient(host, port)
    
    try:
        sessions = oc.list_sessions()
        sync_sessions_from_opencode(cwd, sessions)
    except Exception:
        pass