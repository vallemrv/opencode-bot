"""Enrutado de interacciones del SSE compartido: sólo sesiones de Telegram."""

import json
import os
import re
import time
from pathlib import Path


def owned_by_valle(session_id):
    if not re.fullmatch(r"ses_[A-Za-z0-9_-]{1,196}", session_id):
        return True
    root = Path(os.environ.get("XDG_DATA_HOME", Path.home() / ".local/share"))
    path = root / "valle-bots/opencode/routes" / (session_id + ".json")
    try:
        data = json.loads(path.read_text())
        return data.get("owner") == "valle" and data.get("expires", 0) > time.time()
    except FileNotFoundError:
        return False
    except (OSError, ValueError, TypeError):
        return True


async def interaction_directory(app, session_id, db, oc):
    """Devuelve la carpeta propia o None; nunca usa la sesión activa de otro encargo."""
    if not session_id:
        return None
    active = await db.get_active()
    statuses = app.bot_data.get("statuses", {})
    current, visited = session_id, set()
    for _ in range(8):
        if not current or current in visited or owned_by_valle(current):
            return None
        visited.add(current)
        tracked = statuses.get(current)
        if tracked and tracked.get("directory"):
            return tracked["directory"]
        if active and active.get("session_id") == current:
            return active.get("directory") or None
        try:
            info = await oc.get_session(current)
        except Exception:  # noqa: BLE001 - un fallo del cliente debe descartar el evento
            return None
        current = info.get("parentID")
    return None
