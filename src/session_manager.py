from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from pathlib import Path

PAGE_SESS = 6

def session_manager_kbd(ctx, cwd, sessions, active_id=None, page=0, total=None):
    """
    Devuelve texto y teclado para gestionar sesiones de un cwd.
    sessions: lista de sesiones filtradas por cwd
    active_id: id de la sesión activa
    page: página actual
    total: total de páginas
    """
    total = total or max(1, (len(sessions) + PAGE_SESS - 1) // PAGE_SESS)
    page = max(0, min(page, total - 1))
    chunk = sessions[page * PAGE_SESS:(page + 1) * PAGE_SESS]

    btns = [[InlineKeyboardButton("➕ Nueva", callback_data="sn:")]]
    if sessions:
        btns.append([InlineKeyboardButton("🗑 Borrar todas", callback_data="sda:")])

    for s in chunk:
        sid   = s.get("id", "")
        title = s.get("title") or sid[:12]
        mark  = " ✅" if sid == active_id else ""
        sk    = _key(ctx, sid)
        btns.append([
            InlineKeyboardButton(f"{title[:16]}{mark}", callback_data=f"sa:{sk}"),
            InlineKeyboardButton("🗑", callback_data=f"sd:{sk}"),
        ])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀", callback_data=f"sesspage:{page-1}"))
    if page < total - 1:
        nav.append(InlineKeyboardButton("▶", callback_data=f"sesspage:{page+1}"))
    if nav:
        btns.append(nav)

    # Botón cerrar/cancelar
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])
    btns.append([InlineKeyboardButton("📁 Cerrar carpeta", callback_data="closecwd:")])

    txt = f"Sesiones de `{Path(cwd).name}`  _{page+1}/{total}_"
    return txt, InlineKeyboardMarkup(btns)

def _key(ctx, value: str) -> int:
    store = ctx.bot_data.setdefault("ks", {})
    for k, v in store.items():
        if v == value:
            return k
    k = len(store)
    store[k] = value
    return k
