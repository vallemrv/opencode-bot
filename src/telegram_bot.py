"""
Telegram bot — remote control for OpenCode server.

Flow:
  /open → folder browser → model picker (grouped by provider) → session created
  text  → prompt_async → status message updated every 30s → deleted on idle → final message
"""

import os
import asyncio
import logging
from pathlib import Path
from collections import defaultdict

from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand, Message
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters,
)
from telegram.error import BadRequest

import db
from opencode_client import OpenCodeClient

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

TOKEN     = os.environ["TELEGRAM_BOT_TOKEN"]
ADMIN_ID  = int(os.environ["TELEGRAM_ADMIN_ID"])
OC_HOST   = os.getenv("OPENCODE_HOST", "localhost")
OC_PORT   = int(os.getenv("OPENCODE_PORT", "4096"))
WORKSPACE = Path(os.getenv("DEFAULT_WORKSPACE", "~/proyectos")).expanduser()

oc = OpenCodeClient(OC_HOST, OC_PORT)

# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def admin_only(func):
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != ADMIN_ID:
            return
        return await func(update, ctx)
    return wrapper

# ---------------------------------------------------------------------------
# Key store — int → str to stay within Telegram's 64-byte callback_data limit
# ---------------------------------------------------------------------------

def _key(ctx: ContextTypes.DEFAULT_TYPE, value: str) -> int:
    store = ctx.bot_data.setdefault("ks", {})
    for k, v in store.items():
        if v == value:
            return k
    k = len(store)
    store[k] = value
    return k

def _val(ctx: ContextTypes.DEFAULT_TYPE, k: int) -> str:
    return ctx.bot_data.get("ks", {}).get(k, "")

# ---------------------------------------------------------------------------
# Status tracking (per active session)
# ---------------------------------------------------------------------------
# app.bot_data["status"] = {
#   "session_id": str,
#   "msg_id": int | None,     # Telegram message id del mensaje de estado
#   "state": str,             # busy | thinking | idle | error
#   "tool": str | None,       # herramienta actual
#   "tools_seen": [str],      # todas las herramientas llamadas
#   "files_edited": set(),    # ficheros modificados
#   "message_count": int,     # mensajes del assistant
#   "last_text": str | None,  # último fragmento de texto recibido
# }

STATUS_INTERVAL = 30  # fallback heartbeat en segundos


async def _delete_msg(bot, chat_id: int, msg_id: int):
    try:
        await bot.delete_message(chat_id=chat_id, message_id=msg_id)
    except BadRequest:
        pass


def _build_status_text(st: dict, active: dict) -> str:
    """Construye el texto del mensaje de estado en tiempo real."""
    state = st.get("state", "busy")
    tool  = st.get("tool")
    files = st.get("files_edited", set())
    msgs  = st.get("message_count", 0)
    model = (active or {}).get("model") or "default"
    cwd   = Path((active or {}).get("cwd", "")).name or "?"
    last  = st.get("last_text") or ""

    icons = {"busy": "🔴", "thinking": "🤔", "idle": "🟢", "error": "❌"}
    icon  = icons.get(state, "⚪")

    lines = [
        f"{icon} *{state.upper()}*  —  📂 `{cwd}`  🧩 `{model}`",
        f"💬 mensajes: `{msgs}`   📝 ficheros: `{len(files)}`",
    ]
    if tool:
        lines.append(f"🔧 `{tool}`")
    if last:
        # Mostrar solo las últimas 120 chars del streaming
        snippet = last[-120:].replace("`", "'")
        lines.append(f"_{snippet}_")
    lines.append("")
    lines.append("_Pulsa_ /esc _para cancelar_")
    return "\n".join(lines)


async def _update_status_now(app: Application):
    """Edita el mensaje de estado inmediatamente."""
    st = app.bot_data.get("status")
    if not st or not st.get("msg_id"):
        return
    active = db.get_active()
    text = _build_status_text(st, active)
    try:
        await app.bot.edit_message_text(
            chat_id=ADMIN_ID,
            message_id=st["msg_id"],
            text=text,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Cancelar", callback_data="abort:")
            ]]),
        )
    except BadRequest:
        pass


async def _heartbeat_loop(ctx: ContextTypes.DEFAULT_TYPE):
    """Fallback: refresca el status cada STATUS_INTERVAL si no hay eventos SSE."""
    await _update_status_now(ctx.application)


def _start_status(app: Application, session_id: str, msg_id: int):
    app.bot_data["status"] = {
        "session_id": session_id,
        "msg_id": msg_id,
        "state": "busy",
        "tool": None,
        "tools_seen": [],
        "files_edited": set(),
        "message_count": 0,
        "last_text": None,
    }
    # Heartbeat de fallback
    for job in app.job_queue.get_jobs_by_name("status_heartbeat"):
        job.schedule_removal()
    app.job_queue.run_repeating(
        _heartbeat_loop,
        interval=STATUS_INTERVAL,
        first=STATUS_INTERVAL,
        name="status_heartbeat",
    )


async def _finish_status(app: Application, session_id: str):
    """Cierra el status, muestra la respuesta final del asistente."""
    st = app.bot_data.pop("status", None)
    for job in app.job_queue.get_jobs_by_name("status_heartbeat"):
        job.schedule_removal()

    # Eliminar mensaje de estado
    if st and st.get("msg_id"):
        await _delete_msg(app.bot, ADMIN_ID, st["msg_id"])

    # Obtener respuesta final
    try:
        messages = await oc.get_messages(session_id)
    except Exception as exc:
        await app.bot.send_message(ADMIN_ID, f"⚠️ No pude obtener la respuesta: {exc}")
        return

    # Buscar último mensaje del assistant con texto
    reply_text = None
    for m in reversed(messages):
        if m.get("role") == "assistant":
            parts = m.get("parts", [])
            texts = [p.get("text", "") for p in parts if p.get("type") == "text" and p.get("text")]
            if texts:
                reply_text = "\n".join(texts)
                break

    if not reply_text:
        await app.bot.send_message(ADMIN_ID, "✅ Listo.")
        return

    # Detectar si es una pregunta (para mostrar hint de respuesta)
    is_question = reply_text.rstrip().endswith("?")

    # Enviar en chunks si es largo (límite Telegram ~4096 chars)
    chunk_size = 3900
    chunks = [reply_text[i:i+chunk_size] for i in range(0, len(reply_text), chunk_size)]
    for i, chunk in enumerate(chunks):
        is_last = (i == len(chunks) - 1)
        kbd = None
        if is_last and is_question:
            kbd = InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Cancelar / ignorar", callback_data="abort:")
            ]])
        header = f"✅ _{Path((db.get_active() or {}).get('cwd','')).name}_\n\n" if i == 0 else ""
        await app.bot.send_message(
            ADMIN_ID,
            f"{header}{chunk}",
            parse_mode="Markdown",
            reply_markup=kbd,
        )

# ---------------------------------------------------------------------------
# SSE listener — suscripción completa a todos los eventos
# ---------------------------------------------------------------------------

async def sse_listener(app: Application) -> None:
    logger.info("SSE listener started")
    async for event in oc.event_stream():
        etype = event.get("type", "")
        props = event.get("properties", {})

        active     = db.get_active()
        active_sid = active["session_id"] if active else None
        st         = app.bot_data.get("status")

        # Solo procesamos eventos de la sesión activa con status activo
        def _is_our_session(sid: str) -> bool:
            return sid and sid == active_sid and st and st.get("session_id") == sid

        try:
            # ── session.status (busy / idle) ───────────────────────────────
            if etype == "session.status":
                sid        = props.get("sessionID", "")
                state_type = (props.get("status") or {}).get("type", "idle")
                if _is_our_session(sid):
                    if state_type == "idle":
                        await _finish_status(app, sid)
                    else:
                        st["state"] = state_type
                        await _update_status_now(app)

            # ── tool.invocation ────────────────────────────────────────────
            elif etype in ("tool.invocation", "session.next.tool.called"):
                sid  = props.get("sessionID", "")
                tool = props.get("tool") or props.get("toolName") or props.get("name", "")
                if _is_our_session(sid) and tool:
                    st["state"] = "busy"
                    st["tool"]  = tool
                    if tool not in st["tools_seen"]:
                        st["tools_seen"].append(tool)
                    # Detectar ficheros editados
                    path = props.get("input", {}).get("path") or props.get("path")
                    if path and any(kw in tool.lower() for kw in ("edit", "write", "create", "patch")):
                        st["files_edited"].add(path)
                    await _update_status_now(app)

            # ── message.part.delta (streaming de texto) ───────────────────
            elif etype == "message.part.delta":
                sid   = props.get("sessionID", "")
                delta = props.get("delta", {})
                text  = delta.get("text") or delta.get("content", "")
                ptype = delta.get("type", "")
                if _is_our_session(sid):
                    if ptype == "reasoning":
                        st["state"] = "thinking"
                    else:
                        st["state"] = "busy"
                    if text:
                        st["last_text"] = (st.get("last_text") or "") + text
                    await _update_status_now(app)

            # ── message.part.updated ───────────────────────────────────────
            elif etype == "message.part.updated":
                sid   = props.get("sessionID", "")
                part  = props.get("part", {})
                ptype = part.get("type", "")
                if _is_our_session(sid):
                    if ptype == "reasoning":
                        st["state"] = "thinking"
                    elif ptype in ("text", "tool-invocation"):
                        st["state"] = "busy"
                    await _update_status_now(app)

            # ── message.updated (contador) ─────────────────────────────────
            elif etype == "message.updated":
                sid = props.get("sessionID", "")
                if _is_our_session(sid):
                    role = props.get("message", {}).get("role", "")
                    if role == "assistant":
                        st["message_count"] = st.get("message_count", 0) + 1
                    st["last_text"] = None  # resetear snippet en cada mensaje nuevo
                    await _update_status_now(app)

            # ── session.idle (fallback) ────────────────────────────────────
            elif etype == "session.idle":
                sid = props.get("sessionID", "")
                if _is_our_session(sid):
                    await _finish_status(app, sid)

            # ── session.error ──────────────────────────────────────────────
            elif etype == "session.error":
                sid = props.get("sessionID", "")
                if _is_our_session(sid):
                    msg = props.get("message") or props.get("error") or str(props)
                    app.bot_data.pop("status", None)
                    for job in app.job_queue.get_jobs_by_name("status_heartbeat"):
                        job.schedule_removal()
                    await app.bot.send_message(ADMIN_ID, f"❌ *Error:* {msg}", parse_mode="Markdown")

        except Exception as exc:
            logger.warning("SSE handler error %s: %s", etype, exc)

# ---------------------------------------------------------------------------
# /open  — step 1: browse   step 2: model picker grouped by provider
# ---------------------------------------------------------------------------

PAGE = 8


def _folder_kbd(ctx, path: Path, page: int):
    try:
        entries = sorted(path.iterdir(), key=lambda p: (not p.is_dir(), p.name.lower()))
    except PermissionError:
        entries = []
    dirs  = [e for e in entries if e.is_dir()  and not e.name.startswith(".")]
    files = [e for e in entries if e.is_file()]
    all_e = dirs + files
    total = max(1, (len(all_e) + PAGE - 1) // PAGE)
    page  = max(0, min(page, total - 1))
    chunk = all_e[page * PAGE:(page + 1) * PAGE]

    pk   = _key(ctx, str(path))
    btns = []
    for e in chunk:
        icon = "📁" if e.is_dir() else "📄"
        btns.append([InlineKeyboardButton(f"{icon} {e.name}", callback_data=f"ob:{_key(ctx,str(e))}:0")])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀", callback_data=f"ob:{pk}:{page-1}"))
    if page < total - 1:
        nav.append(InlineKeyboardButton("▶", callback_data=f"ob:{pk}:{page+1}"))
    if nav:
        btns.append(nav)

    btns.append([InlineKeyboardButton("✅ Open here", callback_data=f"os:{pk}")])
    parent = path.parent
    if parent != path:
        btns.append([InlineKeyboardButton("⬆ Up", callback_data=f"ob:{_key(ctx,str(parent))}:0")])

    return f"📂 `{path}`  _{page+1}/{total}_", InlineKeyboardMarkup(btns)


@admin_only
async def cmd_open(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    txt, kbd = _folder_kbd(ctx, WORKSPACE, 0)
    await update.message.reply_text(txt, reply_markup=kbd, parse_mode="Markdown")


async def cb_ob(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    _, pk, pg = q.data.split(":")
    path = Path(_val(ctx, int(pk)))
    if path.is_file():
        path = path.parent
    txt, kbd = _folder_kbd(ctx, path, int(pg))
    await q.edit_message_text(txt, reply_markup=kbd, parse_mode="Markdown")


async def cb_os(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Folder selected → show model picker (providers first)."""
    q = update.callback_query; await q.answer()
    pk  = int(q.data.split(":")[1])
    cwd = _val(ctx, pk)
    ctx.bot_data["pending_cwd"] = cwd

    # Mostrar "Cargando modelos..."
    await q.edit_message_text(f"📂 `{cwd}`\n⏳ Cargando modelos...", parse_mode="Markdown")

    try:
        models = await oc.list_models()
    except Exception as exc:
        await q.edit_message_text(f"❌ Error al cargar modelos: {exc}", parse_mode="Markdown")
        return

    # Agrupar por proveedor
    groups: dict[str, list] = defaultdict(list)
    for m in models:
        pid = m.get("providerID", "?")
        mid = m.get("id") or m.get("modelID", "?")
        groups[pid].append(mid)

    btns = []
    for pid in sorted(groups):
        btns.append([InlineKeyboardButton(f"🔹 {pid}", callback_data=f"pickprov:{pk}:{_key(ctx, pid)}")])
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])

    await q.edit_message_text(
        f"📂 `{cwd}`\n📦 Elige proveedor:",
        reply_markup=InlineKeyboardMarkup(btns),
        parse_mode="Markdown",
    )


async def cb_pickprov(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Provider selected → show models of that provider."""
    q = update.callback_query; await q.answer()
    _, pk, pid_k = q.data.split(":")
    pk = int(pk)
    pid = _val(ctx, int(pid_k))
    cwd = _val(ctx, pk)

    # Mostrar "Cargando modelos..."
    await q.edit_message_text(f"📂 `{cwd}`\n⏳ Cargando modelos de {pid}...", parse_mode="Markdown")

    try:
        models = await oc.list_models()
    except Exception as exc:
        await q.edit_message_text(f"❌ Error: {exc}", parse_mode="Markdown")
        return

    mids = sorted([m.get("id") or m.get("modelID", "?") for m in models if m.get("providerID") == pid])
    btns = []
    row: list[InlineKeyboardButton] = []
    for mid in mids:
        mk = _key(ctx, f"{pid}|{mid}")
        row.append(InlineKeyboardButton(mid, callback_data=f"pickmodel:{pk}:{mk}"))
        if len(row) == 2:
            btns.append(row); row = []
    if row:
        btns.append(row)
    # Opción default
    mk0 = _key(ctx, "|")
    btns.append([InlineKeyboardButton("⚙ Defecto", callback_data=f"pickmodel:{pk}:{mk0}")])
    # Botón volver
    btns.append([InlineKeyboardButton("⬅ Volver", callback_data=f"os:{pk}")])
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])

    await q.edit_message_text(
        f"📂 `{cwd}`\n🧩 Modelos de *{pid}*:",
        reply_markup=InlineKeyboardMarkup(btns),
        parse_mode="Markdown",
    )


async def cb_pickmodel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Model selected → create session → save to DB."""
    q = update.callback_query; await q.answer()
    _, pk, mk = q.data.split(":")
    cwd       = _val(ctx, int(pk))
    model_str = _val(ctx, int(mk))   # "providerID|modelID" or "|"
    pid, mid  = model_str.split("|", 1) if "|" in model_str else ("", "")

    try:
        sess = await oc.create_session()
    except Exception as exc:
        await q.edit_message_text(f"❌ Error al crear sesión: {exc}")
        return

    sid         = sess.get("id", "")
    title       = sess.get("title") or sid[:12]
    model_label = f"{pid}/{mid}" if pid else "default"
    db.set_active(sid, cwd, f"{pid}/{mid}" if pid else None)

    msg = f"""✅ Sesión lista
╭─────────────────────────────╮
│ 📦 Sesión: `{title}`
│ 📂 Proyecto: `{Path(cwd).name}`
│ 🧩 Modelo: `{model_label}`
╰─────────────────────────────╯

Envía tu prompt."""

    await q.edit_message_text(msg, parse_mode="Markdown")


async def cb_noop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()


async def cb_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Cancelar cualquier operación."""
    q = update.callback_query; await q.answer()
    await q.edit_message_text("❌ Cancelado.")


async def cb_abort(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Abortar la sesión activa de OpenCode."""
    q = update.callback_query; await q.answer()
    active = db.get_active()
    if not active:
        await q.edit_message_text("⚠️ No hay sesión activa.")
        return
    try:
        await oc.abort_session(active["session_id"])
    except Exception as exc:
        await q.edit_message_text(f"⚠️ Error al cancelar: {exc}")
        return
    # Limpiar status
    app = q.get_bot().application if hasattr(q.get_bot(), "application") else None
    # Cancelar via bot_data directamente
    ctx.application.bot_data.pop("status", None)
    for job in ctx.application.job_queue.get_jobs_by_name("status_heartbeat"):
        job.schedule_removal()
    await q.edit_message_text("🛑 Tarea cancelada.")


@admin_only
async def cmd_esc(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Cancelar la tarea actual de OpenCode."""
    active = db.get_active()
    if not active:
        await update.message.reply_text("⚠️ No hay sesión activa.")
        return
    try:
        await oc.abort_session(active["session_id"])
    except Exception as exc:
        await update.message.reply_text(f"⚠️ Error al cancelar: {exc}")
        return
    ctx.application.bot_data.pop("status", None)
    for job in ctx.application.job_queue.get_jobs_by_name("status_heartbeat"):
        job.schedule_removal()
    await update.message.reply_text("🛑 Tarea cancelada.")

# ---------------------------------------------------------------------------
# /close
# ---------------------------------------------------------------------------

@admin_only
async def cmd_close(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        sessions = await oc.list_sessions()
    except Exception as exc:
        await update.message.reply_text(f"Error: {exc}"); return

    groups: dict[str, list] = {}
    for s in sessions:
        cwd = s.get("directory") or (s.get("project") or {}).get("worktree", "?")
        groups.setdefault(cwd, []).append(s)

    if not groups:
        await update.message.reply_text("No sessions."); return

    btns = []
    for cwd, slist in sorted(groups.items()):
        ck = _key(ctx, cwd)
        btns.append([InlineKeyboardButton(
            f"{Path(cwd).name}  ({len(slist)})",
            callback_data=f"cl:{ck}"
        )])
    await update.message.reply_text(
        "Select project to close (deletes all its sessions):",
        reply_markup=InlineKeyboardMarkup(btns),
    )


async def cb_cl(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    ck  = int(q.data.split(":")[1])
    cwd = _val(ctx, ck)

    try:
        sessions = await oc.list_sessions()
        to_del   = [s for s in sessions
                    if (s.get("directory") or (s.get("project") or {}).get("worktree","")) == cwd]
        for s in to_del:
            await oc.delete_session(s["id"])
    except Exception as exc:
        await q.edit_message_text(f"Error: {exc}"); return

    if (active := db.get_active()) and active["cwd"] == cwd:
        db.clear_active()

    await q.edit_message_text(f"Deleted {len(to_del)} session(s) from `{Path(cwd).name}`.", parse_mode="Markdown")

# ---------------------------------------------------------------------------
# /sessions
# ---------------------------------------------------------------------------


# Sesiones paginadas solo del cwd actual
@admin_only
async def cmd_sessions(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    # Determinar cwd activo
    active = db.get_active() or {}
    cwd = active.get("cwd")
    if not cwd:
        await update.message.reply_text("No hay proyecto activo. Usa /open para seleccionar uno.")
        return

    # Paginación
    page = 0
    if update.message and update.message.text and "page=" in update.message.text:
        try:
            page = int(update.message.text.split("page=")[-1])
        except Exception:
            page = 0
    PAGE_SESS = 6

    try:
        sessions = await oc.list_sessions()
    except Exception as exc:
        await update.message.reply_text(f"Error: {exc}"); return

    # Filtrar solo sesiones del cwd actual
    filtered = [s for s in sessions if (s.get("directory") or (s.get("project") or {}).get("worktree","")) == cwd]
    total = max(1, (len(filtered) + PAGE_SESS - 1) // PAGE_SESS)
    page = max(0, min(page, total - 1))
    chunk = filtered[page * PAGE_SESS:(page + 1) * PAGE_SESS]

    active_id = active.get("session_id")

    btns = [[InlineKeyboardButton("➕ Nueva", callback_data="sn:")]]
    if filtered:
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

    # Navegación
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀", callback_data=f"sesspage:{page-1}"))
    if page < total - 1:
        nav.append(InlineKeyboardButton("▶", callback_data=f"sesspage:{page+1}"))
    if nav:
        btns.append(nav)

    await update.message.reply_text(f"Sesiones de `{Path(cwd).name}`  _{page+1}/{total}_", reply_markup=InlineKeyboardMarkup(btns), parse_mode="Markdown")

# Callback para paginación de sesiones
async def cb_sesspage(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    page = int(q.data.split(":")[1])
    # Simula un mensaje de texto con page= para reutilizar cmd_sessions
    class DummyMsg:
        text = f"page={page}"
    update.message = DummyMsg()
    await cmd_sessions(update, ctx)


async def cb_sn(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    try:
        sess = await oc.create_session()
    except Exception as exc:
        await q.edit_message_text(f"Failed: {exc}"); return
    sid = sess.get("id",""); cwd = sess.get("directory","")
    db.set_active(sid, cwd, None)
    await q.edit_message_text(f"Session `{sess.get('title') or sid[:12]}` created and activated.", parse_mode="Markdown")


async def cb_sa(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    sid = _val(ctx, int(q.data.split(":")[1]))
    try:
        sessions = await oc.list_sessions()
    except Exception:
        sessions = []
    s   = next((x for x in sessions if x.get("id") == sid), {})
    cwd = s.get("directory","")
    m   = s.get("model") or {}
    model = f"{m.get('providerID','')}/{m.get('id','')}" if m else None
    db.set_active(sid, cwd, model)
    await q.edit_message_text(f"Active: `{s.get('title') or sid[:12]}` @ `{Path(cwd).name}`", parse_mode="Markdown")


async def cb_sd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    sid = _val(ctx, int(q.data.split(":")[1]))
    try:
        await oc.delete_session(sid)
    except Exception as exc:
        await q.edit_message_text(f"Failed: {exc}"); return
    if (active := db.get_active()) and active["session_id"] == sid:
        db.clear_active()
    await q.edit_message_text(f"Session `{sid[:8]}` deleted.", parse_mode="Markdown")


async def cb_sda(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    try:
        sessions = await oc.list_sessions()
        for s in sessions:
            await oc.delete_session(s["id"])
    except Exception as exc:
        await q.edit_message_text(f"Error: {exc}"); return
    db.clear_active()
    await q.edit_message_text("All sessions deleted.")

# ---------------------------------------------------------------------------
# /models
# ---------------------------------------------------------------------------

@admin_only
async def cmd_models(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Change model for active session."""
    active = db.get_active()
    if not active:
        await update.message.reply_text("❌ No hay sesión activa. Usa /open primero.")
        return

    # Mostrar "Cargando modelos..."
    msg = await update.message.reply_text("⏳ Cargando modelos...")

    try:
        models = await oc.list_models()
    except Exception as exc:
        await msg.edit_text(f"❌ Error: {exc}")
        return

    # Agrupar por proveedor
    groups: dict[str, list] = defaultdict(list)
    for m in models:
        pid = m.get("providerID", "?")
        mid = m.get("id") or m.get("modelID", "?")
        groups[pid].append(mid)

    btns = []
    for pid in sorted(groups):
        btns.append([InlineKeyboardButton(f"🔹 {pid}", callback_data=f"modelprov:{_key(ctx, pid)}")])
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])

    await msg.edit_text("📦 Elige proveedor:", reply_markup=InlineKeyboardMarkup(btns))


async def cb_modelprov(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Provider selected (from /models) → show models of that provider."""
    q = update.callback_query; await q.answer()
    pid_k = int(q.data.split(":")[1])
    pid = _val(ctx, pid_k)

    await q.edit_message_text(f"⏳ Cargando modelos de {pid}...")

    try:
        models = await oc.list_models()
    except Exception as exc:
        await q.edit_message_text(f"❌ Error: {exc}")
        return

    mids = sorted([m.get("id") or m.get("modelID", "?") for m in models if m.get("providerID") == pid])
    btns = []
    row: list[InlineKeyboardButton] = []
    for mid in mids:
        mk = _key(ctx, f"{pid}|{mid}")
        row.append(InlineKeyboardButton(mid, callback_data=f"modelset:{mk}"))
        if len(row) == 2:
            btns.append(row); row = []
    if row:
        btns.append(row)
    # Opción default
    mk0 = _key(ctx, "|")
    btns.append([InlineKeyboardButton("⚙ Defecto", callback_data=f"modelset:{mk0}")])
    # Botón volver
    btns.append([InlineKeyboardButton("⬅ Volver", callback_data="models:")])
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])

    await q.edit_message_text(f"🧩 Modelos de *{pid}*:", reply_markup=InlineKeyboardMarkup(btns), parse_mode="Markdown")


async def cb_modelset(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Model selected from /models → set as active model."""
    q = update.callback_query; await q.answer()
    mk = int(q.data.split(":")[1])
    model_str = _val(ctx, mk)
    active = db.get_active()
    if active:
        sid = active["session_id"]
        cwd = active["cwd"]
        title = sid[:12]  # Default: primeros 12 chars del session ID
        
        # Obtener info de sesión para el título
        try:
            sessions = await oc.list_sessions()
            for s in sessions:
                if s.get("id") == sid:
                    title = s.get("title") or sid[:12]
                    break
        except Exception:
            pass
        
        db.set_active(sid, cwd, model_str or None)
        
        msg = f"""✅ Modelo establecido
╭─────────────────────────────╮
│ 📦 Sesión: `{title}`
│ 📂 Proyecto: `{Path(cwd).name}`
│ 🧩 Modelo: `{model_str or 'default'}`
╰─────────────────────────────╯"""
        
        await q.edit_message_text(msg, parse_mode="Markdown")


async def cb_ms(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    model_str = _val(ctx, int(q.data.split(":")[1]))
    active    = db.get_active()
    if active:
        db.set_active(active["session_id"], active["cwd"], model_str or None)
    await q.edit_message_text(f"Model set to `{model_str}`", parse_mode="Markdown")

# ---------------------------------------------------------------------------
# Plain text → prompt_async
# ---------------------------------------------------------------------------

@admin_only
async def handle_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text   = update.message.text
    active = db.get_active()

    if not active:
        try:
            sess  = await oc.create_session()
            sid   = sess.get("id", ""); cwd = sess.get("directory", "")
            db.set_active(sid, cwd, None)
            active = db.get_active()
        except Exception as exc:
            await update.message.reply_text(f"❌ No hay sesión activa y no pude crear una: {exc}")
            return

    model_str = active.get("model")
    pid, mid  = model_str.split("/", 1) if model_str and "/" in model_str else (None, None)
    sid = active["session_id"]
    cwd_name = Path(active.get("cwd", "")).name or "?"
    model_label = model_str or "default"

    # Crear mensaje de estado INMEDIATAMENTE antes de enviar
    status_msg = await update.message.reply_text(
        f"🔴 *BUSY*  —  📂 `{cwd_name}`  🧩 `{model_label}`\n"
        f"💬 mensajes: `0`   📝 ficheros: `0`\n\n"
        f"_Pulsa_ /esc _para cancelar_",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("❌ Cancelar", callback_data="abort:")
        ]]),
    )
    _start_status(ctx.application, sid, status_msg.message_id)

    try:
        await oc.send_message_async(sid, text, provider_id=pid, model_id=mid)
    except Exception as exc:
        app = ctx.application
        app.bot_data.pop("status", None)
        for job in app.job_queue.get_jobs_by_name("status_heartbeat"):
            job.schedule_removal()
        await status_msg.edit_text(f"❌ Error al enviar: {exc}")

# ---------------------------------------------------------------------------
# /start
# ---------------------------------------------------------------------------

@admin_only
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Show current status and main menu."""
    active = db.get_active()
    
    # Verificar conexión
    try:
        connected = await oc.ping()
    except Exception:
        connected = False
    
    status_icon = "✅" if connected else "❌"
    
    # Obtener información de sesiones
    try:
        sessions = await oc.list_sessions()
        total_sessions = len(sessions)
    except Exception:
        sessions = []
        total_sessions = 0
    
    # Información de sesión activa
    if active:
        sid = active["session_id"]
        cwd = active["cwd"]
        model = active["model"] or "default"
        cwd_name = Path(cwd).name
        
        # Obtener título de sesión
        session_title = sid[:12]
        try:
            for s in sessions:
                if s.get("id") == sid:
                    session_title = s.get("title") or sid[:12]
                    break
        except Exception:
            pass
        
        # Obtener estado de la sesión
        try:
            status_data = await oc.get_session_status()
            session_state = status_data.get(sid, {}).get("type", "idle")
        except Exception:
            session_state = "unknown"
        
        state_icon = {"busy": "🔴", "idle": "🟢", "unknown": "⚪"}.get(session_state, "⚪")
        
        msg = f"""╔════════════════════════════════╗
║    {status_icon} OPENCODE BOT STATUS            ║
╚════════════════════════════════╝

🔗 OpenCode Server: {status_icon} {OC_HOST}:{OC_PORT}

📊 ESTADO ACTUAL:
{state_icon} Sesión: `{session_title}`
📂 Proyecto: `{cwd_name}`
🧩 Modelo: `{model}`
💾 Directorio: `{cwd}`

📈 ESTADÍSTICAS:
  • Total de sesiones: {total_sessions}
  • Sesión activa: ✓

📋 COMANDOS:
/open     → Abrir proyecto
/close    → Cerrar proyecto
/sessions → Gestionar sesiones
/models   → Cambiar modelo
/start    → Ver este menú"""
    else:
        msg = f"""╔════════════════════════════════╗
║    {status_icon} OPENCODE BOT STATUS            ║
╚════════════════════════════════╝

🔗 OpenCode Server: {status_icon} {OC_HOST}:{OC_PORT}

⚠️  NO HAY SESIÓN ACTIVA

📈 ESTADÍSTICAS:
  • Total de sesiones: {total_sessions}
  • Sesión activa: ✗

📋 COMANDOS:
/open     → Abrir proyecto (START HERE)
/close    → Cerrar proyecto
/sessions → Gestionar sesiones
/models   → Cambiar modelo
/start    → Ver este menú"""
    
    await update.message.reply_text(msg, parse_mode="Markdown")

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

def main():
    db.init()
    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start",    cmd_start))
    app.add_handler(CommandHandler("open",     cmd_open))
    app.add_handler(CommandHandler("close",    cmd_close))
    app.add_handler(CommandHandler("sessions", cmd_sessions))
    app.add_handler(CommandHandler("models",   cmd_models))
    app.add_handler(CommandHandler("esc",      cmd_esc))

    app.add_handler(CallbackQueryHandler(cb_ob,         pattern=r"^ob:"))
    app.add_handler(CallbackQueryHandler(cb_os,         pattern=r"^os:"))
    app.add_handler(CallbackQueryHandler(cb_pickprov,   pattern=r"^pickprov:"))
    app.add_handler(CallbackQueryHandler(cb_pickmodel,  pattern=r"^pickmodel:"))
    app.add_handler(CallbackQueryHandler(cb_modelprov,  pattern=r"^modelprov:"))
    app.add_handler(CallbackQueryHandler(cb_modelset,   pattern=r"^modelset:"))
    app.add_handler(CallbackQueryHandler(cb_cl,         pattern=r"^cl:"))
    app.add_handler(CallbackQueryHandler(cb_sn,         pattern=r"^sn:"))
    app.add_handler(CallbackQueryHandler(cb_sa,         pattern=r"^sa:"))
    app.add_handler(CallbackQueryHandler(cb_sd,         pattern=r"^sd:"))
    app.add_handler(CallbackQueryHandler(cb_sda,        pattern=r"^sda:"))
    app.add_handler(CallbackQueryHandler(cb_ms,         pattern=r"^ms:"))
    app.add_handler(CallbackQueryHandler(cb_noop,       pattern=r"^noop:"))
    app.add_handler(CallbackQueryHandler(cb_cancel,     pattern=r"^cancel:"))
    app.add_handler(CallbackQueryHandler(cb_abort,      pattern=r"^abort:"))
    app.add_handler(CallbackQueryHandler(cb_sesspage,   pattern=r"^sesspage:"))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    app.job_queue.run_once(
        lambda c: asyncio.ensure_future(sse_listener(app)), when=1
    )

    async def post_init(application: Application):
        await application.bot.set_my_commands([
            BotCommand("start",    "Estado y menú"),
            BotCommand("open",     "Abrir proyecto"),
            BotCommand("close",    "Cerrar proyecto"),
            BotCommand("sessions", "Gestionar sesiones"),
            BotCommand("models",   "Cambiar modelo"),
            BotCommand("esc",      "Cancelar tarea actual"),
        ])
    app.post_init = post_init

    logger.info("Bot starting...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
