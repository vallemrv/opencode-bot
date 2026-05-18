"""
Telegram bot — remote control for OpenCode server.

Flow:
  /open → folder browser → model picker (grouped by provider) → session created
  text  → prompt_async → status message updated every 30s → deleted on idle → final message
"""

import os
import asyncio
import logging
import time
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
# Models cache
# ---------------------------------------------------------------------------

MODELS_CACHE_TTL = 300  # segundos

async def _get_models(ctx: ContextTypes.DEFAULT_TYPE) -> list[dict]:
    """Devuelve la lista de modelos usando caché en bot_data (TTL 5 min)."""
    cache = ctx.bot_data.get("models_cache")
    if cache and (time.time() - cache["ts"]) < MODELS_CACHE_TTL:
        return cache["data"]
    try:
        models = await oc.list_models()
        ctx.bot_data["models_cache"] = {"ts": time.time(), "data": models}
        return models
    except Exception as exc:
        logger.error(f"Failed to list models: {type(exc).__name__}: {exc}")
        raise

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
# Status tracking (multiple sessions)
# ---------------------------------------------------------------------------
# app.bot_data["statuses"] = {
#   session_id: {
#     "msg_id": int | None,     # Telegram message id del mensaje de estado
#     "state": str,             # busy | thinking | idle | error
#     "tool": str | None,       # herramienta actual
#     "tools_seen": [str],      # todas las herramientas llamadas
#     "files_edited": set(),    # ficheros modificados
#     "message_count": int,     # mensajes del assistant
#     "last_text": str | None,  # último fragmento de texto recibido
#     "final_text": str | None, # texto final acumulado del SSE
#     "reasoning_text": str | None, # texto de razonamiento acumulado
#     "start_time": float,      # timestamp de inicio para calcular elapsed
#     "tokens_input": int,      # tokens de input
#     "tokens_output": int,     # tokens de output
#   }
# }
# app.bot_data["active_status_session"] = session_id  # cual está mostrando el mensaje

STATUS_INTERVAL = 30
STATUS_THROTTLE = 5


async def _update_status_now(app: Application, session_id: str, force: bool = False):
    """Edita el mensaje de estado. Throttled to max every STATUS_THROTTLE seconds."""
    statuses = app.bot_data.get("statuses", {})
    st = statuses.get(session_id)
    if not st or not st.get("msg_id"):
        return
    
    now = time.time()
    last_update = st.get("last_update_time", 0)
    
    if not force and (now - last_update) < STATUS_THROTTLE:
        return
    
    st["last_update_time"] = now
    
    session = db.get_session(session_id)
    text = _build_status_text(st, session)
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


def _format_elapsed(seconds: float) -> str:
    """Formatea elapsed time como MM:SS."""
    mins = int(seconds // 60)
    secs = int(seconds % 60)
    return f"{mins:02d}:{secs:02d}"


def _build_status_text(st: dict, session: dict | None) -> str:
    """Construye el texto del mensaje de estado en tiempo real."""
    state = st.get("state", "busy")
    tool  = st.get("tool")
    files = st.get("files_edited", set())
    msgs  = st.get("message_count", 0)
    model = (session or {}).get("model") or "default"
    cwd   = Path((session or {}).get("cwd", "")).name or "?"
    title = (session or {}).get("title") or "?"
    tools_seen = st.get("tools_seen", [])
    
    start_time = st.get("start_time", time.time())
    elapsed = time.time() - start_time
    elapsed_str = _format_elapsed(elapsed)
    
    reasoning = st.get("reasoning_text") or ""
    
    icons = {"busy": "🔴", "thinking": "🤔", "idle": "🟢", "error": "❌"}
    icon  = icons.get(state, "⚪")

    lines = [
        f"{icon} *{state.upper()}* | 📂 `{cwd}` | 🧩 `{model}`",
        f"📦 `{title[:20]}`",
        f"⏱ `{elapsed_str}` | 💬 `{msgs}` msgs | 📝 `{len(files)}` edits",
    ]
    
    if tool:
        lines.append(f"🔧 `{tool}`")
    
    if tools_seen:
        unique_tools = len(set(tools_seen))
        lines.append(f"⚡ `{unique_tools}` herramientas")
    
    if reasoning and state == "thinking":
        snippet = reasoning[-150:].replace("`", "'").replace("*", "")
        lines.append(f"💭 _{snippet}_")
    
    lines.append("")
    lines.append("_Pulsa_ /esc _para cancelar_")
    return "\n".join(lines)


async def _heartbeat_loop(ctx: ContextTypes.DEFAULT_TYPE):
    """Fallback: refresca el status cada STATUS_INTERVAL si no hay eventos SSE."""
    active_sid = ctx.bot_data.get("active_status_session")
    if active_sid:
        await _update_status_now(ctx.application, active_sid, force=True)


def _start_status(app: Application, session_id: str, msg_id: int):
    statuses = app.bot_data.setdefault("statuses", {})
    statuses[session_id] = {
        "msg_id": msg_id,
        "state": "busy",
        "tool": None,
        "tools_seen": [],
        "files_edited": set(),
        "message_count": 0,
        "last_text": None,
        "final_text": None,
        "reasoning_text": None,
        "start_time": time.time(),
        "last_update_time": time.time(),
        "tokens_input": 0,
        "tokens_output": 0,
    }
    app.bot_data["active_status_session"] = session_id
    
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
    statuses = app.bot_data.get("statuses", {})
    st = statuses.pop(session_id, None)
    
    if app.bot_data.get("active_status_session") == session_id:
        app.bot_data.pop("active_status_session", None)
        for job in app.job_queue.get_jobs_by_name("status_heartbeat"):
            job.schedule_removal()

    if st and st.get("msg_id"):
        await _delete_msg(app.bot, ADMIN_ID, st["msg_id"])

    reply_text = st.get("final_text") if st else None
    logger.info(f"Finish status: sid={session_id[:12]} final_text={reply_text[:50] if reply_text else 'None'}")

    if not reply_text:
        session = db.get_session(session_id)
        cwd = session.get("cwd") if session else None
        try:
            messages = await oc.get_messages(session_id, directory=cwd)
            logger.info(f"Got {len(messages)} messages from API")
            for m in reversed(messages):
                if m.get("role") == "assistant":
                    parts = m.get("parts", [])
                    texts = [p.get("text", "") for p in parts if p.get("type") == "text" and p.get("text")]
                    if texts:
                        reply_text = "\n".join(texts)
                        logger.info(f"Found assistant message: {reply_text[:100]}")
                        break
        except Exception as exc:
            logger.error(f"Failed to get messages: {exc}")
            await app.bot.send_message(ADMIN_ID, f"⚠️ No pude obtener la respuesta: {exc}")
            return

    if not reply_text:
        logger.warning(f"No reply text for session {session_id[:12]}")
        await app.bot.send_message(ADMIN_ID, "✅ Listo.")
        return

    is_question = reply_text.rstrip().endswith("?")
    session = db.get_session(session_id)
    cwd_name = Path((session or {}).get("cwd", "")).name or "?"

    chunk_size = 3900
    chunks = [reply_text[i:i+chunk_size] for i in range(0, len(reply_text), chunk_size)]
    for i, chunk in enumerate(chunks):
        is_last = (i == len(chunks) - 1)
        kbd = None
        if is_last and is_question:
            kbd = InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Cancelar / ignorar", callback_data="abort:")
            ]])
        header = f"✅ _{cwd_name}_\n\n" if i == 0 else ""
        await app.bot.send_message(
            ADMIN_ID,
            f"{header}{chunk}",
            parse_mode="Markdown",
            reply_markup=kbd,
        )

# ---------------------------------------------------------------------------
# SSE listener — solo procesa eventos de sesiones en cwds abiertos
# ---------------------------------------------------------------------------

async def sse_listener(app: Application) -> None:
    logger.info("SSE listener started")
    async for event in oc.event_stream():
        payload = event.get("payload", {})
        if not payload:
            continue
        
        etype = payload.get("type", "")
        props = payload.get("properties", {})
        directory = event.get("directory", "")
        
        sid = props.get("sessionID", "")
        
        try:
            if etype == "server.connected":
                logger.info("SSE: Connected to OpenCode server")
                continue
            
            if etype == "session.created":
                info = props.get("info", {})
                cwd = info.get("directory", "") or directory
                if db.is_cwd_open(cwd):
                    title = info.get("title", "")
                    model_obj = info.get("model", {})
                    model = f"{model_obj.get('providerID', '')}/{model_obj.get('id', '')}" if model_obj else None
                    created = info.get("time", {}).get("created")
                    db.add_session(sid, cwd, title, model, created)
                    logger.info(f"Session created: {sid[:12]} @ {cwd}")
                continue
            
            if etype == "session.updated":
                session = db.get_session(sid)
                if session:
                    info = props.get("info", {})
                    title = info.get("title")
                    cwd = info.get("directory")
                    model_obj = info.get("model", {})
                    model = f"{model_obj.get('providerID', '')}/{model_obj.get('id', '')}" if model_obj else None
                    updated = info.get("time", {}).get("updated")
                    update_data = {}
                    if title: update_data["title"] = title
                    if cwd: update_data["cwd"] = cwd
                    if model: update_data["model"] = model
                    if updated: update_data["updated_at"] = updated
                    if update_data:
                        db.update_session(sid, **update_data)
                        logger.info(f"Session updated: {sid[:12]}")
                continue
            
            if etype == "session.deleted":
                if db.get_session(sid):
                    db.delete_session(sid)
                    app.bot_data.get("statuses", {}).pop(sid, None)
                    logger.info(f"Session deleted: {sid[:12]}")
                continue
            
            if etype == "session.status":
                session = db.get_session(sid)
                if session:
                    status = props.get("status", {})
                    state_type = status.get("type", "idle")
                    db.update_session(sid, status=state_type)
                    logger.debug(f"Session status: {sid[:12]} -> {state_type}")
                    
                    statuses = app.bot_data.get("statuses", {})
                    st = statuses.get(sid)
                    
                    if st:
                        if state_type == "idle":
                            if app.bot_data.get("active_status_session") == sid:
                                await _finish_status(app, sid)
                        elif state_type == "retry":
                            st["state"] = "busy"
                            st["last_text"] = status.get("message", "Retrying...")
                            if app.bot_data.get("active_status_session") == sid:
                                await _update_status_now(app, sid, force=True)
                        elif state_type == "busy":
                            st["state"] = "busy"
                            if app.bot_data.get("active_status_session") == sid:
                                await _update_status_now(app, sid, force=True)
                continue
            
            if etype == "session.idle":
                if db.get_session(sid):
                    db.update_session(sid, status="idle")
                    logger.info(f"Session idle: {sid[:12]}")
                    statuses = app.bot_data.get("statuses", {})
                    if sid in statuses and app.bot_data.get("active_status_session") == sid:
                        await _finish_status(app, sid)
                continue
            
            if etype == "session.error":
                if db.get_session(sid):
                    error = props.get("error", {})
                    error_name = error.get("name", "UnknownError")
                    error_msg = error.get("data", {}).get("message", str(error))
                    db.update_session(sid, status="error")
                    logger.error(f"Session error: {sid[:12]} - {error_msg}")
                    
                    statuses = app.bot_data.get("statuses", {})
                    if sid in statuses:
                        st = statuses[sid]
                        st["state"] = "error"
                        st["last_text"] = f"{error_name}: {error_msg}"
                        if app.bot_data.get("active_status_session") == sid:
                            await _update_status_now(app, sid, force=True)
                            await app.bot.send_message(ADMIN_ID, f"❌ *Error en sesión* `{sid[:12]}`:\n{error_msg}", parse_mode="Markdown")
                            await _finish_status(app, sid)
                continue
            
            if not sid or not db.get_session(sid):
                continue
            
            statuses = app.bot_data.setdefault("statuses", {})
            st = statuses.get(sid)
            
            if etype == "message.part.updated":
                part = props.get("part", {})
                part_type = part.get("type", "")
                
                if part_type == "text":
                    if not st:
                        st = statuses[sid] = {
                            "msg_id": None, "state": "busy", "tool": None,
                            "tools_seen": [], "files_edited": set(),
                            "message_count": 0, "last_text": "", "final_text": "",
                            "reasoning_text": None, "start_time": time.time(),
                            "last_update_time": time.time(),
                            "tokens_input": 0, "tokens_output": 0,
                        }
                    st["state"] = "busy"
                    text = part.get("text", "")
                    if text:
                        st["final_text"] = text
                        st["last_text"] = text
                    if app.bot_data.get("active_status_session") == sid and st.get("msg_id"):
                        await _update_status_now(app, sid)
                
                elif part_type == "reasoning":
                    if st:
                        st["state"] = "thinking"
                        text = part.get("text", "")
                        if text:
                            st["reasoning_text"] = text
                            st["last_text"] = text
                        if app.bot_data.get("active_status_session") == sid and st.get("msg_id"):
                            await _update_status_now(app, sid)
                
                elif part_type == "tool-call":
                    if st:
                        st["state"] = "busy"
                        tool_name = part.get("name", "")
                        if tool_name:
                            st["tool"] = tool_name
                            if tool_name not in st["tools_seen"]:
                                st["tools_seen"].append(tool_name)
                        if app.bot_data.get("active_status_session") == sid and st.get("msg_id"):
                            await _update_status_now(app, sid, force=True)
                continue
            
            if etype == "message.part.delta":
                field = props.get("field", "")
                delta = props.get("delta", "")
                
                if field == "text" and delta:
                    if not st:
                        st = statuses[sid] = {
                            "msg_id": None, "state": "busy", "tool": None,
                            "tools_seen": [], "files_edited": set(),
                            "message_count": 0, "last_text": "", "final_text": "",
                            "reasoning_text": None, "start_time": time.time(),
                            "last_update_time": time.time(),
                            "tokens_input": 0, "tokens_output": 0,
                        }
                    st["state"] = "busy"
                    st["last_text"] = (st.get("last_text") or "") + delta
                    st["final_text"] = (st.get("final_text") or "") + delta
                    if app.bot_data.get("active_status_session") == sid and st.get("msg_id"):
                        await _update_status_now(app, sid)
                continue
            
            if etype == "message.updated":
                info = props.get("info", {})
                if info.get("role") == "assistant":
                    if st:
                        tokens = info.get("tokens", {})
                        if tokens:
                            st["tokens_input"] = tokens.get("input", 0)
                            st["tokens_output"] = tokens.get("output", 0)
                        st["message_count"] = st.get("message_count", 0) + 1
                        if app.bot_data.get("active_status_session") == sid and st.get("msg_id"):
                            await _update_status_now(app, sid)
                continue
        
        except Exception as exc:
            logger.error(f"SSE handler error {etype}: {exc}", exc_info=True)

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
    """Folder selected → check if cwd open → sync sessions or show model picker."""
    q = update.callback_query; await q.answer()
    pk  = int(q.data.split(":")[1])
    cwd = _val(ctx, pk)
    ctx.bot_data["pending_cwd"] = cwd

    cwd_path = Path(cwd)
    
    # Si cwd ya está open, sincronizar sesiones y mostrar picker
    if db.is_cwd_open(cwd):
        await q.edit_message_text(f"📂 `{cwd_path.name}`\n⏳ Sincronizando sesiones...", parse_mode="Markdown")
        
        try:
            sessions = await oc.list_sessions()
            db.sync_sessions_from_opencode(cwd, sessions)
        except Exception as exc:
            await q.edit_message_text(f"❌ Error: {exc}", parse_mode="Markdown")
            return
        
        # Mostrar picker de sesiones + opción nueva sesión
        db_sessions = db.get_sessions_by_cwd(cwd)
        btns = [[InlineKeyboardButton("➕ Nueva sesión", callback_data=f"newsess:{pk}")]]
        
        for s in db_sessions[:8]:
            sid = s["session_id"]
            title = s.get("title") or sid[:12]
            status = s.get("status", "idle")
            mark = "🔴" if status == "busy" else "🟢" if status == "idle" else "⚠️"
            sk = _key(ctx, sid)
            btns.append([
                InlineKeyboardButton(f"{mark} {title[:20]}", callback_data=f"actsess:{sk}"),
                InlineKeyboardButton("🗑", callback_data=f"delsess:{sk}:{pk}")
            ])
        
        btns.append([InlineKeyboardButton("❌ Cerrar proyecto", callback_data=f"closecwd:{pk}")])
        
        await q.edit_message_text(
            f"📂 `{cwd_path.name}` ({len(db_sessions)} sesiones)",
            reply_markup=InlineKeyboardMarkup(btns),
            parse_mode="Markdown"
        )
    else:
        # Cwd no está open → mostrar model picker
        await q.edit_message_text(f"📂 `{cwd}`\n⏳ Cargando modelos...", parse_mode="Markdown")

        try:
            models = await _get_models(ctx)
        except Exception as exc:
            await q.edit_message_text(f"❌ Error al cargar modelos: {exc}", parse_mode="Markdown")
            return

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
            f"📂 `{cwd_path.name}`\n📦 Elige proveedor:",
            reply_markup=InlineKeyboardMarkup(btns),
            parse_mode="Markdown",
        )


async def cb_pickprov(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Provider selected → show models of that provider (paginated)."""
    q = update.callback_query; await q.answer()
    parts = q.data.split(":")
    pk = int(parts[1])
    pid_k = int(parts[2])
    page = int(parts[3]) if len(parts) > 3 else 0
    pid = _val(ctx, pid_k)
    cwd = _val(ctx, pk)
    cwd_path = Path(cwd)

    await q.edit_message_text(f"📂 `{cwd_path.name}`\n⏳ Cargando modelos de {pid}...", parse_mode="Markdown")

    try:
        models = await _get_models(ctx)
    except Exception as exc:
        await q.edit_message_text(f"❌ Error: {exc}", parse_mode="Markdown")
        return

    mids = sorted([m.get("id") or m.get("modelID", "?") for m in models if m.get("providerID") == pid])
    
    MODELS_PER_PAGE = 6
    total_pages = max(1, (len(mids) + MODELS_PER_PAGE - 1) // MODELS_PER_PAGE)
    page = max(0, min(page, total_pages - 1))
    chunk = mids[page * MODELS_PER_PAGE:(page + 1) * MODELS_PER_PAGE]
    
    btns = []
    row: list[InlineKeyboardButton] = []
    for mid in chunk:
        mk = _key(ctx, f"{pid}|{mid}")
        row.append(InlineKeyboardButton(mid, callback_data=f"pickmodel:{pk}:{mk}"))
        if len(row) == 2:
            btns.append(row); row = []
    if row:
        btns.append(row)
    
    mk0 = _key(ctx, "|")
    btns.append([InlineKeyboardButton("⚙ Defecto", callback_data=f"pickmodel:{pk}:{mk0}")])
    
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀", callback_data=f"pickprov:{pk}:{pid_k}:{page-1}"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("▶", callback_data=f"pickprov:{pk}:{pid_k}:{page+1}"))
    if nav:
        btns.append(nav)
    
    btns.append([InlineKeyboardButton("⬅ Volver", callback_data=f"os:{pk}")])
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])

    await q.edit_message_text(
        f"📂 `{cwd_path.name}`\n🧩 Modelos de *{pid}*  _{page+1}/{total_pages}_",
        reply_markup=InlineKeyboardMarkup(btns),
        parse_mode="Markdown",
    )


async def cb_pickmodel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Model selected → create session → open cwd → save to DB."""
    q = update.callback_query; await q.answer()
    _, pk, mk = q.data.split(":")
    cwd       = _val(ctx, int(pk))
    model_str = _val(ctx, int(mk))
    pid, mid  = model_str.split("|", 1) if "|" in model_str else ("", "")
    cwd_path = Path(cwd)

    await q.edit_message_text(f"📂 `{cwd_path.name}`\n⏳ Creando sesión...", parse_mode="Markdown")

    try:
        sess = await oc.create_session(
            directory=cwd,
            provider_id=pid if pid else None,
            model_id=mid if mid else None,
        )
    except Exception as exc:
        await q.edit_message_text(f"❌ Error al crear sesión: {exc}")
        return

    sid         = sess.get("id", "")
    title       = sess.get("title") or sid[:12]
    model_label = f"{pid}/{mid}" if pid else "default"
    created     = sess.get("time", {}).get("created")
    
    db.open_cwd(cwd)
    db.add_session(sid, cwd, title, f"{pid}/{mid}" if pid else None, created)
    db.set_active(sid)

    msg = f"""✅ Sesión creada
╭─────────────────────────────╮
│ 📦 Sesión: `{title}`
│ 📂 Proyecto: `{cwd_path.name}`
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


async def cb_newsess(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Nueva sesión para cwd ya abierto → model picker."""
    q = update.callback_query; await q.answer()
    pk = int(q.data.split(":")[1])
    cwd = _val(ctx, pk)
    cwd_path = Path(cwd)
    
    await q.edit_message_text(f"📂 `{cwd_path.name}`\n⏳ Cargando modelos...", parse_mode="Markdown")
    
    try:
        models = await _get_models(ctx)
    except Exception as exc:
        await q.edit_message_text(f"❌ Error: {exc}", parse_mode="Markdown")
        return
    
    groups: dict[str, list] = defaultdict(list)
    for m in models:
        pid = m.get("providerID", "?")
        mid = m.get("id") or m.get("modelID", "?")
        groups[pid].append(mid)
    
    btns = []
    for pid in sorted(groups):
        btns.append([InlineKeyboardButton(f"🔹 {pid}", callback_data=f"pickprov:{pk}:{_key(ctx, pid)}")])
    btns.append([InlineKeyboardButton("⬅ Volver", callback_data=f"os:{pk}")])
    
    await q.edit_message_text(
        f"📂 `{cwd_path.name}`\n📦 Elige proveedor:",
        reply_markup=InlineKeyboardMarkup(btns),
        parse_mode="Markdown",
    )


async def cb_actsess(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Activar sesión."""
    q = update.callback_query; await q.answer()
    sk = int(q.data.split(":")[1])
    sid = _val(ctx, sk)
    
    session = db.get_session(sid)
    if not session:
        await q.edit_message_text("⚠️ Sesión no encontrada.")
        return
    
    db.set_active(sid)
    cwd_name = Path(session.get("cwd", "")).name
    title = session.get("title") or sid[:12]
    model = session.get("model") or "default"
    
    await q.edit_message_text(
        f"✅ Activada: `{title}`\n📂 `{cwd_name}` | 🧩 `{model}`",
        parse_mode="Markdown"
    )


async def cb_delsess(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Borrar sesión."""
    q = update.callback_query; await q.answer()
    parts = q.data.split(":")
    sk = int(parts[1])
    pk = int(parts[2]) if len(parts) > 2 else None
    sid = _val(ctx, sk)
    
    try:
        await oc.delete_session(sid)
    except Exception as exc:
        await q.edit_message_text(f"❌ Error: {exc}", parse_mode="Markdown")
        return
    
    db.delete_session(sid)
    ctx.application.bot_data.get("statuses", {}).pop(sid, None)
    
    # Si estaba activa, limpiar
    active = db.get_active()
    if active and active.get("session_id") == sid:
        db.clear_active()
    
    # Si tenemos pk, volver a la lista de sesiones
    if pk:
        cwd = _val(ctx, pk)
        cwd_path = Path(cwd)
        db_sessions = db.get_sessions_by_cwd(cwd)
        
        btns = [[InlineKeyboardButton("➕ Nueva sesión", callback_data=f"newsess:{pk}")]]
        for s in db_sessions[:8]:
            sid2 = s["session_id"]
            title2 = s.get("title") or sid2[:12]
            status = s.get("status", "idle")
            mark = "🔴" if status == "busy" else "🟢" if status == "idle" else "⚠️"
            sk2 = _key(ctx, sid2)
            btns.append([
                InlineKeyboardButton(f"{mark} {title2[:20]}", callback_data=f"actsess:{sk2}"),
                InlineKeyboardButton("🗑", callback_data=f"delsess:{sk2}:{pk}")
            ])
        btns.append([InlineKeyboardButton("❌ Cerrar proyecto", callback_data=f"closecwd:{pk}")])
        
        await q.edit_message_text(
            f"✅ Sesión borrada\n📂 `{cwd_path.name}` ({len(db_sessions)} sesiones)",
            reply_markup=InlineKeyboardMarkup(btns),
            parse_mode="Markdown"
        )
    else:
        await q.edit_message_text(f"✅ Sesión `{sid[:12]}` borrada.", parse_mode="Markdown")


async def cb_closecwd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Cerrar proyecto (cwd) y todas sus sesiones."""
    q = update.callback_query; await q.answer()
    pk = int(q.data.split(":")[1])
    cwd = _val(ctx, pk)
    cwd_path = Path(cwd)
    
    sessions = db.get_sessions_by_cwd(cwd)
    
    for s in sessions:
        sid = s["session_id"]
        try:
            await oc.delete_session(sid)
        except Exception:
            pass
        ctx.application.bot_data.get("statuses", {}).pop(sid, None)
    
    db.close_cwd(cwd)
    
    # Si la sesión activa era de este cwd, limpiar
    active = db.get_active()
    if active and active.get("cwd") == cwd:
        db.clear_active()
    
    await q.edit_message_text(
        f"✅ Proyecto cerrado\n📂 `{cwd_path.name}` ({len(sessions)} sesiones borradas)",
        parse_mode="Markdown"
    )


async def cb_abort(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Abortar la sesión activa de OpenCode."""
    q = update.callback_query; await q.answer()
    active = db.get_active()
    if not active:
        await q.edit_message_text("⚠️ No hay sesión activa.")
        return
    sid = active["session_id"]
    try:
        await oc.abort_session(sid)
    except Exception as exc:
        await q.edit_message_text(f"⚠️ Error al cancelar: {exc}")
        return
    
    statuses = ctx.application.bot_data.get("statuses", {})
    st = statuses.pop(sid, None)
    
    if ctx.application.bot_data.get("active_status_session") == sid:
        ctx.application.bot_data.pop("active_status_session", None)
        for job in ctx.application.job_queue.get_jobs_by_name("status_heartbeat"):
            job.schedule_removal()
    
    db.update_session(sid, status="idle")
    await q.edit_message_text("🛑 Tarea cancelada.")


@admin_only
async def cmd_esc(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Cancelar la tarea actual de OpenCode."""
    active = db.get_active()
    if not active:
        await update.message.reply_text("⚠️ No hay sesión activa.")
        return
    sid = active["session_id"]
    try:
        await oc.abort_session(sid)
    except Exception as exc:
        await update.message.reply_text(f"⚠️ Error al cancelar: {exc}")
        return
    
    statuses = ctx.application.bot_data.get("statuses", {})
    statuses.pop(sid, None)
    
    if ctx.application.bot_data.get("active_status_session") == sid:
        ctx.application.bot_data.pop("active_status_session", None)
        for job in ctx.application.job_queue.get_jobs_by_name("status_heartbeat"):
            job.schedule_removal()
    
    db.update_session(sid, status="idle")
    await update.message.reply_text("🛑 Tarea cancelada.")

# ---------------------------------------------------------------------------
# /close
# ---------------------------------------------------------------------------

@admin_only
async def cmd_close(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cwds = db.get_all_open_cwds()
    
    if not cwds:
        await update.message.reply_text("No hay proyectos abiertos.")
        return
    
    btns = []
    for cwd in sorted(cwds):
        sessions = db.get_sessions_by_cwd(cwd)
        count = len(sessions)
        ck = _key(ctx, cwd)
        btns.append([InlineKeyboardButton(
            f"{Path(cwd).name}  ({count})",
            callback_data=f"cl:{ck}"
        )])
    
    await update.message.reply_text(
        "Cerrar proyecto (elimina todas sus sesiones):",
        reply_markup=InlineKeyboardMarkup(btns),
    )


async def cb_cl(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    ck  = int(q.data.split(":")[1])
    cwd = _val(ctx, ck)
    cwd_path = Path(cwd)
    
    # Obtener TODAS las sesiones de OpenCode para este cwd (incluidas las de la web)
    try:
        all_sessions = await oc.list_sessions()
        to_delete = [s for s in all_sessions if s.get("directory") == cwd]
    except Exception:
        to_delete = []
    
    # Borrar en OpenCode
    for s in to_delete:
        sid = s.get("id", "")
        try:
            await oc.delete_session(sid)
        except Exception:
            pass
        ctx.application.bot_data.get("statuses", {}).pop(sid, None)
    
    # Limpiar BD (todas las que conocemos + activa)
    db.close_cwd(cwd)
    
    await q.edit_message_text(
        f"✅ Proyecto cerrado\n📂 `{cwd_path.name}` ({len(to_delete)} sesiones)",
        parse_mode="Markdown"
    )

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
    
    active = db.get_active()
    if not active:
        await q.edit_message_text("⚠️ No hay proyecto activo.")
        return
    
    cwd = active.get("cwd")
    
    try:
        sess = await oc.create_session(directory=cwd)
    except Exception as exc:
        await q.edit_message_text(f"Failed: {exc}"); return
    
    sid = sess.get("id", "")
    title = sess.get("title") or sid[:12]
    model_obj = sess.get("model", {})
    model = f"{model_obj.get('providerID', '')}/{model_obj.get('id', '')}" if model_obj else None
    created = sess.get("time", {}).get("created")
    
    db.add_session(sid, cwd, title, model, created)
    db.set_active(sid)
    await q.edit_message_text(f"Session `{title}` created and activated.", parse_mode="Markdown")


async def cb_sa(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    sid = _val(ctx, int(q.data.split(":")[1]))
    
    session = db.get_session(sid)
    if not session:
        await q.edit_message_text("⚠️ Session not in DB.")
        return
    
    db.set_active(sid)
    
    title = session.get("title") or sid[:12]
    cwd_name = Path(session.get("cwd", "")).name or "?"
    await q.edit_message_text(f"Active: `{title}` @ `{cwd_name}`", parse_mode="Markdown")


async def cb_sd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    sid = _val(ctx, int(q.data.split(":")[1]))
    try:
        await oc.delete_session(sid)
    except Exception as exc:
        await q.edit_message_text(f"Failed: {exc}"); return
    
    db.delete_session(sid)
    ctx.application.bot_data.get("statuses", {}).pop(sid, None)
    
    await q.edit_message_text(f"Session `{sid[:8]}` deleted.", parse_mode="Markdown")


async def cb_sda(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    active = db.get_active()
    cwd = active.get("cwd") if active else None
    
    try:
        sessions = await oc.list_sessions()
        for s in sessions:
            sid = s["id"]
            await oc.delete_session(sid)
            db.delete_session(sid)
            ctx.application.bot_data.get("statuses", {}).pop(sid, None)
    except Exception as exc:
        await q.edit_message_text(f"Error: {exc}"); return
    
    await q.edit_message_text(f"All sessions deleted from `{Path(cwd or '?').name}`.", parse_mode="Markdown")

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
        models = await _get_models(ctx)
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
    """Provider selected (from /models) → show models of that provider (paginated)."""
    q = update.callback_query; await q.answer()
    parts = q.data.split(":")
    pid_k = int(parts[1])
    page = int(parts[2]) if len(parts) > 2 else 0
    pid = _val(ctx, pid_k)

    await q.edit_message_text(f"⏳ Cargando modelos de {pid}...")

    try:
        models = await _get_models(ctx)
    except Exception as exc:
        await q.edit_message_text(f"❌ Error: {exc}")
        return

    mids = sorted([m.get("id") or m.get("modelID", "?") for m in models if m.get("providerID") == pid])
    
    MODELS_PER_PAGE = 6
    total_pages = max(1, (len(mids) + MODELS_PER_PAGE - 1) // MODELS_PER_PAGE)
    page = max(0, min(page, total_pages - 1))
    chunk = mids[page * MODELS_PER_PAGE:(page + 1) * MODELS_PER_PAGE]
    
    btns = []
    row: list[InlineKeyboardButton] = []
    for mid in chunk:
        mk = _key(ctx, f"{pid}|{mid}")
        row.append(InlineKeyboardButton(mid, callback_data=f"modelset:{mk}"))
        if len(row) == 2:
            btns.append(row); row = []
    if row:
        btns.append(row)
    
    mk0 = _key(ctx, "|")
    btns.append([InlineKeyboardButton("⚙ Defecto", callback_data=f"modelset:{mk0}")])
    
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀", callback_data=f"modelprov:{pid_k}:{page-1}"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("▶", callback_data=f"modelprov:{pid_k}:{page+1}"))
    if nav:
        btns.append(nav)
    
    btns.append([InlineKeyboardButton("⬅ Volver", callback_data="models:")])
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])

    await q.edit_message_text(f"🧩 Modelos de *{pid}*  _{page+1}/{total_pages}_", reply_markup=InlineKeyboardMarkup(btns), parse_mode="Markdown")


async def cb_modelset(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Model selected from /models → set as active model."""
    q = update.callback_query; await q.answer()
    mk = int(q.data.split(":")[1])
    model_str = _val(ctx, mk)
    active = db.get_active()
    if active:
        sid = active["session_id"]
        
        if model_str and "|" in model_str:
            pid, mid = model_str.split("|", 1)
            try:
                await oc.update_session_model(sid, pid, mid)
            except Exception as exc:
                logger.warning(f"Could not update session model: {exc}")
        
        db.update_session(sid, model=model_str or None)
        
        session = db.get_session(sid)
        title = session.get("title") or sid[:12] if session else sid[:12]
        cwd_name = Path(session.get("cwd", "")).name if session else "?"
        
        msg = f"""✅ Modelo establecido
╭─────────────────────────────╮
│ 📦 Sesión: `{title}`
│ 📂 Proyecto: `{cwd_name}`
│ 🧩 Modelo: `{model_str or 'default'}`
╰─────────────────────────────╯"""
        
        await q.edit_message_text(msg, parse_mode="Markdown")


async def cb_ms(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    model_str = _val(ctx, int(q.data.split(":")[1]))
    active    = db.get_active()
    if active:
        sid = active["session_id"]
        db.update_session(sid, model=model_str or None)
    await q.edit_message_text(f"Model set to `{model_str}`", parse_mode="Markdown")

# ---------------------------------------------------------------------------
# Plain text → prompt_async
# ---------------------------------------------------------------------------

@admin_only
async def handle_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text   = update.message.text
    active = db.get_active()

    if not active:
        await update.message.reply_text(
            "❌ No hay sesión activa.\nUsa /open para seleccionar un proyecto primero.",
            parse_mode="Markdown"
        )
        return

    model_str = active.get("model")
    pid, mid  = model_str.split("/", 1) if model_str and "/" in model_str else (None, None)
    sid = active["session_id"]
    cwd_name = Path(active.get("cwd", "")).name or "?"
    model_label = model_str or "default"

    status_msg = await update.message.reply_text(
        f"🔴 *BUSY* | 📂 `{cwd_name}` | 🧩 `{model_label}`\n"
        f"⏱ `00:00` | 💬 `0` msgs | 📝 `0` edits\n\n"
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
        statuses = ctx.application.bot_data.get("statuses", {})
        statuses.pop(sid, None)
        if ctx.application.bot_data.get("active_status_session") == sid:
            ctx.application.bot_data.pop("active_status_session", None)
            for job in ctx.application.job_queue.get_jobs_by_name("status_heartbeat"):
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
    app.add_handler(CallbackQueryHandler(cb_newsess,    pattern=r"^newsess:"))
    app.add_handler(CallbackQueryHandler(cb_actsess,    pattern=r"^actsess:"))
    app.add_handler(CallbackQueryHandler(cb_delsess,    pattern=r"^delsess:"))
    app.add_handler(CallbackQueryHandler(cb_closecwd,   pattern=r"^closecwd:"))
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

    async def post_shutdown(application: Application):
        await oc.close()

    app.post_shutdown = post_shutdown

    logger.info("Bot starting...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
