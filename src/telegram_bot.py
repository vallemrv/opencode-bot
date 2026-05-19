"""
Telegram bot — remote control for OpenCode server.

Architecture:
- OpenCode Server manages all projects and sessions natively.
- Bot uses ?directory=<path> param to scope all API calls to a project.
- DB only stores the active session (session_id + directory).
- SSE /global/event streams events for all sessions across all projects.

Flow:
  /open  → browse folders → "Open here" → if project has sessions: session picker
                                        → new session: model picker → create
  text   → prompt_async → status message (live via SSE) → final reply
"""

import os
import asyncio
import logging
import time
from pathlib import Path
from collections import defaultdict, deque

from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
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

MODELS_CACHE_TTL = 300

async def _get_models(ctx: ContextTypes.DEFAULT_TYPE) -> list[dict]:
    cache = ctx.bot_data.get("models_cache")
    if cache and (time.time() - cache["ts"]) < MODELS_CACHE_TTL:
        return cache["data"]
    models = await oc.list_models()
    ctx.bot_data["models_cache"] = {"ts": time.time(), "data": models}
    return models

# ---------------------------------------------------------------------------
# Auth guard
# ---------------------------------------------------------------------------

def admin_only(func):
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != ADMIN_ID:
            return
        return await func(update, ctx)
    return wrapper

# ---------------------------------------------------------------------------
# Key store — compress long strings into short int keys for callback_data
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
# Helpers
# ---------------------------------------------------------------------------

async def _delete_msg(bot, chat_id: int, msg_id: int):
    try:
        await bot.delete_message(chat_id=chat_id, message_id=msg_id)
    except Exception:
        pass

def _format_elapsed(seconds: float) -> str:
    mins = int(seconds // 60)
    secs = int(seconds % 60)
    return f"{mins:02d}:{secs:02d}"

# ---------------------------------------------------------------------------
# Status tracking
# ---------------------------------------------------------------------------
# app.bot_data["statuses"] = {
#   session_id: {
#     "msg_id": int | None,
#     "directory": str,
#     "state": str,           # busy | thinking | idle | error
#     "tool": str | None,
#     "tools_seen": [str],
#     "files_edited": set(),
#     "message_count": int,
#     "last_text": str | None,
#     "final_text": str | None,
#     "reasoning_text": str | None,
#     "start_time": float,
#     "last_update_time": float,
#     "tokens_input": int,
#     "tokens_output": int,
#   }
# }
# app.bot_data["queues"] = {
#   session_id: deque([{"text": str, "directory": str}])   # pending prompts
# }
# app.bot_data["msg_to_session"] = OrderedDict {
#   message_id (int): {"session_id": str, "directory": str}
# }  — max MSG_TRACK_LIMIT entries, oldest evicted first

STATUS_INTERVAL = 30
STATUS_THROTTLE = 5
MSG_TRACK_LIMIT = 200


def _track_msg(app: Application, message_id: int, session_id: str, directory: str):
    """Register a bot message so replies can be routed to the right session."""
    from collections import OrderedDict
    store: OrderedDict = app.bot_data.setdefault("msg_to_session", OrderedDict())
    store[message_id] = {"session_id": session_id, "directory": directory}
    while len(store) > MSG_TRACK_LIMIT:
        store.popitem(last=False)


def _resolve_target(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> dict | None:
    """
    Resolve which session a message targets.
    Priority: reply-to-bot-message > active session.
    Returns {"session_id": ..., "directory": ...} or None.
    """
    reply = update.message.reply_to_message
    if reply and reply.from_user and reply.from_user.is_bot:
        store = ctx.bot_data.get("msg_to_session", {})
        target = store.get(reply.message_id)
        if target:
            return target
    # Fallback: active session
    active = db.get_active()
    if active:
        return {"session_id": active["session_id"], "directory": active["directory"]}
    return None


def _build_status_text(st: dict) -> str:
    state      = st.get("state", "busy")
    tool       = st.get("tool")
    files      = st.get("files_edited", set())
    msgs       = st.get("message_count", 0)
    tools_seen = st.get("tools_seen", [])
    directory  = st.get("directory", "")
    cwd_name   = Path(directory).name or "?"
    model      = st.get("model") or "default"
    reasoning  = st.get("reasoning_text") or ""

    elapsed_str = _format_elapsed(time.time() - st.get("start_time", time.time()))
    icons = {"busy": "🔴", "thinking": "🤔", "idle": "🟢", "error": "❌"}
    icon  = icons.get(state, "⚪")

    lines = [
        f"{icon} *{state.upper()}*",
        f"📂 `{cwd_name}` | 🧩 `{model}`",
        f"⏱ `{elapsed_str}` | 💬 `{msgs}` msgs | 📝 `{len(files)}` edits",
    ]
    if tool:
        lines.append(f"🔧 `{tool}`")
    if tools_seen:
        lines.append(f"⚡ `{len(set(tools_seen))}` herramientas")
    if reasoning and state == "thinking":
        snippet = reasoning[-150:].replace("`", "'").replace("*", "")
        lines.append(f"💭 _{snippet}_")
    lines.append("")
    lines.append("_Pulsa_ /esc _para cancelar_")
    return "\n".join(lines)


async def _update_status_now(app: Application, session_id: str, force: bool = False):
    statuses = app.bot_data.get("statuses", {})
    st = statuses.get(session_id)
    if not st or not st.get("msg_id"):
        return
    now = time.time()
    if not force and (now - st.get("last_update_time", 0)) < STATUS_THROTTLE:
        return
    st["last_update_time"] = now
    try:
        await app.bot.edit_message_text(
            chat_id=ADMIN_ID,
            message_id=st["msg_id"],
            text=_build_status_text(st),
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Cancelar", callback_data="abort:")
            ]]),
        )
    except BadRequest:
        pass


async def _heartbeat_loop(ctx: ContextTypes.DEFAULT_TYPE):
    statuses = ctx.bot_data.get("statuses", {})
    for sid in list(statuses.keys()):
        await _update_status_now(ctx.application, sid, force=True)


def _start_status(app: Application, session_id: str, directory: str, msg_id: int, model: str = "default"):
    statuses = app.bot_data.setdefault("statuses", {})
    statuses[session_id] = {
        "msg_id": msg_id,
        "directory": directory,
        "model": model,
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
    # ensure heartbeat job
    if not app.job_queue.get_jobs_by_name("status_heartbeat"):
        app.job_queue.run_repeating(
            _heartbeat_loop,
            interval=STATUS_INTERVAL,
            first=STATUS_INTERVAL,
            name="status_heartbeat",
        )


async def _finish_status(app: Application, session_id: str):
    statuses = app.bot_data.get("statuses", {})
    st = statuses.pop(session_id, None)

    # stop heartbeat if no more active statuses
    if not statuses:
        for job in app.job_queue.get_jobs_by_name("status_heartbeat"):
            job.schedule_removal()

    if st and st.get("msg_id"):
        await _delete_msg(app.bot, ADMIN_ID, st["msg_id"])

    reply_text = st.get("final_text") if st else None
    directory  = st.get("directory") if st else None

    if not reply_text and directory:
        try:
            messages = await oc.get_messages(session_id, directory=directory)
            for m in reversed(messages):
                if m.get("role") == "assistant":
                    parts = m.get("parts", [])
                    texts = [p.get("text", "") for p in parts if p.get("type") == "text" and p.get("text")]
                    if texts:
                        reply_text = "\n".join(texts)
                        break
        except Exception as exc:
            logger.error(f"Failed to get messages: {exc}")

    cwd_name = Path(directory or "").name or "?"

    if not reply_text:
        sent = await app.bot.send_message(ADMIN_ID, f"✅ _{cwd_name}_ Listo.", parse_mode="Markdown")
        _track_msg(app, sent.message_id, session_id, directory or "")
        return

    chunk_size = 3900
    chunks = [reply_text[i:i+chunk_size] for i in range(0, len(reply_text), chunk_size)]
    last_sent = None
    for i, chunk in enumerate(chunks):
        header = f"✅ _{cwd_name}_\n\n" if i == 0 else ""
        kbd = None
        if i == len(chunks) - 1 and reply_text.rstrip().endswith("?"):
            kbd = InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Ignorar", callback_data="cancel:")
            ]])
        last_sent = await app.bot.send_message(
            ADMIN_ID,
            f"{header}{chunk}",
            parse_mode="Markdown",
            reply_markup=kbd,
        )
    if last_sent:
        _track_msg(app, last_sent.message_id, session_id, directory or "")

    # Process next queued message for this session, if any
    await _drain_queue(app, session_id)


async def _drain_queue(app: Application, session_id: str):
    """Send next pending prompt from the local queue for this session."""
    queues = app.bot_data.get("queues", {})
    q = queues.get(session_id)
    if not q:
        return
    item = q.popleft()
    if not q:
        queues.pop(session_id, None)

    text      = item["text"]
    directory = item["directory"]
    cwd_name  = Path(directory).name or "?"

    try:
        await oc.send_message_async(session_id, text, directory=directory)
        remaining = len(q) if q else 0
        note = f" ({remaining} más en cola)" if remaining else ""
        await app.bot.send_message(
            ADMIN_ID,
            f"📨 Enviando siguiente mensaje a `{cwd_name}`{note}...",
            parse_mode="Markdown",
        )
    except Exception as exc:
        await app.bot.send_message(ADMIN_ID, f"❌ Error al enviar mensaje encolado: {exc}")


# SSE listener — global, no DB filtering
# ---------------------------------------------------------------------------

async def sse_listener(app: Application) -> None:
    logger.info("SSE listener started")
    async for event in oc.event_stream():
        payload = event.get("payload", {})
        if not payload:
            continue

        etype = payload.get("type", "")
        props = payload.get("properties", {})
        sid   = props.get("sessionID", "")

        logger.debug(f"SSE {etype} sid={sid[:12] if sid else '-'}")

        try:
            if etype in ("server.connected", "session.created", "session.updated", "session.deleted"):
                # No local DB to update — OpenCode handles it natively
                continue

            if not sid:
                continue

            statuses = app.bot_data.setdefault("statuses", {})
            st = statuses.get(sid)

            # ---- session status ----
            if etype == "session.status":
                status     = props.get("status", {})
                state_type = status.get("type", "idle")

                if state_type in ("busy", "retry"):
                    if not st:
                        # Session just started processing — create status msg
                        try:
                            sess_info = await oc.get_session(sid)
                            directory = sess_info.get("directory", "")
                            model_obj = sess_info.get("model", {})
                            model_label = (
                                f"{model_obj.get('providerID','')}/{model_obj.get('id','')}"
                                if model_obj else "default"
                            )
                        except Exception:
                            directory   = ""
                            model_label = "default"
                        cwd_name = Path(directory).name or "?"
                        status_msg = await app.bot.send_message(
                            ADMIN_ID,
                            f"🔴 *BUSY* | 📂 `{cwd_name}` | 🧩 `{model_label}`\n"
                            f"⏱ `00:00` | 💬 `0` msgs | 📝 `0` edits\n\n"
                            f"_Pulsa_ /esc _para cancelar_",
                            parse_mode="Markdown",
                            reply_markup=InlineKeyboardMarkup([[
                                InlineKeyboardButton("❌ Cancelar", callback_data="abort:")
                            ]]),
                        )
                        _start_status(app, sid, directory, status_msg.message_id, model=model_label)
                        _track_msg(app, status_msg.message_id, sid, directory)
                        st = app.bot_data["statuses"].get(sid)

                    if st:
                        st["state"] = "busy"
                        if state_type == "retry":
                            st["last_text"] = status.get("message", "Retrying...")
                        await _update_status_now(app, sid, force=True)

                elif state_type == "idle":
                    if st:
                        await _finish_status(app, sid)
                continue

            if etype == "session.idle":
                if st:
                    await _finish_status(app, sid)
                continue

            if etype == "session.error":
                if st:
                    error     = props.get("error", {})
                    error_msg = error.get("data", {}).get("message", str(error))
                    st["state"] = "error"
                    await _update_status_now(app, sid, force=True)
                    await app.bot.send_message(
                        ADMIN_ID,
                        f"❌ *Error* `{sid[:12]}`:\n{error_msg}",
                        parse_mode="Markdown",
                    )
                    await _finish_status(app, sid)
                continue

            # ---- streaming parts ----
            if etype == "message.part.updated":
                part      = props.get("part", {})
                part_type = part.get("type", "")

                if not st:
                    continue

                if part_type == "text":
                    st["state"] = "busy"
                    text = part.get("text", "")
                    if text:
                        st["final_text"] = text
                        st["last_text"]  = text
                    await _update_status_now(app, sid)

                elif part_type == "reasoning":
                    st["state"] = "thinking"
                    text = part.get("text", "")
                    if text:
                        st["reasoning_text"] = text
                    await _update_status_now(app, sid)

                elif part_type == "tool-call":
                    st["state"] = "busy"
                    tool_name = part.get("name", "")
                    if tool_name:
                        st["tool"] = tool_name
                        if tool_name not in st["tools_seen"]:
                            st["tools_seen"].append(tool_name)
                    await _update_status_now(app, sid, force=True)
                continue

            if etype == "message.part.delta":
                field = props.get("field", "")
                delta = props.get("delta", "")
                if field == "text" and delta and st:
                    st["state"]      = "busy"
                    st["last_text"]  = (st.get("last_text") or "") + delta
                    st["final_text"] = (st.get("final_text") or "") + delta
                    await _update_status_now(app, sid)
                continue

            if etype == "message.updated":
                info = props.get("info", {})
                if info.get("role") == "assistant" and st:
                    tokens = info.get("tokens", {})
                    if tokens:
                        st["tokens_input"]  = tokens.get("input", 0)
                        st["tokens_output"] = tokens.get("output", 0)
                    st["message_count"] = st.get("message_count", 0) + 1
                    await _update_status_now(app, sid)
                continue

        except Exception as exc:
            logger.error(f"SSE handler error [{etype}]: {exc}", exc_info=True)

# ---------------------------------------------------------------------------
# Folder browser
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
    """Folder selected → check if project has sessions → session picker or model picker."""
    q = update.callback_query; await q.answer()
    cwd      = _val(ctx, int(q.data.split(":")[1]))
    cwd_path = Path(cwd)
    pk       = _key(ctx, cwd)

    await q.edit_message_text(f"📂 `{cwd_path.name}`\n⏳ Consultando OpenCode...", parse_mode="Markdown")

    try:
        sessions = await oc.list_sessions(directory=cwd)
    except Exception as exc:
        await q.edit_message_text(f"❌ Error: {exc}", parse_mode="Markdown")
        return

    if sessions:
        await _show_session_picker(q, ctx, cwd, sessions)
    else:
        await _show_provider_picker(q, ctx, cwd)


async def _show_session_picker(q, ctx, cwd: str, sessions: list[dict]):
    """Show existing sessions for this project + option to create new one."""
    cwd_path = Path(cwd)
    pk = _key(ctx, cwd)
    active = db.get_active()
    active_sid = (active or {}).get("session_id")

    btns = [[InlineKeyboardButton("➕ Nueva sesión", callback_data=f"newsess:{pk}")]]
    for s in sessions[:8]:
        sid   = s.get("id", "")
        title = s.get("title") or sid[:12]
        mark  = " ✅" if sid == active_sid else ""
        sk    = _key(ctx, sid)
        btns.append([
            InlineKeyboardButton(f"{title[:22]}{mark}", callback_data=f"actsess:{sk}:{pk}"),
            InlineKeyboardButton("🗑", callback_data=f"delsess:{sk}:{pk}"),
        ])
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])

    await q.edit_message_text(
        f"📂 `{cwd_path.name}` — {len(sessions)} sesiones",
        reply_markup=InlineKeyboardMarkup(btns),
        parse_mode="Markdown",
    )


async def _show_provider_picker(q, ctx, cwd: str | None, skip_loading: bool = False):
    """
    Shared provider picker used by both the /open wizard and /models.
    cwd = directory path when creating a new session (wizard mode).
    cwd = None when changing the model of the active session (/models mode).
    skip_loading = True if the caller already showed a loading message.
    """
    pk       = _key(ctx, cwd) if cwd else 0
    cwd_name = Path(cwd).name if cwd else None
    header   = f"📂 `{cwd_name}`\n" if cwd_name else ""

    if not skip_loading:
        await q.edit_message_text(f"{header}⏳ Cargando modelos...", parse_mode="Markdown")

    try:
        models = await _get_models(ctx)
    except Exception as exc:
        await q.edit_message_text(f"❌ Error al cargar modelos: {exc}", parse_mode="Markdown")
        return

    groups: dict[str, list] = defaultdict(list)
    for m in models:
        groups[m.get("providerID", "?")].append(m.get("id") or m.get("modelID", "?"))

    btns = []
    for pid in sorted(groups):
        btns.append([InlineKeyboardButton(f"🔹 {pid}", callback_data=f"prov:{pk}:{_key(ctx, pid)}:0")])
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])

    await q.edit_message_text(
        f"{header}📦 Elige proveedor:",
        reply_markup=InlineKeyboardMarkup(btns),
        parse_mode="Markdown",
    )


async def cb_prov(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Shared provider→model list. Works for both /open wizard and /models."""
    q = update.callback_query; await q.answer()
    parts = q.data.split(":")
    pk    = int(parts[1])
    pid_k = int(parts[2])
    page  = int(parts[3]) if len(parts) > 3 else 0
    pid   = _val(ctx, pid_k)
    cwd   = _val(ctx, pk) if pk != 0 else None

    try:
        models = await _get_models(ctx)
    except Exception as exc:
        await q.edit_message_text(f"❌ Error: {exc}", parse_mode="Markdown")
        return

    mids = sorted([m.get("id") or m.get("modelID", "?") for m in models if m.get("providerID") == pid])
    PER  = 6
    total_pages = max(1, (len(mids) + PER - 1) // PER)
    page  = max(0, min(page, total_pages - 1))
    chunk = mids[page * PER:(page + 1) * PER]

    btns = []
    row: list[InlineKeyboardButton] = []
    for mid in chunk:
        mk = _key(ctx, f"{pid}|{mid}")
        row.append(InlineKeyboardButton(mid, callback_data=f"provmodel:{pk}:{mk}"))
        if len(row) == 2:
            btns.append(row); row = []
    if row:
        btns.append(row)

    btns.append([InlineKeyboardButton("⚙ Defecto del proveedor", callback_data=f"provmodel:{pk}:{_key(ctx, f'{pid}|')}")])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀", callback_data=f"prov:{pk}:{pid_k}:{page-1}"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("▶", callback_data=f"prov:{pk}:{pid_k}:{page+1}"))
    if nav:
        btns.append(nav)

    # Back button: wizard → session picker / models → provider list
    if cwd:
        btns.append([InlineKeyboardButton("⬅ Proveedores", callback_data=f"os:{pk}")])
    else:
        btns.append([InlineKeyboardButton("⬅ Proveedores", callback_data=f"prov:0:0:0")])
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])

    await q.edit_message_text(
        f"🧩 *{pid}*  _{page+1}/{total_pages}_",
        reply_markup=InlineKeyboardMarkup(btns),
        parse_mode="Markdown",
    )


async def cb_provmodel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    Shared model-selected handler.
    pk == 0  → /models mode: update model of active session.
    pk != 0  → wizard mode: create new session in cwd.
    """
    q = update.callback_query; await q.answer()
    parts     = q.data.split(":")
    pk        = int(parts[1])
    model_str = _val(ctx, int(parts[2]))
    pid, mid  = model_str.split("|", 1) if "|" in model_str else ("", "")

    if pk == 0:
        # /models mode — update model of active session
        active = db.get_active()
        if not active:
            await q.edit_message_text("⚠️ No hay sesión activa.")
            return
        sid       = active["session_id"]
        directory = active["directory"]
        if pid and mid:
            try:
                await oc.update_session(sid, directory=directory, model={"providerID": pid, "id": mid})
            except Exception as exc:
                logger.warning(f"Could not update session model: {exc}")
        await q.edit_message_text(
            f"✅ Modelo: `{model_str or 'default'}`",
            parse_mode="Markdown",
        )
    else:
        # wizard mode — create new session
        cwd = _val(ctx, pk)
        await q.edit_message_text(f"📂 `{Path(cwd).name}`\n⏳ Creando sesión...", parse_mode="Markdown")
        try:
            sess = await oc.create_session(
                directory=cwd,
                provider_id=pid or None,
                model_id=mid or None,
            )
        except Exception as exc:
            await q.edit_message_text(f"❌ Error al crear sesión: {exc}")
            return
        sid         = sess.get("id", "")
        title       = sess.get("title") or sid[:12]
        model_label = f"{pid}/{mid}" if pid and mid else (pid or "default")
        db.set_active(sid, cwd)
        await q.edit_message_text(
            f"✅ Sesión creada\n"
            f"📦 `{title}`\n"
            f"📂 `{Path(cwd).name}` | 🧩 `{model_label}`\n\n"
            f"Envía tu primer prompt.",
            parse_mode="Markdown",
        )


async def cb_newsess(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Nueva sesión para proyecto que ya tiene sesiones."""
    q = update.callback_query; await q.answer()
    pk  = int(q.data.split(":")[1])
    cwd = _val(ctx, pk)
    await _show_provider_picker(q, ctx, cwd)


async def cb_actsess(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Activate an existing session."""
    q = update.callback_query; await q.answer()
    parts = q.data.split(":")
    sid   = _val(ctx, int(parts[1]))
    pk    = int(parts[2]) if len(parts) > 2 else None
    cwd   = _val(ctx, pk) if pk is not None else ""

    # If we don't have the directory, try to look it up from OpenCode
    if not cwd:
        try:
            sess_info = await oc.get_session(sid)
            cwd = sess_info.get("directory", "")
        except Exception:
            pass

    db.set_active(sid, cwd)
    cwd_name = Path(cwd).name or "?"

    await q.edit_message_text(
        f"✅ Sesión activa\n📂 `{cwd_name}`",
        parse_mode="Markdown",
    )


async def cb_delsess(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Delete a session and refresh the session picker."""
    q = update.callback_query; await q.answer()
    parts = q.data.split(":")
    sid   = _val(ctx, int(parts[1]))
    pk    = int(parts[2]) if len(parts) > 2 else None
    cwd   = _val(ctx, pk) if pk is not None else ""

    try:
        await oc.delete_session(sid, directory=cwd or None)
    except Exception as exc:
        await q.edit_message_text(f"❌ Error: {exc}", parse_mode="Markdown")
        return

    ctx.application.bot_data.get("statuses", {}).pop(sid, None)

    active = db.get_active()
    if active and active.get("session_id") == sid:
        db.clear_active()

    # Refresh picker if we know the cwd
    if cwd:
        try:
            sessions = await oc.list_sessions(directory=cwd)
        except Exception:
            sessions = []
        if sessions:
            await _show_session_picker(q, ctx, cwd, sessions)
        else:
            pk = _key(ctx, cwd)
            await q.edit_message_text(
                f"✅ Sesión borrada. No quedan sesiones en `{Path(cwd).name}`.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("➕ Nueva sesión", callback_data=f"newsess:{pk}")],
                    [InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")],
                ]),
                parse_mode="Markdown",
            )
    else:
        await q.edit_message_text(f"✅ Sesión `{sid[:12]}` borrada.", parse_mode="Markdown")


async def cb_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    await q.edit_message_text("❌ Cancelado.")


# ---------------------------------------------------------------------------
# /close — close (delete all sessions of) a project
# ---------------------------------------------------------------------------

@admin_only
async def cmd_close(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Show projects that have sessions, choose one to close."""
    await update.message.reply_text("⏳ Cargando proyectos...")

    try:
        projects = await oc.list_projects()
        all_sessions = await oc.list_sessions()
    except Exception as exc:
        await update.message.reply_text(f"❌ Error: {exc}")
        return

    # Group sessions by directory
    by_dir: dict[str, list] = defaultdict(list)
    for s in all_sessions:
        d = s.get("directory", "")
        if d:
            by_dir[d].append(s)

    # Map worktree → project for name lookup
    proj_by_wt = {p.get("worktree", ""): p for p in projects}

    if not by_dir:
        await update.message.reply_text("No hay proyectos con sesiones.")
        return

    btns = []
    for directory, sessions in sorted(by_dir.items()):
        p    = proj_by_wt.get(directory)
        name = (p or {}).get("name") or Path(directory).name
        ck   = _key(ctx, directory)
        btns.append([InlineKeyboardButton(
            f"📂 {name}  ({len(sessions)} sesiones)",
            callback_data=f"closedir:{ck}",
        )])
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])

    await update.message.reply_text(
        "Selecciona proyecto a cerrar (borra todas sus sesiones):",
        reply_markup=InlineKeyboardMarkup(btns),
    )


async def cb_closedir(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    ck        = int(q.data.split(":")[1])
    directory = _val(ctx, ck)

    try:
        sessions = await oc.list_sessions(directory=directory)
    except Exception as exc:
        await q.edit_message_text(f"❌ Error: {exc}")
        return

    for s in sessions:
        sid = s.get("id", "")
        try:
            await oc.delete_session(sid, directory=directory)
        except Exception:
            pass
        ctx.application.bot_data.get("statuses", {}).pop(sid, None)

    active = db.get_active()
    if active and active.get("directory") == directory:
        db.clear_active()

    await q.edit_message_text(
        f"✅ `{Path(directory).name}` cerrado ({len(sessions)} sesiones borradas)",
        parse_mode="Markdown",
    )


# ---------------------------------------------------------------------------
# /sessions — manage sessions of the active project
# ---------------------------------------------------------------------------

@admin_only
async def cmd_sessions(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    active = db.get_active()
    if not active:
        await update.message.reply_text("No hay sesión activa. Usa /open primero.")
        return

    directory = active["directory"]

    try:
        sessions = await oc.list_sessions(directory=directory)
    except Exception as exc:
        await update.message.reply_text(f"❌ Error: {exc}")
        return

    active_sid = active["session_id"]
    pk         = _key(ctx, directory)

    btns = [[InlineKeyboardButton("➕ Nueva sesión", callback_data=f"newsess:{pk}")]]
    if sessions:
        btns.append([InlineKeyboardButton("🗑 Borrar todas", callback_data=f"sda:{pk}")])

    for s in sessions[:8]:
        sid   = s.get("id", "")
        title = s.get("title") or sid[:12]
        mark  = " ✅" if sid == active_sid else ""
        sk    = _key(ctx, sid)
        btns.append([
            InlineKeyboardButton(f"{title[:22]}{mark}", callback_data=f"actsess:{sk}:{pk}"),
            InlineKeyboardButton("🗑", callback_data=f"delsess:{sk}:{pk}"),
        ])

    await update.message.reply_text(
        f"Sesiones de `{Path(directory).name}`",
        reply_markup=InlineKeyboardMarkup(btns),
        parse_mode="Markdown",
    )


async def cb_sda(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Delete all sessions for a project."""
    q = update.callback_query; await q.answer()
    pk        = int(q.data.split(":")[1])
    directory = _val(ctx, pk)

    try:
        sessions = await oc.list_sessions(directory=directory)
        for s in sessions:
            sid = s.get("id", "")
            await oc.delete_session(sid, directory=directory)
            ctx.application.bot_data.get("statuses", {}).pop(sid, None)
    except Exception as exc:
        await q.edit_message_text(f"❌ Error: {exc}")
        return

    active = db.get_active()
    if active and active.get("directory") == directory:
        db.clear_active()

    await q.edit_message_text(
        f"✅ Todas las sesiones de `{Path(directory).name}` borradas.",
        parse_mode="Markdown",
    )


# ---------------------------------------------------------------------------
# /models — change model for active session
# ---------------------------------------------------------------------------

@admin_only
async def cmd_models(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    active = db.get_active()
    if not active:
        await update.message.reply_text("❌ No hay sesión activa. Usa /open primero.")
        return
    msg = await update.message.reply_text("⏳ Cargando modelos...")

    class _MsgWrapper:
        async def edit_message_text(self, text, reply_markup=None, parse_mode=None):
            await msg.edit_text(text, reply_markup=reply_markup, parse_mode=parse_mode)

    await _show_provider_picker(_MsgWrapper(), ctx, cwd=None, skip_loading=True)


# ---------------------------------------------------------------------------
# /esc + abort callback
# ---------------------------------------------------------------------------

async def _do_abort(app: Application, sid: str, directory: str) -> str:
    try:
        await oc.abort_session(sid, directory=directory or None)
    except Exception as exc:
        return f"⚠️ Error al cancelar: {exc}"

    app.bot_data.get("statuses", {}).pop(sid, None)
    return "🛑 Tarea cancelada."


@admin_only
async def cmd_esc(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    active = db.get_active()
    if not active:
        await update.message.reply_text("⚠️ No hay sesión activa.")
        return
    msg = await _do_abort(ctx.application, active["session_id"], active["directory"])
    await update.message.reply_text(msg)


async def cb_abort(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    active = db.get_active()
    if not active:
        await q.edit_message_text("⚠️ No hay sesión activa.")
        return
    msg = await _do_abort(ctx.application, active["session_id"], active["directory"])
    await q.edit_message_text(msg)


# ---------------------------------------------------------------------------
# Plain text → prompt
# ---------------------------------------------------------------------------

@admin_only
async def handle_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text

    # /send flow: explicit target from picker takes highest priority
    send_target = ctx.bot_data.pop("send_target", None)
    if send_target:
        sid       = send_target["session_id"]
        directory = send_target["directory"]
    else:
        target = _resolve_target(update, ctx)
        if not target:
            await update.message.reply_text(
                "❌ No hay sesión activa. Usa /open para seleccionar un proyecto."
            )
            return
        sid       = target["session_id"]
        directory = target["directory"]
    cwd_name = Path(directory).name or "?"

    # If session is currently busy, queue the message locally and notify
    statuses = ctx.bot_data.get("statuses", {})
    if sid in statuses:
        queues = ctx.bot_data.setdefault("queues", {})
        q = queues.setdefault(sid, deque())
        q.append({"text": text, "directory": directory})
        pos = len(q)
        await update.message.reply_text(
            f"⏳ `{cwd_name}` ocupado. Mensaje encolado (posición {pos}).\n"
            f"Se enviará cuando OpenCode termine la tarea actual.",
            parse_mode="Markdown",
        )
        return

    try:
        await oc.send_message_async(sid, text, directory=directory)
    except Exception as exc:
        await update.message.reply_text(f"❌ Error al enviar: {exc}")
        return

    sent = await update.message.reply_text(
        f"📨 Enviado a `{cwd_name}` — esperando respuesta...",
        parse_mode="Markdown",
    )
    _track_msg(ctx.application, sent.message_id, sid, directory)


# ---------------------------------------------------------------------------
# /projects — list all projects that have sessions
# ---------------------------------------------------------------------------

@admin_only
async def cmd_projects(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        projects     = await oc.list_projects()
        all_sessions = await oc.list_sessions()
    except Exception as exc:
        await update.message.reply_text(f"❌ Error: {exc}")
        return

    by_dir: dict[str, list] = defaultdict(list)
    for s in all_sessions:
        d = s.get("directory", "")
        if d:
            by_dir[d].append(s)

    if not by_dir:
        await update.message.reply_text("No hay proyectos con sesiones abiertas.")
        return

    proj_by_wt = {p.get("worktree", ""): p for p in projects}
    active     = db.get_active()
    active_dir = (active or {}).get("directory", "")
    active_sid = (active or {}).get("session_id", "")

    lines = ["*Proyectos abiertos*\n"]
    btns  = []
    for directory in sorted(by_dir.keys()):
        sessions = by_dir[directory]
        p        = proj_by_wt.get(directory)
        name     = (p or {}).get("name") or Path(directory).name
        is_active = directory == active_dir

        # Which session is "current" for this project
        cur_sess = next((s for s in sessions if s.get("id") == active_sid), sessions[0])
        sess_title = cur_sess.get("title") or cur_sess.get("id", "")[:12]
        marker = " ◀" if is_active else ""

        lines.append(
            f"📂 *{name}*{marker}\n"
            f"   📦 `{sess_title}`\n"
            f"   {len(sessions)} sesión{'es' if len(sessions) != 1 else ''}"
        )

        if not is_active:
            dk = _key(ctx, directory)
            sk = _key(ctx, cur_sess.get("id", ""))
            btns.append([InlineKeyboardButton(
                f"▶ Activar {name}",
                callback_data=f"projact:{dk}:{sk}",
            )])

    kbd = InlineKeyboardMarkup(btns) if btns else None
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown", reply_markup=kbd)


async def cb_projact(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Activate a project's session from /projects."""
    q = update.callback_query; await q.answer()
    parts     = q.data.split(":")
    directory = _val(ctx, int(parts[1]))
    sid       = _val(ctx, int(parts[2]))

    db.set_active(sid, directory)
    cwd_name = Path(directory).name

    # Rebuild the message without the button for the now-active project
    await q.edit_message_text(
        f"✅ Proyecto activo: `{cwd_name}`\n\nUsa /projects para ver el estado actualizado.",
        parse_mode="Markdown",
    )


# ---------------------------------------------------------------------------
# /send — send a prompt to a specific project's active session
# ---------------------------------------------------------------------------

@admin_only
async def cmd_send(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Pick a project, then type a prompt for it."""
    try:
        all_sessions = await oc.list_sessions()
    except Exception as exc:
        await update.message.reply_text(f"❌ Error: {exc}")
        return

    by_dir: dict[str, list] = defaultdict(list)
    for s in all_sessions:
        d = s.get("directory", "")
        if d:
            by_dir[d].append(s)

    if not by_dir:
        await update.message.reply_text("No hay proyectos con sesiones. Usa /open primero.")
        return

    active     = db.get_active()
    active_dir = (active or {}).get("directory", "")

    btns = []
    for directory in sorted(by_dir.keys()):
        sessions = by_dir[directory]
        name     = Path(directory).name
        marker   = " ✅" if directory == active_dir else ""
        dk       = _key(ctx, directory)
        btns.append([InlineKeyboardButton(
            f"📂 {name}{marker}  ({len(sessions)} sesiones)",
            callback_data=f"sendpick:{dk}",
        )])
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])

    await update.message.reply_text(
        "¿A qué proyecto envías el prompt?",
        reply_markup=InlineKeyboardMarkup(btns),
    )


async def cb_sendpick(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Project selected for /send → show its sessions to pick one."""
    q = update.callback_query; await q.answer()
    dk        = int(q.data.split(":")[1])
    directory = _val(ctx, dk)

    try:
        sessions = await oc.list_sessions(directory=directory)
    except Exception as exc:
        await q.edit_message_text(f"❌ Error: {exc}")
        return

    active    = db.get_active()
    active_sid = (active or {}).get("session_id", "")

    if len(sessions) == 1:
        # Only one session — go straight to prompt input
        sid = sessions[0].get("id", "")
        ctx.bot_data["send_target"] = {"session_id": sid, "directory": directory}
        title = sessions[0].get("title") or sid[:12]
        await q.edit_message_text(
            f"📂 `{Path(directory).name}` · `{title}`\n\nEscribe el prompt:",
            parse_mode="Markdown",
        )
        return

    btns = []
    for s in sessions[:8]:
        sid   = s.get("id", "")
        title = s.get("title") or sid[:12]
        mark  = " ✅" if sid == active_sid else ""
        sk    = _key(ctx, sid)
        btns.append([InlineKeyboardButton(f"{title[:28]}{mark}", callback_data=f"sendsess:{sk}:{dk}")])
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])

    await q.edit_message_text(
        f"📂 `{Path(directory).name}` — elige sesión:",
        reply_markup=InlineKeyboardMarkup(btns),
        parse_mode="Markdown",
    )


async def cb_sendsess(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Session selected for /send → ask for prompt text."""
    q = update.callback_query; await q.answer()
    parts = q.data.split(":")
    sid   = _val(ctx, int(parts[1]))
    dk    = int(parts[2])
    directory = _val(ctx, dk)

    ctx.bot_data["send_target"] = {"session_id": sid, "directory": directory}

    try:
        sess_info = await oc.get_session(sid, directory=directory)
        title     = sess_info.get("title") or sid[:12]
    except Exception:
        title = sid[:12]

    await q.edit_message_text(
        f"📂 `{Path(directory).name}` · `{title}`\n\nEscribe el prompt:",
        parse_mode="Markdown",
    )


# ---------------------------------------------------------------------------
# /start
# ---------------------------------------------------------------------------

@admin_only
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        connected = await oc.ping()
    except Exception:
        connected = False

    status_icon = "✅" if connected else "❌"
    active      = db.get_active()

    if active:
        sid       = active["session_id"]
        directory = active["directory"]
        cwd_name  = Path(directory).name

        session_title = sid[:12]
        model_label   = "default"
        state_icon    = "⚪"
        try:
            sess_info   = await oc.get_session(sid, directory=directory)
            session_title = sess_info.get("title") or sid[:12]
            model_obj     = sess_info.get("model", {})
            if model_obj:
                model_label = f"{model_obj.get('providerID','')}/{model_obj.get('id','')}"
        except Exception:
            pass

        msg = (
            f"🔗 OpenCode: {status_icon} `{OC_HOST}:{OC_PORT}`\n\n"
            f"*Sesión activa*\n"
            f"📂 Proyecto: `{cwd_name}`\n"
            f"📦 Sesión: `{session_title}`\n"
            f"🧩 Modelo: `{model_label}`\n\n"
            f"*Comandos*\n"
            f"/open — abrir proyecto o cambiar sesión\n"
            f"/projects — ver todos los proyectos con sesiones abiertas\n"
            f"/send — enviar prompt a un proyecto específico\n"
            f"/sessions — sesiones del proyecto activo (`{cwd_name}`)\n"
            f"/models — modelo de la sesión activa (`{cwd_name}`)\n"
            f"/close — borrar todas las sesiones de un proyecto\n"
            f"/esc — cancelar la tarea en curso"
        )
    else:
        try:
            all_sessions = await oc.list_sessions()
            total = len(all_sessions)
        except Exception:
            total = 0
        msg = (
            f"🔗 OpenCode: {status_icon} `{OC_HOST}:{OC_PORT}`\n\n"
            f"⚠️ Sin sesión activa ({total} sesiones en el servidor)\n\n"
            f"*Comandos*\n"
            f"/open — navega carpetas, elige proyecto y modelo, crea sesión\n"
            f"/projects — ver todos los proyectos con sesiones abiertas\n"
            f"/send — enviar prompt a un proyecto específico\n"
            f"/sessions — ver y gestionar sesiones del proyecto actual\n"
            f"/models — cambiar el modelo de la sesión activa\n"
            f"/close — borrar todas las sesiones de un proyecto\n"
            f"/esc — cancelar la tarea en curso"
        )

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
    app.add_handler(CommandHandler("projects", cmd_projects))
    app.add_handler(CommandHandler("send",     cmd_send))

    app.add_handler(CallbackQueryHandler(cb_ob,        pattern=r"^ob:"))
    app.add_handler(CallbackQueryHandler(cb_os,        pattern=r"^os:"))
    app.add_handler(CallbackQueryHandler(cb_prov,      pattern=r"^prov:"))
    app.add_handler(CallbackQueryHandler(cb_provmodel, pattern=r"^provmodel:"))
    app.add_handler(CallbackQueryHandler(cb_newsess,   pattern=r"^newsess:"))
    app.add_handler(CallbackQueryHandler(cb_actsess,   pattern=r"^actsess:"))
    app.add_handler(CallbackQueryHandler(cb_delsess,   pattern=r"^delsess:"))
    app.add_handler(CallbackQueryHandler(cb_closedir,  pattern=r"^closedir:"))
    app.add_handler(CallbackQueryHandler(cb_sda,       pattern=r"^sda:"))
    app.add_handler(CallbackQueryHandler(cb_abort,     pattern=r"^abort:"))
    app.add_handler(CallbackQueryHandler(cb_cancel,    pattern=r"^cancel:"))
    app.add_handler(CallbackQueryHandler(cb_sendpick,  pattern=r"^sendpick:"))
    app.add_handler(CallbackQueryHandler(cb_sendsess,  pattern=r"^sendsess:"))
    app.add_handler(CallbackQueryHandler(cb_projact,   pattern=r"^projact:"))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    app.job_queue.run_once(
        lambda c: asyncio.ensure_future(sse_listener(app)), when=1
    )

    async def post_init(application: Application):
        await application.bot.set_my_commands([
            BotCommand("start",    "Estado y menú"),
            BotCommand("open",     "Abrir proyecto / sesión"),
            BotCommand("projects", "Ver proyectos con sesiones abiertas"),
            BotCommand("send",     "Enviar prompt a un proyecto específico"),
            BotCommand("close",    "Cerrar proyecto"),
            BotCommand("sessions", "Gestionar sesiones del proyecto activo"),
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
