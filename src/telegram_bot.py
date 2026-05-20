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

import shutil
import db
import transcription as grok_stt
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

# Directory of this bot — excluded from /sessions and /close listings
BOT_DIR = str(Path(__file__).parent.parent.resolve())

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

def _key_raw(bot_data: dict, value: str) -> int:
    """Same as _key but takes bot_data directly (for use outside handlers)."""
    store = bot_data.setdefault("ks", {})
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
    sess_title = st.get("session_title") or ""

    elapsed_str = _format_elapsed(time.time() - st.get("start_time", time.time()))
    icons = {"busy": "🔴", "thinking": "🤔", "idle": "🟢", "error": "❌"}
    icon  = icons.get(state, "⚪")

    # Model: show only the model id part (after slash) to save space
    model_short = model.split("/")[-1] if "/" in model else model

    title_part = f" · `{sess_title[:20]}`" if sess_title else ""
    lines = [
        f"{icon} *{state.upper()}* | 📂 `{cwd_name}`{title_part}",
        f"🧩 `{model_short}` | ⏱ `{elapsed_str}`",
    ]

    # Files edited (only show if any)
    if files:
        files_str = ", ".join(f"`{f}`" for f in list(files)[:4])
        if len(files) > 4:
            files_str += f" +{len(files)-4}"
        lines.append(f"📝 {files_str}")

    # Current tool being called
    if tool:
        lines.append(f"🔧 `{tool}`")

    # All tools seen (compact list, deduplicated)
    unique_tools = list(dict.fromkeys(tools_seen))  # preserve order, deduplicate
    if unique_tools and not tool:
        # Only show tool history when not currently running a tool
        tools_str = " · ".join(f"`{t}`" for t in unique_tools[-5:])
        lines.append(f"⚡ {tools_str}")

    # Reasoning snippet (only when actively thinking)
    if reasoning and state == "thinking":
        snippet = reasoning[-200:].replace("`", "'").replace("*", "").strip()
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


def _start_status(app: Application, session_id: str, directory: str, msg_id: int, model: str = "default", session_title: str = ""):
    statuses = app.bot_data.setdefault("statuses", {})
    statuses[session_id] = {
        "msg_id": msg_id,
        "directory": directory,
        "model": model,
        "session_title": session_title,
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

    # Fetch messages once — used both for fallback text and token info
    fetched_messages = None
    if directory:
        try:
            fetched_messages = await oc.get_messages(session_id, directory=directory)
        except Exception as exc:
            logger.error(f"Failed to get messages: {exc}")

    if not reply_text and fetched_messages:
        for m in reversed(fetched_messages):
            info  = m.get("info", {})
            role  = info.get("role") or m.get("role")
            parts = m.get("parts", [])
            if role == "assistant":
                texts = [p.get("text", "") for p in parts if isinstance(p, dict) and p.get("type") == "text" and p.get("text")]
                if texts:
                    reply_text = "\n".join(texts)
                    break

    cwd_name = Path(directory or "").name or "?"

    # Build model + context % info (reuse fetched_messages for token count)
    model_info = ""
    try:
        session_data = await oc.get_session(session_id, directory=directory)
        model_obj    = session_data.get("model") or {}
        provider_id  = model_obj.get("providerID", "")
        model_id     = model_obj.get("id", "")

        total_tokens = 0
        if fetched_messages:
            for m in reversed(fetched_messages):
                info = m.get("info", {})
                if info.get("role") == "assistant":
                    tok = info.get("tokens", {}) or {}
                    cache = tok.get("cache", {}) or {}
                    total_tokens = (tok.get("input", 0) or 0) + (cache.get("read", 0) or 0) + (cache.get("write", 0) or 0)
                    break

        model_short = model_id.split("/")[-1] if "/" in (model_id or "") else (model_id or "")
        ctx_str = ""
        if provider_id and model_id:
            ctx_limit = await oc.get_model_context_limit(provider_id, model_id)
            if ctx_limit and ctx_limit > 0 and total_tokens > 0:
                pct = round(total_tokens / ctx_limit * 100, 1)
                ctx_str = f" ctx {pct}%"
        if model_short:
            model_info = f"`{model_short}`{ctx_str}"
    except Exception as exc:
        logger.warning(f"Could not get session info for footer: {exc}")

    # Elapsed time
    elapsed = ""
    if st and st.get("start_time"):
        elapsed = f" ⏱{_format_elapsed(time.time() - st['start_time'])}"

    # Files edited summary
    files_edited = st.get("files_edited", set()) if st else set()
    files_str = ""
    if files_edited:
        names = list(files_edited)[:3]
        files_str = " 📝 " + ", ".join(f"`{f}`" for f in names)
        if len(files_edited) > 3:
            files_str += f" +{len(files_edited)-3}"

    header_parts = [f"✅ `{cwd_name}`"]
    if model_info:
        header_parts.append(model_info)
    if elapsed:
        header_parts.append(elapsed)
    header_line = " | ".join(header_parts)
    if files_str:
        header_line += files_str

    if not reply_text:
        sent = await app.bot.send_message(ADMIN_ID, f"{header_line}\n_Listo._", parse_mode="Markdown")
        _track_msg(app, sent.message_id, session_id, directory or "")
        return

    chunk_size = 3900
    chunks = [reply_text[i:i+chunk_size] for i in range(0, len(reply_text), chunk_size)]
    last_sent = None
    for i, chunk in enumerate(chunks):
        header = f"{header_line}\n" if i == 0 else ""
        kbd = None
        if i == len(chunks) - 1 and reply_text.rstrip().endswith("?"):
            kbd = InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Ignorar", callback_data="cancel:")
            ]])
        try:
            last_sent = await app.bot.send_message(
                ADMIN_ID,
                f"{header}{chunk}",
                parse_mode="Markdown",
                reply_markup=kbd,
            )
        except BadRequest:
            # Fallback: send as plain text if Markdown parsing fails
            try:
                last_sent = await app.bot.send_message(
                    ADMIN_ID,
                    f"{header}{chunk}",
                    reply_markup=kbd,
                )
            except Exception as exc2:
                logger.error(f"Failed to send final reply chunk: {exc2}")
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
        # Apply pending model if any
        pending_models = app.bot_data.get("pending_model", {})
        pending    = pending_models.pop(session_id, None)
        provider_id = pending["providerID"] if pending else None
        model_id    = pending["modelID"]    if pending else None
        await oc.send_message_async(session_id, text, directory=directory,
                                    provider_id=provider_id, model_id=model_id)
        remaining = len(q) if q else 0
        note = f" ({remaining} más en cola)" if remaining else ""
        await app.bot.send_message(
            ADMIN_ID,
            f"📨 Enviando siguiente mensaje a `{cwd_name}`{note}...",
            parse_mode="Markdown",
        )
    except Exception as exc:
        await app.bot.send_message(ADMIN_ID, f"❌ Error al enviar mensaje encolado: {exc}")


# ---------------------------------------------------------------------------
# Question tool handler
# ---------------------------------------------------------------------------

async def _handle_question_asked(app: Application, props: dict) -> None:
    """
    When the LLM calls the `question` tool, build inline keyboards for each
    question and send them to Telegram. The session stays paused until the
    user answers.

    props structure:
      {
        "id": "que...",
        "sessionID": "ses...",
        "questions": [
          { "header": "...", "question": "...", "options": [{"label":"..","description":".."},...],
            "multiple": false, "custom": true }
        ]
      }
    """
    req_id    = props.get("id", "")
    session_id = props.get("sessionID", "")
    questions  = props.get("questions", [])

    if not req_id or not questions:
        return

    # Resolve directory from statuses or active session
    directory = ""
    statuses  = app.bot_data.get("statuses", {})
    st = statuses.get(session_id)
    if st:
        directory = st.get("directory", "")
    if not directory:
        active = db.get_active()
        if active and active.get("session_id") == session_id:
            directory = active.get("directory", "")
    if not directory:
        # Fallback: any active session
        active = db.get_active()
        if active:
            directory = active.get("directory", "")

    # Store pending question state
    pending = app.bot_data.setdefault("pending_questions", {})
    pending[req_id] = {
        "session_id": session_id,
        "directory":  directory,
        "questions":  questions,
        "answers":    [None] * len(questions),  # one answer per question slot
        "msg_ids":    [],
    }

    # Store req_id in key-store so callbacks can retrieve it
    rk = _key_raw(app.bot_data, req_id)
    sk = _key_raw(app.bot_data, session_id)

    msg_ids = []
    for q_idx, q in enumerate(questions):
        header   = q.get("header", f"Pregunta {q_idx+1}")
        question = q.get("question", "")
        options  = q.get("options", [])
        multiple = q.get("multiple", False)
        custom   = q.get("custom", True)

        # Build option buttons (one per row)
        btns = []
        for opt_idx, opt in enumerate(options):
            label = opt.get("label", "")
            desc  = opt.get("description", "")
            ok    = _key_raw(app.bot_data, f"{req_id}|{q_idx}|{opt_idx}|{label}")
            btn_text = f"{label}"
            if desc and len(desc) < 40:
                btn_text = f"{label} — {desc}"
            btns.append([InlineKeyboardButton(
                btn_text[:64],
                callback_data=f"qans:{rk}:{sk}:{q_idx}:{ok}",
            )])

        # Custom answer + cancel row
        if custom:
            ck = _key_raw(app.bot_data, f"{req_id}|{q_idx}|custom")
            btns.append([InlineKeyboardButton(
                "✏️ Escribe tu propia respuesta",
                callback_data=f"qcustom:{rk}:{sk}:{q_idx}",
            )])
        btns.append([InlineKeyboardButton(
            "❌ Cancelar pregunta",
            callback_data=f"qreject:{rk}:{sk}",
        )])

        sent = await app.bot.send_message(
            ADMIN_ID,
            f"❓ *{header}*\n\n{question}",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(btns),
        )
        msg_ids.append(sent.message_id)

    pending[req_id]["msg_ids"] = msg_ids


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

        logger.info(f"SSE {etype} sid={sid[:12] if sid else '-'}")

        try:
            if etype in ("server.connected", "session.created", "session.updated", "session.deleted"):
                # No local DB to update — OpenCode handles it natively
                continue

            # ---- question tool (LLM asks user a question) ----
            if etype == "question.asked":
                await _handle_question_asked(app, props)
                continue

            if etype == "question.replied" or etype == "question.rejected":
                req_id = props.get("requestID", "")
                q_data = app.bot_data.get("pending_questions", {}).pop(req_id, None)
                if q_data and q_data.get("msg_ids"):
                    for mid in q_data["msg_ids"]:
                        try:
                            await app.bot.edit_message_reply_markup(
                                chat_id=ADMIN_ID, message_id=mid, reply_markup=None
                            )
                        except Exception:
                            pass
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
                            sess_title = sess_info.get("title") or ""
                        except Exception:
                            directory   = ""
                            model_label = "default"
                            sess_title  = ""
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
                        _start_status(app, sid, directory, status_msg.message_id, model=model_label, session_title=sess_title)
                        _track_msg(app, status_msg.message_id, sid, directory)
                        st = app.bot_data["statuses"].get(sid)
                        # Delete the "Enviado a..." placeholder if present
                        pending_sent = app.bot_data.get("pending_sent_msgs", {}).pop(sid, None)
                        if pending_sent:
                            await _delete_msg(app.bot, ADMIN_ID, pending_sent)

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

            # ---- permission request (OpenCode asks for approval) ----
            if etype == "permission.updated":
                perm_id   = props.get("id", "")
                title     = props.get("title", "OpenCode solicita permiso")
                perm_type = props.get("type", "")
                meta      = props.get("metadata", {})
                p_sid     = props.get("sessionID", sid)

                # Try to get directory from statuses or active session
                p_dir = ""
                p_st  = statuses.get(p_sid)
                if p_st:
                    p_dir = p_st.get("directory", "")
                if not p_dir:
                    active = db.get_active()
                    if active and active.get("session_id") == p_sid:
                        p_dir = active.get("directory", "")

                pk  = _key_raw(app.bot_data, perm_id)
                sk  = _key_raw(app.bot_data, p_sid)
                dk  = _key_raw(app.bot_data, p_dir)

                # Build options: allow, deny + remember variants
                btns = [
                    [
                        InlineKeyboardButton("✅ Permitir", callback_data=f"perm:{sk}:{pk}:{dk}:allow"),
                        InlineKeyboardButton("✅ Permitir siempre", callback_data=f"perm:{sk}:{pk}:{dk}:allow_always"),
                    ],
                    [
                        InlineKeyboardButton("❌ Denegar", callback_data=f"perm:{sk}:{pk}:{dk}:deny"),
                        InlineKeyboardButton("❌ Denegar siempre", callback_data=f"perm:{sk}:{pk}:{dk}:deny_always"),
                    ],
                    [InlineKeyboardButton("✏️ Respuesta personalizada", callback_data=f"perminput:{sk}:{pk}:{dk}")],
                    [InlineKeyboardButton("🚫 Cancelar tarea", callback_data=f"permabort:{sk}:{dk}")],
                ]

                pattern_info = ""
                pattern = props.get("pattern")
                if pattern:
                    if isinstance(pattern, list):
                        pattern_info = "\n`" + "`, `".join(pattern[:5]) + "`"
                    else:
                        pattern_info = f"\n`{pattern}`"

                meta_info = ""
                for k, v in list(meta.items())[:3]:
                    meta_info += f"\n• `{k}`: `{str(v)[:60]}`"

                perm_msg = await app.bot.send_message(
                    ADMIN_ID,
                    f"🔐 *Permiso requerido*\n\n"
                    f"*{title}*\n"
                    f"Tipo: `{perm_type}`"
                    f"{pattern_info}"
                    f"{meta_info}\n\n"
                    f"_OpenCode está esperando tu respuesta para continuar._",
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup(btns),
                )
                # Track pending permissions
                perms = app.bot_data.setdefault("pending_perms", {})
                perms[perm_id] = {
                    "msg_id": perm_msg.message_id,
                    "session_id": p_sid,
                    "directory": p_dir,
                }
                continue

            if etype == "permission.replied":
                # Clean up tracked permission if any
                perm_id = props.get("permissionID", "")
                perms   = app.bot_data.get("pending_perms", {})
                perm    = perms.pop(perm_id, None)
                if perm and perm.get("msg_id"):
                    try:
                        await app.bot.edit_message_reply_markup(
                            chat_id=ADMIN_ID,
                            message_id=perm["msg_id"],
                            reply_markup=None,
                        )
                    except Exception:
                        pass
                continue

            # ---- streaming parts ----
            if etype == "message.part.updated":
                if not st:
                    continue
                part      = props.get("part", {})
                part_type = part.get("type", "")

                if part_type == "step-start":
                    # New step: clear current tool and last_text fragment,
                    # but preserve final_text accumulated so far across steps
                    st["last_text"] = None
                    st["tool"]      = None

                elif part_type == "text":
                    st["state"] = "busy"
                    text = part.get("text", "")
                    if text:
                        # updated events carry the full current text of this part;
                        # overwrite last_text (current part snapshot) but use it
                        # as the authoritative final_text for this step
                        st["last_text"]  = text
                        st["final_text"] = text
                    await _update_status_now(app, sid)

                elif part_type == "reasoning":
                    st["state"] = "thinking"
                    text = part.get("text", "")
                    if text:
                        st["reasoning_text"] = text
                    await _update_status_now(app, sid)

                elif part_type in ("tool-call", "tool"):
                    st["state"] = "busy"
                    tool_name  = part.get("name") or part.get("tool", "")
                    tool_input = part.get("input") or (part.get("state") or {}).get("input") or {}
                    if tool_name:
                        st["tool"] = tool_name
                        if tool_name not in st["tools_seen"]:
                            st["tools_seen"].append(tool_name)
                        EDIT_TOOLS = {"write", "edit", "patch", "fs_write", "str_replace_editor",
                                      "str_replace_based_edit_tool", "create_file", "write_file"}
                        if tool_name.lower() in EDIT_TOOLS or "write" in tool_name.lower() or "edit" in tool_name.lower():
                            fpath = (tool_input.get("path") or tool_input.get("file_path") or
                                     tool_input.get("filePath") or tool_input.get("target_file", ""))
                            if fpath:
                                st["files_edited"].add(Path(fpath).name)
                    await _update_status_now(app, sid, force=True)

                elif part_type == "patch":
                    files = part.get("files", [])
                    for f in files:
                        fname = Path(f).name if f else ""
                        if fname:
                            st["files_edited"].add(fname)

                continue

            if etype == "message.part.delta":
                if not st:
                    continue
                field = props.get("field", "")
                delta = props.get("delta", "")
                if field == "text" and delta:
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

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(f"SSE handler error [{etype}]: {exc}", exc_info=True)

def _is_bot_dir(directory: str) -> bool:
    """Return True if the directory is the bot's own project directory."""
    try:
        return Path(directory).resolve() == Path(BOT_DIR).resolve()
    except Exception:
        return False


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


async def _server_ok(reply_fn) -> bool:
    """Ping OpenCode server; if unreachable, send a message and return False."""
    try:
        ok = await oc.ping()
    except Exception:
        ok = False
    if not ok:
        await reply_fn(
            f"❌ OpenCode server no disponible en `{OC_HOST}:{OC_PORT}`.\n"
            "Arráncalo con `opencode serve` y vuelve a intentarlo.",
            parse_mode="Markdown",
        )
    return ok


@admin_only
async def cmd_open(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await _server_ok(update.message.reply_text):
        return
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
        # /models mode — store pending model, applied on next prompt
        active = db.get_active()
        if not active:
            await q.edit_message_text("⚠️ No hay sesión activa.")
            return
        sid       = active["session_id"]
        directory = active["directory"]
        if pid and mid:
            pending = ctx.bot_data.setdefault("pending_model", {})
            pending[sid] = {"providerID": pid, "modelID": mid}
        await q.edit_message_text(
            f"✅ Modelo `{model_str or 'default'}` aplicado al próximo prompt.",
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

    model_label = "default"
    sess_title  = sid[:12]
    try:
        sess_info   = await oc.get_session(sid, directory=cwd or None)
        sess_title  = sess_info.get("title") or sid[:12]
        model_obj   = sess_info.get("model", {})
        if model_obj:
            model_label = f"{model_obj.get('providerID','')}/{model_obj.get('id','')}"
    except Exception:
        pass

    await q.edit_message_text(
        f"✅ Sesión activa\n"
        f"📂 `{cwd_name}` · `{sess_title}`\n"
        f"🧩 `{model_label}`",
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
# Question tool callbacks
# ---------------------------------------------------------------------------

async def _send_question_answer(app: Application, req_id: str, session_id: str, answers: list):
    """Send the collected answers to OpenCode and clean up."""
    # Get directory from pending before popping
    q_data_pre = app.bot_data.get("pending_questions", {}).get(req_id, {})
    directory  = q_data_pre.get("directory", "")
    try:
        await oc.reply_question(req_id, answers, directory=directory or None)
    except Exception as exc:
        await app.bot.send_message(ADMIN_ID, f"❌ Error enviando respuesta: {exc}")
        return
    # Clean up pending
    q_data = app.bot_data.get("pending_questions", {}).pop(req_id, None)
    if q_data:
        for mid in q_data.get("msg_ids", []):
            try:
                await app.bot.edit_message_reply_markup(
                    chat_id=ADMIN_ID, message_id=mid, reply_markup=None
                )
            except Exception:
                pass


async def cb_qans(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Option button pressed for a question."""
    q = update.callback_query; await q.answer()
    parts  = q.data.split(":")
    rk     = int(parts[1])
    sk     = int(parts[2])
    q_idx  = int(parts[3])
    ok     = int(parts[4])

    req_id     = _val(ctx, rk)
    session_id = _val(ctx, sk)
    opt_str    = _val(ctx, ok)   # "req_id|q_idx|opt_idx|label"
    label      = opt_str.split("|", 3)[-1] if "|" in opt_str else opt_str

    pending = ctx.bot_data.get("pending_questions", {})
    q_data  = pending.get(req_id)
    if not q_data:
        await q.edit_message_text("⚠️ Pregunta ya respondida o expirada.")
        return

    questions   = q_data["questions"]
    n_questions = len(questions)

    # Record answer for this question slot
    q_data["answers"][q_idx] = [label]

    # Check if all questions have an answer
    if all(a is not None for a in q_data["answers"]):
        await q.edit_message_text(f"✅ Respuesta enviada: *{label}*", parse_mode="Markdown")
        await _send_question_answer(ctx.application, req_id, session_id, q_data["answers"])
    else:
        await q.edit_message_text(
            f"✅ Pregunta {q_idx+1}/{n_questions} respondida: *{label}*\n\n_Responde las demás preguntas._",
            parse_mode="Markdown",
        )


async def cb_qcustom(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """User wants to type a custom answer for a question."""
    q = update.callback_query; await q.answer()
    parts  = q.data.split(":")
    rk     = int(parts[1])
    sk     = int(parts[2])
    q_idx  = int(parts[3])

    req_id     = _val(ctx, rk)
    session_id = _val(ctx, sk)

    pending = ctx.bot_data.get("pending_questions", {})
    q_data  = pending.get(req_id)
    if not q_data:
        await q.edit_message_text("⚠️ Pregunta ya respondida o expirada.")
        return

    # Store custom answer context
    ctx.bot_data["question_custom_input"] = {
        "req_id":     req_id,
        "session_id": session_id,
        "q_idx":      q_idx,
        "msg_id":     q.message.message_id,
    }
    await q.edit_message_text(
        "✏️ Escribe tu respuesta a continuación:\n\n_El próximo mensaje que envíes se usará como respuesta._",
        parse_mode="Markdown",
    )


async def cb_qreject(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """User cancels/rejects a question."""
    q = update.callback_query; await q.answer()
    parts  = q.data.split(":")
    rk     = int(parts[1])
    req_id = _val(ctx, int(parts[1]))

    q_data    = ctx.bot_data.get("pending_questions", {}).get(req_id, {})
    directory = q_data.get("directory", "")

    try:
        await oc.reject_question(req_id, directory=directory or None)
    except Exception as exc:
        await q.edit_message_text(f"❌ Error: {exc}")
        return

    q_data = ctx.bot_data.get("pending_questions", {}).pop(req_id, None)
    if q_data:
        for mid in q_data.get("msg_ids", []):
            try:
                await ctx.bot.edit_message_reply_markup(
                    chat_id=ADMIN_ID, message_id=mid, reply_markup=None
                )
            except Exception:
                pass

    await q.edit_message_text("❌ Pregunta cancelada. OpenCode continuará sin respuesta.")


# ---------------------------------------------------------------------------
# Permission callbacks
# ---------------------------------------------------------------------------

async def cb_perm(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle permission allow/deny responses."""
    q = update.callback_query; await q.answer()
    parts     = q.data.split(":")
    # perm:sk:pk:dk:response  (pk=perm_id_key, dk=dir_key)
    sk        = int(parts[1])
    perm_k    = int(parts[2])
    dk        = int(parts[3])
    response  = parts[4]  # allow | allow_always | deny | deny_always

    sid       = _val(ctx, sk)
    perm_id   = _val(ctx, perm_k)
    directory = _val(ctx, dk)

    remember  = response.endswith("_always")
    resp_val  = response.replace("_always", "")  # "allow" or "deny"

    try:
        await oc.respond_permission(sid, perm_id, resp_val, remember=remember, directory=directory or None)
    except Exception as exc:
        await q.edit_message_text(f"❌ Error al responder permiso: {exc}", parse_mode="Markdown")
        return

    icons = {"allow": "✅", "deny": "❌"}
    icon  = icons.get(resp_val, "✅")
    note  = " (recordado)" if remember else ""
    await q.edit_message_text(
        f"{icon} Permiso *{resp_val}*{note}",
        parse_mode="Markdown",
    )
    # Remove from pending
    ctx.bot_data.get("pending_perms", {}).pop(perm_id, None)


async def cb_perminput(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Ask user to type a custom permission response."""
    q = update.callback_query; await q.answer()
    parts   = q.data.split(":")
    sk      = int(parts[1])
    perm_k  = int(parts[2])
    dk      = int(parts[3])

    # Store pending custom perm input
    ctx.bot_data["perm_input"] = {
        "session_id": _val(ctx, sk),
        "perm_id":    _val(ctx, perm_k),
        "directory":  _val(ctx, dk),
        "msg_id":     q.message.message_id,
    }
    await q.edit_message_text(
        "✏️ Escribe tu respuesta al permiso (ej: `allow`, `deny`, o texto libre):\n\n"
        "_El siguiente mensaje que envíes se usará como respuesta._",
        parse_mode="Markdown",
    )


async def cb_permabort(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Abort session when permission is pending."""
    q = update.callback_query; await q.answer()
    parts     = q.data.split(":")
    sk        = int(parts[1])
    dk        = int(parts[2])
    sid       = _val(ctx, sk)
    directory = _val(ctx, dk)

    msg = await _do_abort(ctx.application, sid, directory)
    await q.edit_message_text(msg)


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
    btns.append([InlineKeyboardButton("🗑 Cerrar todo del server", callback_data="closeall:")])
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])

    total = sum(len(v) for v in by_dir.values())
    await update.message.reply_text(
        f"Selecciona proyecto a quitar del bot, o cierra todo ({total} sesiones en {len(by_dir)} proyectos):",
        reply_markup=InlineKeyboardMarkup(btns),
    )


async def cb_closedir(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Project selected in /close → ask what to do."""
    q = update.callback_query; await q.answer()
    ck        = int(q.data.split(":")[1])
    directory = _val(ctx, ck)
    name      = Path(directory).name

    btns = [
        [InlineKeyboardButton("🗑 Borrar sesiones en OpenCode", callback_data=f"closedel:{ck}")],
        [InlineKeyboardButton("↩ Solo quitar sesión activa del bot", callback_data=f"closebot:{ck}")],
        [InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")],
    ]
    await q.edit_message_text(
        f"📂 `{name}` — ¿qué quieres hacer?",
        reply_markup=InlineKeyboardMarkup(btns),
        parse_mode="Markdown",
    )


async def cb_closedel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Delete all sessions of a project from OpenCode."""
    q = update.callback_query; await q.answer()
    ck        = int(q.data.split(":")[1])
    directory = _val(ctx, ck)

    try:
        sessions = await oc.list_sessions(directory=directory)
    except Exception as exc:
        await q.edit_message_text(f"❌ Error: {exc}")
        return

    deleted = 0
    for s in sessions:
        sid = s.get("id", "")
        try:
            await oc.delete_session(sid, directory=directory)
            ctx.application.bot_data.get("statuses", {}).pop(sid, None)
            deleted += 1
        except Exception:
            pass

    active = db.get_active()
    if active and active.get("directory") == directory:
        db.clear_active()

    await q.edit_message_text(
        f"✅ `{Path(directory).name}` cerrado — {deleted} sesiones borradas de OpenCode.",
        parse_mode="Markdown",
    )


async def cb_closebot(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Only clear active session in bot, keep OpenCode sessions intact."""
    q = update.callback_query; await q.answer()
    ck        = int(q.data.split(":")[1])
    directory = _val(ctx, ck)

    active = db.get_active()
    if active and active.get("directory") == directory:
        db.clear_active()

    await q.edit_message_text(
        f"✅ `{Path(directory).name}` quitado del bot (sesiones siguen en OpenCode).",
        parse_mode="Markdown",
    )


async def cb_closeall(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Delete ALL sessions from the OpenCode server and clear active session."""
    q = update.callback_query; await q.answer()

    await q.edit_message_text("⏳ Borrando todas las sesiones del server...")

    try:
        all_sessions = await oc.list_sessions()
    except Exception as exc:
        await q.edit_message_text(f"❌ Error: {exc}")
        return

    deleted = 0
    errors  = 0
    for s in all_sessions:
        sid = s.get("id", "")
        directory = s.get("directory") or s.get("_worktree") or None
        try:
            await oc.delete_session(sid, directory=directory)
            ctx.application.bot_data.get("statuses", {}).pop(sid, None)
            deleted += 1
        except Exception:
            errors += 1

    db.clear_active()

    msg = f"✅ {deleted} sesiones borradas del server."
    if errors:
        msg += f" ({errors} errores)"
    await q.edit_message_text(msg)


# ---------------------------------------------------------------------------
# /sessions — manage sessions of any project
# ---------------------------------------------------------------------------

@admin_only
async def cmd_sessions(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Show all projects so the user can pick one to manage its sessions."""
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
        n_sess = len(by_dir[directory])
        name   = Path(directory).name
        mark   = " ✅" if directory == active_dir else ""
        dk     = _key(ctx, directory)
        btns.append([InlineKeyboardButton(
            f"📂 {name}{mark}  ({n_sess} sesión{'es' if n_sess != 1 else ''})",
            callback_data=f"sesspick:{dk}",
        )])
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])

    await update.message.reply_text(
        "¿De qué proyecto quieres gestionar las sesiones?",
        reply_markup=InlineKeyboardMarkup(btns),
    )


async def cb_sesspick(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Project chosen in /sessions → show its session picker."""
    q = update.callback_query; await q.answer()
    dk        = int(q.data.split(":")[1])
    directory = _val(ctx, dk)

    try:
        sessions = await oc.list_sessions(directory=directory)
    except Exception as exc:
        await q.edit_message_text(f"❌ Error: {exc}")
        return

    active     = db.get_active()
    active_sid = (active or {}).get("session_id", "")
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
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])

    await q.edit_message_text(
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
# /models — change model for any session of any project
# ---------------------------------------------------------------------------

@admin_only
async def cmd_models(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Show all projects so the user can pick one to change a session's model."""
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
        n_sess = len(by_dir[directory])
        name   = Path(directory).name
        mark   = " ✅" if directory == active_dir else ""
        dk     = _key(ctx, directory)
        btns.append([InlineKeyboardButton(
            f"📂 {name}{mark}  ({n_sess} sesión{'es' if n_sess != 1 else ''})",
            callback_data=f"modpick:{dk}",
        )])
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])

    await update.message.reply_text(
        "¿De qué proyecto quieres cambiar el modelo?",
        reply_markup=InlineKeyboardMarkup(btns),
    )


async def cb_modpick(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Project chosen in /models → show its sessions."""
    q = update.callback_query; await q.answer()
    dk        = int(q.data.split(":")[1])
    directory = _val(ctx, dk)

    try:
        sessions = await oc.list_sessions(directory=directory)
    except Exception as exc:
        await q.edit_message_text(f"❌ Error: {exc}")
        return

    if not sessions:
        await q.edit_message_text(f"No hay sesiones en `{Path(directory).name}`.", parse_mode="Markdown")
        return

    active     = db.get_active()
    active_sid = (active or {}).get("session_id", "")

    if len(sessions) == 1:
        # Only one session — go straight to provider picker
        sid = sessions[0].get("id", "")
        sk  = _key(ctx, sid)
        await _show_model_provider_picker(q, ctx, directory, sid)
        return

    btns = []
    for s in sessions[:8]:
        sid   = s.get("id", "")
        title = s.get("title") or sid[:12]
        mark  = " ✅" if sid == active_sid else ""
        sk    = _key(ctx, sid)
        dk2   = _key(ctx, directory)
        btns.append([InlineKeyboardButton(f"{title[:28]}{mark}", callback_data=f"modsess:{sk}:{dk2}")])
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])

    await q.edit_message_text(
        f"📂 `{Path(directory).name}` — elige sesión:",
        reply_markup=InlineKeyboardMarkup(btns),
        parse_mode="Markdown",
    )


async def cb_modsess(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Session chosen in /models → show provider picker."""
    q = update.callback_query; await q.answer()
    parts     = q.data.split(":")
    sid       = _val(ctx, int(parts[1]))
    directory = _val(ctx, int(parts[2]))
    await _show_model_provider_picker(q, ctx, directory, sid)


async def _show_model_provider_picker(q, ctx, directory: str, sid: str):
    """Show provider list for the /models flow (targeting a specific session)."""
    sk       = _key(ctx, sid)
    cwd_name = Path(directory).name

    await q.edit_message_text(f"📂 `{cwd_name}`\n⏳ Cargando modelos...", parse_mode="Markdown")

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
        btns.append([InlineKeyboardButton(
            f"🔹 {pid}",
            callback_data=f"modprov:{sk}:{_key(ctx, pid)}:0",
        )])
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])

    await q.edit_message_text(
        f"📂 `{cwd_name}`\n📦 Elige proveedor:",
        reply_markup=InlineKeyboardMarkup(btns),
        parse_mode="Markdown",
    )


async def cb_modprov(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Provider chosen in /models → show model list."""
    q = update.callback_query; await q.answer()
    parts = q.data.split(":")
    sk    = int(parts[1])
    pid_k = int(parts[2])
    page  = int(parts[3]) if len(parts) > 3 else 0
    sid   = _val(ctx, sk)
    pid   = _val(ctx, pid_k)

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
        row.append(InlineKeyboardButton(mid, callback_data=f"setmodel:{sk}:{mk}"))
        if len(row) == 2:
            btns.append(row); row = []
    if row:
        btns.append(row)
    btns.append([InlineKeyboardButton("⚙ Defecto del proveedor", callback_data=f"setmodel:{sk}:{_key(ctx, f'{pid}|')}")])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀", callback_data=f"modprov:{sk}:{pid_k}:{page-1}"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("▶", callback_data=f"modprov:{sk}:{pid_k}:{page+1}"))
    if nav:
        btns.append(nav)
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])

    await q.edit_message_text(
        f"🧩 *{pid}*  _{page+1}/{total_pages}_",
        reply_markup=InlineKeyboardMarkup(btns),
        parse_mode="Markdown",
    )


async def cb_setmodel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Model chosen in /models → apply to the target session."""
    q = update.callback_query; await q.answer()
    parts     = q.data.split(":")
    sid       = _val(ctx, int(parts[1]))
    model_str = _val(ctx, int(parts[2]))
    pid, mid  = model_str.split("|", 1) if "|" in model_str else ("", "")

    # Get session info to find directory and show name
    try:
        sess_info = await oc.get_session(sid)
        directory = sess_info.get("directory", "")
        title     = sess_info.get("title") or sid[:12]
    except Exception:
        directory = ""
        title     = sid[:12]

    if pid and mid:
        pending = ctx.bot_data.setdefault("pending_model", {})
        pending[sid] = {"providerID": pid, "modelID": mid}

    cwd_name    = Path(directory).name if directory else "?"
    model_label = f"{pid}/{mid}" if pid and mid else "default del proveedor"
    await q.edit_message_text(
        f"✅ Modelo `{model_label}` aplicado al próximo prompt.\n"
        f"📂 `{cwd_name}` · `{title}`",
        parse_mode="Markdown",
    )


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
# File upload → save to active session cwd
# ---------------------------------------------------------------------------

TMP_DIR = Path("/tmp/opencode-bot-media")

@admin_only
async def handle_file_upload(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle documents, photos, videos — save directly to the active session cwd."""
    msg = update.message
    file_id: str | None = None
    file_name: str | None = None

    if msg.document:
        file_id   = msg.document.file_id
        file_name = msg.document.file_name or f"document_{int(time.time())}"
    elif msg.photo:
        file_id   = msg.photo[-1].file_id
        file_name = f"photo_{int(time.time())}.jpg"
    elif msg.video:
        file_id   = msg.video.file_id
        file_name = msg.video.file_name or f"video_{int(time.time())}.mp4"

    if not file_id:
        await msg.reply_text("❌ Tipo de archivo no soportado.")
        return

    active = db.get_active()
    if not active:
        await msg.reply_text("❌ No hay sesión activa. Usa /open para abrir un proyecto.")
        return

    cwd      = active["directory"]
    cwd_name = Path(cwd).name
    save_path = Path(cwd) / file_name

    try:
        tg_file = await ctx.bot.get_file(file_id)
        await tg_file.download_to_drive(save_path)
    except Exception as exc:
        await msg.reply_text(f"❌ Error al guardar el archivo: {exc}")
        return

    caption = msg.caption or ""
    caption_note = f"\n📝 _{caption}_" if caption else ""
    await msg.reply_text(
        f"✅ `{file_name}` guardado en `{cwd_name}`{caption_note}",
        parse_mode="Markdown",
    )


@admin_only
async def handle_audio_upload(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle audio/voice — transcribe with Grok STT, save file to cwd."""
    msg = update.message
    file_id: str | None = None
    file_name: str | None = None

    if msg.audio:
        file_id   = msg.audio.file_id
        file_name = msg.audio.file_name or f"audio_{int(time.time())}.mp3"
    elif msg.voice:
        file_id   = msg.voice.file_id
        file_name = f"voice_{int(time.time())}.ogg"

    if not file_id:
        await msg.reply_text("❌ Tipo de audio no soportado.")
        return

    active   = db.get_active()
    cwd      = active["directory"] if active else None
    cwd_name = Path(cwd).name if cwd else None

    # Download to temp directory
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path = TMP_DIR / file_name
    try:
        tg_file = await ctx.bot.get_file(file_id)
        await tg_file.download_to_drive(tmp_path)
    except Exception as exc:
        await msg.reply_text(f"❌ Error al descargar el audio: {exc}")
        return

    # Move to cwd if there's an active session
    if cwd:
        final_path = Path(cwd) / file_name
        shutil.move(str(tmp_path), str(final_path))
        saved_info = f"`{cwd_name}/{file_name}`"
    else:
        final_path = tmp_path
        saved_info = f"`/tmp/.../{file_name}` (sin sesión activa)"

    # Transcribe with Grok STT and send directly to OpenCode
    if grok_stt.is_configured():
        status_msg = await msg.reply_text("🎙️ Transcribiendo audio con Grok...")
        transcribed = await grok_stt.transcribe(str(final_path))
        # Delete audio file after transcription (voice notes, not kept)
        try:
            final_path.unlink(missing_ok=True)
        except Exception:
            pass
        if transcribed:
            if active and active.get("session_id"):
                sid      = active["session_id"]
                cwd_name_str = Path(cwd).name if cwd else "?"
                try:
                    await oc.send_message_async(sid, transcribed, directory=cwd)
                    await status_msg.edit_text(
                        f"🎙️ *Transcripción enviada a* `{cwd_name_str}`:\n\n{transcribed}",
                        parse_mode="Markdown",
                    )
                except Exception as exc:
                    await status_msg.edit_text(
                        f"🎙️ *Transcripción* (error al enviar: {exc}):\n\n{transcribed}",
                        parse_mode="Markdown",
                    )
            else:
                await status_msg.edit_text(
                    f"🎙️ *Transcripción* (sin sesión activa):\n\n{transcribed}\n\n"
                    f"💾 Archivo: {saved_info}",
                    parse_mode="Markdown",
                )
        else:
            await status_msg.edit_text(
                f"⚠️ No se pudo transcribir el audio.\n💾 Archivo guardado en {saved_info}",
                parse_mode="Markdown",
            )
    else:
        await msg.reply_text(
            f"💾 Audio guardado en {saved_info}\n\n"
            f"_Transcripción no disponible: configura XAI\\_API\\_KEY._",
            parse_mode="Markdown",
        )


# ---------------------------------------------------------------------------
# Plain text → prompt
# ---------------------------------------------------------------------------

@admin_only
async def handle_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text

    # Pending custom permission response?
    perm_input = ctx.bot_data.pop("perm_input", None)
    if perm_input:
        sid       = perm_input["session_id"]
        perm_id   = perm_input["perm_id"]
        directory = perm_input["directory"]
        try:
            await oc.respond_permission(sid, perm_id, text, remember=False, directory=directory or None)
            await update.message.reply_text(f"✅ Respuesta enviada: `{text}`", parse_mode="Markdown")
        except Exception as exc:
            await update.message.reply_text(f"❌ Error al responder permiso: {exc}")
        ctx.bot_data.get("pending_perms", {}).pop(perm_id, None)
        return

    # Pending custom question answer?
    q_custom = ctx.bot_data.pop("question_custom_input", None)
    if q_custom:
        req_id     = q_custom["req_id"]
        session_id = q_custom["session_id"]
        q_idx      = q_custom["q_idx"]
        pending    = ctx.bot_data.get("pending_questions", {})
        q_data     = pending.get(req_id)
        if q_data:
            q_data["answers"][q_idx] = [text]
            if all(a is not None for a in q_data["answers"]):
                await update.message.reply_text(f"✅ Respuesta enviada: `{text}`", parse_mode="Markdown")
                await _send_question_answer(ctx.application, req_id, session_id, q_data["answers"])
            else:
                n = len(q_data["questions"])
                await update.message.reply_text(
                    f"✅ Pregunta {q_idx+1}/{n} respondida: `{text}`\n\n_Responde las demás preguntas._",
                    parse_mode="Markdown",
                )
        else:
            await update.message.reply_text("⚠️ La pregunta ya fue respondida o expiró.")
        return

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

    if not await _server_ok(update.message.reply_text):
        return

    # Apply pending model change if any for this session
    pending_models = ctx.bot_data.get("pending_model", {})
    pending = pending_models.pop(sid, None)
    provider_id = pending["providerID"] if pending else None
    model_id    = pending["modelID"]    if pending else None

    try:
        await oc.send_message_async(sid, text, directory=directory,
                                    provider_id=provider_id, model_id=model_id)
    except Exception as exc:
        await update.message.reply_text(f"❌ Error al enviar: {exc}")
        return

    sent = await update.message.reply_text(
        f"📨 Enviado a `{cwd_name}` — esperando respuesta...",
        parse_mode="Markdown",
    )
    _track_msg(ctx.application, sent.message_id, sid, directory)
    # Store so it can be deleted when the status message appears
    ctx.bot_data.setdefault("pending_sent_msgs", {})[sid] = sent.message_id


# ---------------------------------------------------------------------------
# /projects — read-only overview of all projects with sessions
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
        d = s.get("_worktree") or s.get("directory", "")
        if d:
            by_dir[d].append(s)

    if not by_dir:
        await update.message.reply_text("No hay proyectos con sesiones. Usa /open primero.")
        return


    proj_by_wt = {p.get("worktree", ""): p for p in projects}
    active     = db.get_active()
    active_dir = (active or {}).get("directory", "")
    active_sid = (active or {}).get("session_id", "")

    lines = ["*Proyectos abiertos*\n"]
    for directory in sorted(by_dir.keys()):
        sessions   = by_dir[directory]
        p          = proj_by_wt.get(directory)
        name       = (p or {}).get("name") or Path(directory).name
        is_active  = directory == active_dir
        cur_sess   = next((s for s in sessions if s.get("id") == active_sid), sessions[0])
        sess_title = cur_sess.get("title") or cur_sess.get("id", "")[:12]
        marker     = " ◀ activo" if is_active else ""

        lines.append(
            f"📂 *{name}*{marker}\n"
            f"   📦 `{sess_title}`\n"
            f"   {len(sessions)} sesión{'es' if len(sessions) != 1 else ''}"
        )

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


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
            f"/projects — ver todos los proyectos con sesiones\n"
            f"/send — enviar prompt a un proyecto específico\n"
            f"/sessions — gestionar sesiones (todas o por proyecto)\n"
            f"/models — ver y cambiar modelos disponibles\n"
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
            f"/projects — ver todos los proyectos con sesiones\n"
            f"/send — enviar prompt a un proyecto específico\n"
            f"/sessions — gestionar sesiones (todas o por proyecto)\n"
            f"/models — ver y cambiar modelos disponibles\n"
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
    app.add_handler(CallbackQueryHandler(cb_closedel,  pattern=r"^closedel:"))
    app.add_handler(CallbackQueryHandler(cb_closebot,  pattern=r"^closebot:"))
    app.add_handler(CallbackQueryHandler(cb_closeall,  pattern=r"^closeall:"))
    app.add_handler(CallbackQueryHandler(cb_sda,       pattern=r"^sda:"))
    app.add_handler(CallbackQueryHandler(cb_abort,     pattern=r"^abort:"))
    app.add_handler(CallbackQueryHandler(cb_cancel,    pattern=r"^cancel:"))
    app.add_handler(CallbackQueryHandler(cb_perm,      pattern=r"^perm:"))
    app.add_handler(CallbackQueryHandler(cb_perminput, pattern=r"^perminput:"))
    app.add_handler(CallbackQueryHandler(cb_permabort, pattern=r"^permabort:"))
    app.add_handler(CallbackQueryHandler(cb_qans,      pattern=r"^qans:"))
    app.add_handler(CallbackQueryHandler(cb_qcustom,   pattern=r"^qcustom:"))
    app.add_handler(CallbackQueryHandler(cb_qreject,   pattern=r"^qreject:"))
    app.add_handler(CallbackQueryHandler(cb_sendpick,  pattern=r"^sendpick:"))
    app.add_handler(CallbackQueryHandler(cb_sendsess,  pattern=r"^sendsess:"))
    app.add_handler(CallbackQueryHandler(cb_sesspick,  pattern=r"^sesspick:"))
    app.add_handler(CallbackQueryHandler(cb_modpick,   pattern=r"^modpick:"))
    app.add_handler(CallbackQueryHandler(cb_modsess,   pattern=r"^modsess:"))
    app.add_handler(CallbackQueryHandler(cb_modprov,   pattern=r"^modprov:"))
    app.add_handler(CallbackQueryHandler(cb_setmodel,  pattern=r"^setmodel:"))

    app.add_handler(MessageHandler(filters.Document.ALL | filters.PHOTO | filters.VIDEO, handle_file_upload))
    app.add_handler(MessageHandler(filters.AUDIO | filters.VOICE, handle_audio_upload))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    async def _start_sse(c):
        task = asyncio.ensure_future(sse_listener(app))
        app.bot_data["_sse_task"] = task

    app.job_queue.run_once(_start_sse, when=1)

    async def post_init(application: Application):
        await application.bot.set_my_commands([
            BotCommand("start",    "Estado y menú"),
            BotCommand("open",     "Abrir proyecto / sesión"),
            BotCommand("projects", "Ver proyectos con sesiones abiertas"),
            BotCommand("send",     "Enviar prompt a un proyecto específico"),
            BotCommand("close",    "Cerrar proyecto"),
            BotCommand("sessions", "Gestionar sesiones de cualquier proyecto"),
            BotCommand("models",   "Cambiar modelo de cualquier sesión"),
            BotCommand("esc",      "Cancelar tarea actual"),
        ])

    app.post_init = post_init

    async def post_shutdown(application: Application):
        task = application.bot_data.get("_sse_task")
        if task and not task.done():
            task.cancel()
            try:
                await asyncio.wait_for(task, timeout=3)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
        await oc.close()

    app.post_shutdown = post_shutdown

    logger.info("Bot starting...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
