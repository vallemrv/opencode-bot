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
from telegram.error import BadRequest, RetryAfter

import shutil
import db
import transcription as grok_stt
import md2tgv2
from opencode_client import OpenCodeClient

MAX_KS = 2000

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
MODELS_FETCH_TIMEOUT = 15

async def _get_models(ctx: ContextTypes.DEFAULT_TYPE) -> list[dict]:
    cache = ctx.bot_data.get("models_cache")
    if cache and (time.time() - cache["ts"]) < MODELS_CACHE_TTL:
        return cache["data"]
    models = await asyncio.wait_for(oc.list_models(), timeout=MODELS_FETCH_TIMEOUT)
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
    seq = ctx.bot_data.get("ks_seq")
    if seq is None:
        seq = max(store.keys()) + 1 if store else 0
    k = seq
    ctx.bot_data["ks_seq"] = seq + 1
    store[k] = value
    while len(store) > MAX_KS:
        store.pop(next(iter(store)))
    return k

def _key_raw(bot_data: dict, value: str) -> int:
    """Same as _key but takes bot_data directly (for use outside handlers)."""
    store = bot_data.setdefault("ks", {})
    for k, v in store.items():
        if v == value:
            return k
    seq = bot_data.get("ks_seq")
    if seq is None:
        seq = max(store.keys()) + 1 if store else 0
    k = seq
    bot_data["ks_seq"] = seq + 1
    store[k] = value
    while len(store) > MAX_KS:
        store.pop(next(iter(store)))
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
#     "state": str,           # pending | busy | thinking | idle | error
#     "pending": bool,        # True if waiting for first SSE event
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
#     "model": str,
#     "session_title": str,
#   }
# }
# app.bot_data["queues"] = {
#   session_id: deque([{"text": str, "directory": str}])   # pending prompts
# }
# app.bot_data["msg_to_session"] = OrderedDict {
#   message_id (int): {"session_id": str, "directory": str}
# }  — max MSG_TRACK_LIMIT entries, oldest evicted first

STATUS_INTERVAL = 10
STATUS_THROTTLE = 3
MSG_TRACK_LIMIT = 200


def _track_msg(app: Application, message_id: int, session_id: str, directory: str):
    """Register a bot message so replies can be routed to the right session."""
    from collections import OrderedDict
    store: OrderedDict = app.bot_data.setdefault("msg_to_session", OrderedDict())
    store[message_id] = {"session_id": session_id, "directory": directory}
    while len(store) > MSG_TRACK_LIMIT:
        store.popitem(last=False)


async def _resolve_target(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> dict | None:
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
    active = await db.get_active()
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
    icons = {"pending": "⚪", "busy": "🔴", "thinking": "🤔", "idle": "🟢", "error": "❌"}
    icon  = icons.get(state, "⚪")
    state_labels = {"pending": "WAITING", "busy": "BUSY", "thinking": "THINKING", "idle": "IDLE", "error": "ERROR"}
    state_label = state_labels.get(state, state.upper())

    model_short = model.split("/")[-1] if "/" in model else model

    title_part = f" · `{sess_title[:20]}`" if sess_title else ""
    lines = [
        f"{icon} *{state_label}* | 📂 `{cwd_name}`{title_part}",
        f"🧩 `{model_short}` | ⏱ `{elapsed_str}`",
    ]

    if files:
        files_str = ", ".join(f"`{f}`" for f in list(files)[:4])
        if len(files) > 4:
            files_str += f" +{len(files)-4}"
        lines.append(f"📝 {files_str}")

    if tool:
        lines.append(f"🔧 `{tool}`")

    unique_tools = list(dict.fromkeys(tools_seen))
    if unique_tools and not tool:
        tools_str = " · ".join(f"`{t}`" for t in unique_tools[-5:])
        lines.append(f"⚡ {tools_str}")

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
    except RetryAfter as e:
        retry_after = e.retry_after
        logger.warning(f"Telegram flood control: retry after {retry_after}s")
        st["last_update_time"] = now + retry_after
        await asyncio.sleep(retry_after)
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
        except Exception:
            pass
    except Exception as exc:
        # Forbidden / TimedOut / NetworkError, etc. — never let a live-update
        # failure kill the heartbeat tick or the SSE listener.
        logger.warning(f"Status update failed for {session_id[:12]}: {exc}")


async def _heartbeat_loop(ctx: ContextTypes.DEFAULT_TYPE):
    statuses = ctx.bot_data.get("statuses", {})
    for sid in list(statuses.keys()):
        await _update_status_now(ctx.application, sid, force=False)


def _start_status(app: Application, session_id: str, directory: str, msg_id: int, model: str = "default", session_title: str = "", pending: bool = False):
    statuses = app.bot_data.setdefault("statuses", {})
    statuses[session_id] = {
        "msg_id": msg_id,
        "directory": directory,
        "model": model,
        "session_title": session_title,
        "state": "pending" if pending else "busy",
        "pending": pending,
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

    if not statuses:
        for job in app.job_queue.get_jobs_by_name("status_heartbeat"):
            job.schedule_removal()

    child_map = app.bot_data.get("child_to_parent", {})
    dead_children = [c for c, p in list(child_map.items()) if p == session_id]
    for c in dead_children:
        child_map.pop(c, None)

    if st and st.get("msg_id"):
        await _delete_msg(app.bot, ADMIN_ID, st["msg_id"])

    cached_text = st.get("final_text") if st else None
    directory   = st.get("directory") if st else None

    # Clean up orphaned pending_perms and pending_questions for this session
    pending_perms = app.bot_data.get("pending_perms", {})
    for perm_key in list(pending_perms.keys()):
        if pending_perms[perm_key].get("session_id") == session_id:
            pdata = pending_perms.pop(perm_key)
            try:
                await app.bot.edit_message_reply_markup(
                    chat_id=ADMIN_ID, message_id=pdata["msg_id"], reply_markup=None
                )
            except Exception:
                pass

    pending_questions = app.bot_data.get("pending_questions", {})
    for q_key in list(pending_questions.keys()):
        if pending_questions[q_key].get("session_id") == session_id:
            qdata = pending_questions.pop(q_key)
            for mid in qdata.get("msg_ids", []):
                try:
                    await app.bot.edit_message_reply_markup(
                        chat_id=ADMIN_ID, message_id=mid, reply_markup=None
                    )
                except Exception:
                    pass

    # Always fetch messages from API — it's the authoritative source for the full response.
    # final_text from SSE events may be incomplete (multi-step responses, partial deltas).
    # Fetch messages and session data concurrently (they are independent).
    fetched_messages = None
    session_data = None
    reply_text = None
    if directory:
        results = await asyncio.gather(
            oc.get_messages(session_id, directory=directory),
            oc.get_session(session_id, directory=directory),
            return_exceptions=True,
        )
        if isinstance(results[0], Exception):
            logger.error(f"Failed to get messages: {results[0]}")
        else:
            fetched_messages = results[0]
        if isinstance(results[1], Exception):
            logger.warning(f"Could not get session data concurrently: {results[1]}")
        else:
            session_data = results[1]

    if fetched_messages:
        for m in reversed(fetched_messages):
            info  = m.get("info", {})
            role  = info.get("role") or m.get("role")
            parts = m.get("parts", [])
            if role == "assistant":
                texts = [p.get("text", "") for p in parts if isinstance(p, dict) and p.get("type") == "text" and p.get("text")]
                if texts:
                    reply_text = "\n".join(texts)
                    break

    # Fallback to cached SSE text if API fetch failed or returned nothing
    if not reply_text and cached_text:
        reply_text = cached_text
        logger.warning(f"Using cached SSE text for {session_id[:12]} (API returned no text)")

    cwd_name = Path(directory or "").name or "?"

    # Build model + context % info (reuse fetched_messages for token count)
    model_info = ""
    try:
        if session_data is None:
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
            # MarkdownV2: backtick code + escaped plain text
            model_info = f"`{md2tgv2._escape(model_short)}`{md2tgv2._escape(ctx_str)}"
    except Exception as exc:
        logger.warning(f"Could not get session info for footer: {exc}")

    # Elapsed time
    elapsed = ""
    if st and st.get("start_time"):
        elapsed = md2tgv2._escape(f"⏱{_format_elapsed(time.time() - st['start_time'])}")

    # Files edited summary
    files_edited = st.get("files_edited", set()) if st else set()
    files_str = ""
    if files_edited:
        names = list(files_edited)[:3]
        files_str = " 📝 " + ", ".join(f"`{md2tgv2._escape(f)}`" for f in names)
        if len(files_edited) > 3:
            files_str += md2tgv2._escape(f" +{len(files_edited)-3}")

    sess_title = st.get("session_title", "") if st else ""

    header_parts = [f"✅ `{md2tgv2._escape(cwd_name)}`"]
    if app.bot_data.get("send_target"):
        header_parts.append("📤")
    if model_info:
        header_parts.append(model_info)
    if elapsed:
        header_parts.append(elapsed)
    header_line = " \\| ".join(header_parts)
    if files_str:
        header_line += files_str
    if sess_title:
        header_line += f"\n📌 `{md2tgv2._escape(sess_title[:40])}`"

    # Plain-text header for fallback (no MarkdownV2 escaping, no backticks)
    plain_header = f"✅ {cwd_name}"
    if st and st.get("start_time"):
        plain_header += f" ⏱{_format_elapsed(time.time() - st['start_time'])}"
    if sess_title:
        plain_header += f"\n{sess_title[:40]}"

    if not reply_text:
        try:
            sent = await app.bot.send_message(ADMIN_ID, f"{header_line}\n_Listo\\._", parse_mode="MarkdownV2")
        except Exception as exc:
            logger.warning(f"Final 'Listo' send failed ({exc}), retrying as plain text")
            try:
                sent = await app.bot.send_message(ADMIN_ID, f"{plain_header}\nListo.")
            except Exception as exc2:
                logger.error(f"Failed to send final 'Listo' reply: {exc2}")
                return
        _track_msg(app, sent.message_id, session_id, directory or "")
        return

    # Long responses → send as respuesta.md file to avoid flooding
    MD_FILE_THRESHOLD = 6000
    if len(reply_text) > MD_FILE_THRESHOLD:
        import io
        md_bytes = reply_text.encode("utf-8")
        md_file  = io.BytesIO(md_bytes)
        md_file.name = "respuesta.md"
        try:
            last_sent = await app.bot.send_document(
                ADMIN_ID,
                document=md_file,
                caption=header_line,
                parse_mode="MarkdownV2",
            )
            _track_msg(app, last_sent.message_id, session_id, directory or "")
        except Exception as exc:
            logger.error(f"Failed to send respuesta.md: {exc}")
        await _drain_queue(app, session_id)
        return

    chunk_size = 3900
    chunks = [reply_text[i:i+chunk_size] for i in range(0, len(reply_text), chunk_size)]
    last_sent = None
    for i, chunk in enumerate(chunks):
        header = f"{header_line}\n" if i == 0 else ""
        plain_hdr = f"{plain_header}\n" if i == 0 else ""
        kbd = None
        # Convert LLM markdown to Telegram MarkdownV2
        tg_chunk = md2tgv2.convert(chunk)
        try:
            last_sent = await app.bot.send_message(
                ADMIN_ID,
                f"{header}{tg_chunk}",
                parse_mode="MarkdownV2",
                reply_markup=kbd,
            )
            if last_sent:
                _track_msg(app, last_sent.message_id, session_id, directory or "")
        except BadRequest as e:
            logger.warning(f"MarkdownV2 parse failed ({e}), sending as plain text")
            # Fallback: send raw text without any parse mode, using plain header
            try:
                last_sent = await app.bot.send_message(
                    ADMIN_ID,
                    f"{plain_hdr}{chunk}",
                    reply_markup=kbd,
                )
                if last_sent:
                    _track_msg(app, last_sent.message_id, session_id, directory or "")
            except Exception as exc2:
                logger.error(f"Failed to send final reply chunk: {exc2}")

    # Clean up child sessions in OpenCode (they are independent copies, no context loss)
    if directory:
        try:
            children = await oc.get_session_children(session_id, directory=directory)
            for child in children:
                child_id = child.get("id")
                if child_id:
                    try:
                        await oc.delete_session(child_id, directory=directory)
                        logger.info(f"Cleaned up child session {child_id[:12]}")
                    except Exception as exc:
                        logger.warning(f"Failed to delete child {child_id[:12]}: {exc}")
        except Exception as exc:
            logger.warning(f"Failed to list children for {session_id[:12]}: {exc}")

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
        sess_info = await oc.get_session(session_id, directory=directory)
        sess_title = sess_info.get("title") or session_id[:12]
        model_obj = sess_info.get("model", {})
        model_short = ""
        if model_obj:
            model_full = f"{model_obj.get('providerID','')}/{model_obj.get('id','')}"
            model_short = model_full.split("/")[-1] if "/" in model_full else model_full
    except Exception:
        sess_title = session_id[:12]
        model_short = ""

    try:
        sent = await app.bot.send_message(
            ADMIN_ID,
            f"⚪ *WAITING* | 📂 `{cwd_name}`\n"
            f"📦 `{sess_title[:16]}`\n"
            f"🧩 `{model_short or '...'}` | ⏱ `00:00`\n\n"
            f"_Pulsa_ /esc _para cancelar_",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Cancelar", callback_data="abort:")
            ]]),
        )
    except Exception as exc:
        logger.error(f"Failed to send WAITING msg for queued prompt ({exc}); dropping item")
        try:
            await app.bot.send_message(ADMIN_ID, f"❌ No pude encolar el siguiente mensaje de `{cwd_name}`: {exc}", parse_mode="Markdown")
        except Exception:
            pass
        return
    _start_status(app, session_id, directory, sent.message_id, model=model_short, session_title=sess_title, pending=True)
    _track_msg(app, sent.message_id, session_id, directory)

    try:
        pending_models = app.bot_data.get("pending_model", {})
        pending    = pending_models.pop(session_id, None)
        provider_id = pending["providerID"] if pending else None
        model_id    = pending["modelID"]    if pending else None
        await oc.send_message_async(session_id, text, directory=directory,
                                    provider_id=provider_id, model_id=model_id)
        remaining = len(q) if q else 0
        if remaining:
            await app.bot.send_message(
                ADMIN_ID,
                f"📨 `{cwd_name}` — {remaining} mensaje{'s' if remaining != 1 else ''} en cola",
                parse_mode="Markdown",
            )
    except Exception as exc:
        statuses = app.bot_data.get("statuses", {})
        statuses.pop(session_id, None)
        await _delete_msg(app.bot, ADMIN_ID, sent.message_id)
        await app.bot.send_message(ADMIN_ID, f"❌ Error al enviar mensaje encolado: {exc}")


# ---------------------------------------------------------------------------
# Question tool handler
# ---------------------------------------------------------------------------

async def _handle_question_asked(app: Application, props: dict) -> None:
    """
    When the LLM calls the `question` tool, build inline keyboards for each
    question and send them to Telegram. The session stays paused until the
    user answers.

    Supports multiple questions in a single event — answers are accumulated
    and sent only when all questions are answered, or the user presses
    "Enviar ahora" for partial answers.
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
        active = await db.get_active()
        if active and active.get("session_id") == session_id:
            directory = active.get("directory", "")
    if not directory:
        active = await db.get_active()
        if active:
            directory = active.get("directory", "")

    n = len(questions)

    # Store pending question state
    pending = app.bot_data.setdefault("pending_questions", {})
    pending[req_id] = {
        "session_id": session_id,
        "directory":  directory,
        "questions":  questions,
        "answers":    [None] * n,
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

        # Custom answer row
        if custom:
            ck = _key_raw(app.bot_data, f"{req_id}|{q_idx}|custom")
            btns.append([InlineKeyboardButton(
                "✏️ Escribe tu propia respuesta",
                callback_data=f"qcustom:{rk}:{sk}:{q_idx}",
            )])

        # If multiple questions, add "Enviar ahora" for partial answers
        # and cancel row
        if n > 1:
            btns.append([InlineKeyboardButton(
                "📨 Enviar ahora",
                callback_data=f"qsendnow:{rk}:{sk}",
            )])
        btns.append([InlineKeyboardButton(
            "❌ Cancelar pregunta",
            callback_data=f"qreject:{rk}:{sk}",
        )])

        suffix = f" ({q_idx+1}/{n})" if n > 1 else ""
        try:
            sent = await app.bot.send_message(
                ADMIN_ID,
                f"❓ *{md2tgv2._escape(header)}{md2tgv2._escape(suffix)}*\n\n{md2tgv2._escape(question)}",
                parse_mode="MarkdownV2",
                reply_markup=InlineKeyboardMarkup(btns),
            )
        except Exception:
            sent = await app.bot.send_message(
                ADMIN_ID,
                f"❓ {header}{suffix}\n\n{question}",
                reply_markup=InlineKeyboardMarkup(btns),
            )
        msg_ids.append(sent.message_id)
        _track_msg(app, sent.message_id, session_id, directory)

    pending[req_id]["msg_ids"] = msg_ids


def _all_questions_answered(q_data: dict) -> bool:
    """Check if all questions have been answered."""
    return all(a is not None for a in q_data.get("answers", []))


async def _refresh_question_buttons(app: Application, req_id: str) -> None:
    """Update inline keyboards to show answered state and remaining count."""
    q_data = app.bot_data.get("pending_questions", {}).get(req_id)
    if not q_data:
        return

    rk = _key_raw(app.bot_data, req_id)
    sk = _key_raw(app.bot_data, q_data["session_id"])
    answers = q_data["answers"]
    questions = q_data["questions"]
    n = len(questions)
    answered_count = sum(1 for a in answers if a is not None)

    for q_idx, (q, msg_id) in enumerate(zip(questions, q_data["msg_ids"])):
        if answers[q_idx] is not None:
            # Already answered — show result and disable buttons
            ans_text = ", ".join(answers[q_idx]) if isinstance(answers[q_idx], list) else str(answers[q_idx])
            try:
                await app.bot.edit_message_text(
                    chat_id=ADMIN_ID, message_id=msg_id,
                    text=f"✅ *{md2tgv2._escape(q.get('header', f'Pregunta {q_idx+1}'))}*\n\n{md2tgv2._escape(ans_text)}",
                    parse_mode="MarkdownV2",
                    reply_markup=None,
                )
            except Exception:
                try:
                    await app.bot.edit_message_text(
                        chat_id=ADMIN_ID, message_id=msg_id,
                        text=f"✅ {q.get('header', f'Pregunta {q_idx+1}')}\n\n{ans_text}",
                        reply_markup=None,
                    )
                except Exception:
                    pass
            continue

        # Still pending — rebuild buttons
        header   = q.get("header", f"Pregunta {q_idx+1}")
        question = q.get("question", "")
        options  = q.get("options", [])
        custom   = q.get("custom", True)

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

        if custom:
            ck = _key_raw(app.bot_data, f"{req_id}|{q_idx}|custom")
            btns.append([InlineKeyboardButton(
                "✏️ Escribe tu propia respuesta",
                callback_data=f"qcustom:{rk}:{sk}:{q_idx}",
            )])

        remaining = n - answered_count
        if n > 1:
            btns.append([InlineKeyboardButton(
                f"📨 Enviar ahora ({answered_count}/{n})",
                callback_data=f"qsendnow:{rk}:{sk}",
            )])
        btns.append([InlineKeyboardButton(
            "❌ Cancelar pregunta",
            callback_data=f"qreject:{rk}:{sk}",
        )])

        suffix = f" ({q_idx+1}/{n})" if n > 1 else ""
        try:
            await app.bot.edit_message_text(
                chat_id=ADMIN_ID, message_id=msg_id,
                text=f"❓ *{md2tgv2._escape(header)}{md2tgv2._escape(suffix)}*\n\n{md2tgv2._escape(question)}",
                parse_mode="MarkdownV2",
                reply_markup=InlineKeyboardMarkup(btns),
            )
        except Exception:
            try:
                await app.bot.edit_message_text(
                    chat_id=ADMIN_ID, message_id=msg_id,
                    text=f"❓ {header}{suffix}\n\n{question}",
                    reply_markup=InlineKeyboardMarkup(btns),
                )
            except Exception:
                pass


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

        NOISY_EVENTS = {"message.part.delta", "sync", "server.heartbeat", "message.part.updated"}
        if etype in NOISY_EVENTS:
            logger.debug(f"SSE {etype} sid={sid[:12] if sid else '-'}")
        else:
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

            # ---- resolve child sessions to their parent ----
            # If this sid is a known child, route its events to the parent status.
            # If it's unknown, check with OpenCode whether it has a parentID.
            # IMPORTANT: Only mark as child if the parent is actually being tracked.
            child_map = app.bot_data.setdefault("child_to_parent", {})
            effective_sid = sid  # the sid whose status entry we'll update

            if sid not in statuses:
                # Could be a child session we haven't seen yet
                if sid in child_map:
                    parent_id = child_map[sid]
                    # Only route to parent if parent is actually tracked
                    if parent_id in statuses:
                        effective_sid = parent_id
                    else:
                        # Parent not tracked — treat this session as independent
                        child_map.pop(sid, None)
                else:
                    # Ask OpenCode if this session has a parent
                    try:
                        sess_info = await oc.get_session(sid)
                        parent_id = sess_info.get("parentID") or ""
                        if parent_id and parent_id in statuses:
                            child_map[sid] = parent_id
                            effective_sid  = parent_id
                            logger.info(f"Child session {sid[:12]} → parent {parent_id[:12]}")
                    except Exception:
                        pass

            st = statuses.get(effective_sid)
            logger.debug(f"SSE {etype} sid={sid[:12]} effective={effective_sid[:12]} st={'yes' if st else 'no'}")

            # ---- session status ----
            if etype == "session.status":
                status     = props.get("status", {})
                state_type = status.get("type", "idle")
                is_child   = (sid != effective_sid)  # True if this event is from a child session

                if state_type in ("busy", "retry"):
                    if not st:
                        active_check = await db.get_active()
                        is_active = active_check and active_check.get("session_id") == sid

                        if not is_active and not is_child:
                            continue

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
                            f"⏱ `00:00`\n\n"
                            f"_Pulsa_ /esc _para cancelar_",
                            parse_mode="Markdown",
                            reply_markup=InlineKeyboardMarkup([[
                                InlineKeyboardButton("❌ Cancelar", callback_data="abort:")
                            ]]),
                        )
                        _start_status(app, sid, directory, status_msg.message_id, model=model_label, session_title=sess_title)
                        _track_msg(app, status_msg.message_id, sid, directory)
                        st = app.bot_data["statuses"].get(sid)

                    if st:
                        st["pending"] = False
                        st["state"] = "busy"
                        if not st.get("model") or st.get("model") == "default":
                            try:
                                sess_info = await oc.get_session(sid)
                                model_obj = sess_info.get("model", {})
                                if model_obj:
                                    st["model"] = f"{model_obj.get('providerID','')}/{model_obj.get('id','')}"
                                if not st.get("session_title"):
                                    st["session_title"] = sess_info.get("title") or ""
                            except Exception:
                                pass
                        if state_type == "retry":
                            st["last_text"] = status.get("message", "Retrying...")
                        await _update_status_now(app, effective_sid, force=True)

                elif state_type == "idle":
                    if is_child:
                        continue
                    if st:
                        # Verify with API that session is actually idle before finishing.
                        # When a new session is created in the same project, OpenCode may
                        # send stale idle events for other sessions. Double-check to avoid
                        # prematurely finishing an active session.
                        try:
                            sess_info = await oc.get_session(effective_sid, directory=st.get("directory", ""))
                            srv_status = sess_info.get("status") or {}
                            srv_type = srv_status.get("type") if isinstance(srv_status, dict) else str(srv_status)
                            if srv_type in ("busy", "retry"):
                                logger.info(f"Ignoring stale idle event for {effective_sid[:12]} (server says {srv_type})")
                                continue
                        except Exception:
                            pass  # If API unreachable, proceed with idle (safe default)
                        await _finish_status(app, effective_sid)
                continue

            if etype == "session.idle":
                if sid != effective_sid:
                    continue
                if st:
                    # Same stale-event protection as session.status idle
                    try:
                        sess_info = await oc.get_session(effective_sid, directory=st.get("directory", ""))
                        srv_status = sess_info.get("status") or {}
                        srv_type = srv_status.get("type") if isinstance(srv_status, dict) else str(srv_status)
                        if srv_type in ("busy", "retry"):
                            logger.info(f"Ignoring stale session.idle for {effective_sid[:12]} (server says {srv_type})")
                            continue
                    except Exception:
                        pass
                    await _finish_status(app, effective_sid)
                continue

            if etype == "session.error":
                if st:
                    error     = props.get("error", {})
                    error_msg = error.get("data", {}).get("message", str(error))
                    st["state"] = "error"
                    await _update_status_now(app, effective_sid, force=True)
                    err_sent = await app.bot.send_message(
                        ADMIN_ID,
                        f"❌ *Error* `{sid[:12]}`:\n{error_msg}",
                        parse_mode="Markdown",
                    )
                    _track_msg(app, err_sent.message_id, sid, st.get("directory", ""))
                    await _finish_status(app, effective_sid)
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
                    active = await db.get_active()
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
                _track_msg(app, perm_msg.message_id, p_sid, p_dir)
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
                    # but preserve final_text accumulated so far across steps.
                    st["last_text"] = None
                    st["tool"]      = None

                elif part_type == "text":
                    st["state"] = "busy"
                    text = part.get("text", "")
                    if text:
                        # updated events carry the full current text of this part.
                        # Keep last_text for live status display.
                        # final_text tracks the last known text (API is authoritative at finish).
                        st["last_text"]  = text
                        st["final_text"] = text
                    await _update_status_now(app, effective_sid)

                elif part_type == "reasoning":
                    st["state"] = "thinking"
                    text = part.get("text", "")
                    if text:
                        st["reasoning_text"] = text
                    await _update_status_now(app, effective_sid)

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
                    await _update_status_now(app, effective_sid, force=True)

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
                    st["state"]     = "busy"
                    st["last_text"] = (st.get("last_text") or "") + delta
                continue

            if etype == "message.updated":
                info = props.get("info", {})
                if info.get("role") == "assistant" and st:
                    tokens = info.get("tokens", {})
                    if tokens:
                        st["tokens_input"]  = tokens.get("input", 0)
                        st["tokens_output"] = tokens.get("output", 0)
                    st["message_count"] = st.get("message_count", 0) + 1
                    await _update_status_now(app, effective_sid)
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
    btns.append([InlineKeyboardButton("📁 Nueva carpeta", callback_data=f"mkdir:{pk}")])
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
    if _clear_send_mode(ctx.bot_data):
        await update.message.reply_text("📤 Modo send desactivado.", parse_mode="Markdown")
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


async def cb_mkdir(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """User pressed 'Nueva carpeta' — ask for the folder name."""
    q = update.callback_query; await q.answer()
    pk   = int(q.data.split(":")[1])
    path = _val(ctx, pk)
    ctx.bot_data["mkdir_pending"] = {"path": path, "msg_id": q.message.message_id}
    await q.edit_message_text(
        f"📁 Nueva carpeta en `{Path(path).name}`\n\nEscribe el nombre:",
        parse_mode="Markdown",
    )


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
    """Show existing sessions for this project + option to create new one.

    Sessions are displayed in parent→child hierarchy. Child sessions
    (those with parentID set) are shown indented under their parent.
    Deleting a parent also deletes all its children (OpenCode behaviour),
    so the UI makes this visible.
    """
    cwd_path = Path(cwd)
    pk = _key(ctx, cwd)
    active = await db.get_active()
    active_sid = (active or {}).get("session_id")

    # Build parent→children map
    by_id    = {s["id"]: s for s in sessions}
    children_map: dict[str, list] = defaultdict(list)
    roots    = []
    for s in sessions:
        pid = s.get("parentID")
        if pid and pid in by_id:
            children_map[pid].append(s)
        else:
            roots.append(s)

    # Show only root sessions; child sessions are internal to OpenCode
    btns = [[InlineKeyboardButton("➕ Nueva sesión", callback_data=f"newsess:{pk}")]]
    for s in roots[:10]:
        sid   = s.get("id", "")
        title = s.get("title") or sid[:12]
        mark  = " ✅" if sid == active_sid else ""
        sk = _key(ctx, sid)
        btns.append([
            InlineKeyboardButton(
                f"{title[:24]}{mark}",
                callback_data=f"actsess:{sk}:{pk}",
            ),
            InlineKeyboardButton("🗑", callback_data=f"delsess:{sk}:{pk}"),
        ])
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])

    await q.edit_message_text(
        f"📂 `{cwd_path.name}` — {len(roots)} sesión{'es' if len(roots) != 1 else ''}",
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
    pk       = _key(ctx, cwd) if cwd else -1
    cwd_name = Path(cwd).name if cwd else None
    header   = f"📂 `{cwd_name}`\n" if cwd_name else ""

    if not skip_loading:
        await q.edit_message_text(f"{header}⏳ Cargando modelos...", parse_mode="Markdown")

    try:
        models = await _get_models(ctx)
    except asyncio.TimeoutError:
        await q.edit_message_text(
            f"{header}❌ Timeout al cargar modelos. Verifica que OpenCode está corriendo.",
            parse_mode="Markdown",
        )
        return
    except Exception as exc:
        await q.edit_message_text(f"❌ Error al cargar modelos: {exc}", parse_mode="Markdown")
        return

    if not models:
        await q.edit_message_text(
            f"{header}⚠️ No hay modelos disponibles. Configura un proveedor en OpenCode.",
            parse_mode="Markdown",
        )
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
    cwd   = _val(ctx, pk) if pk != -1 else None

    try:
        models = await _get_models(ctx)
    except asyncio.TimeoutError:
        await q.edit_message_text("❌ Timeout al cargar modelos.", parse_mode="Markdown")
        return
    except Exception as exc:
        await q.edit_message_text(f"❌ Error: {exc}", parse_mode="Markdown")
        return

    mids = sorted([m.get("id") or m.get("modelID", "?") for m in models if m.get("providerID") == pid])
    if not mids:
        await q.edit_message_text(f"⚠️ No hay modelos para el proveedor `{pid}`.", parse_mode="Markdown")
        return
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
        btns.append([InlineKeyboardButton("⬅ Proveedores", callback_data=f"prov:-1:0:0")])
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])

    await q.edit_message_text(
        f"🧩 *{pid}*  _{page+1}/{total_pages}_",
        reply_markup=InlineKeyboardMarkup(btns),
        parse_mode="Markdown",
    )


async def cb_provmodel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    Shared model-selected handler.
    pk == -1 → /models mode: update model of active session.
    pk != -1 → wizard mode: create new session in cwd.
    """
    q = update.callback_query; await q.answer()
    parts     = q.data.split(":")
    pk        = int(parts[1])
    model_str = _val(ctx, int(parts[2]))
    pid, mid  = model_str.split("|", 1) if "|" in model_str else ("", "")

    if pk == -1:
        # /models mode — store pending model, applied on next prompt
        active = await db.get_active()
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
        await db.set_active(sid, cwd)

        # If this session was created from the /send flow
        send_new_dir = ctx.bot_data.pop("send_new_sess_dir", None)
        pending_text = ctx.bot_data.pop("send_mode_text", None)
        
        if send_new_dir and Path(send_new_dir).resolve() == Path(cwd).resolve():
            if pending_text:
                await q.edit_message_text(
                    f"✅ Sesión creada\n📦 `{title}`\n📂 `{Path(cwd).name}` | 🧩 `{model_label}`\n\n📤 Enviando...",
                    parse_mode="Markdown",
                )
                await _do_send_text(ctx.application, pending_text, sid, cwd, q.message.chat_id)
            else:
                ctx.bot_data["send_target"] = {"session_id": sid, "directory": cwd}
                await q.edit_message_text(
                    f"✅ Sesión creada\n"
                    f"📦 `{title}`\n"
                    f"📂 `{Path(cwd).name}` | 🧩 `{model_label}`\n\n"
                    f"📤 *Sesión seleccionada*\n\n"
                    f"Escribe el mensaje:",
                    parse_mode="Markdown",
                )
        else:
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

    await db.set_active(sid, cwd)
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
    """Delete a session — if it has children, ask for confirmation first."""
    q = update.callback_query; await q.answer()
    parts = q.data.split(":")
    sid   = _val(ctx, int(parts[1]))
    pk    = int(parts[2]) if len(parts) > 2 else None
    cwd   = _val(ctx, pk) if pk is not None else ""

    # Check if this session has children before deleting
    children = await oc.get_session_children(sid, directory=cwd or None)
    if children:
        sk = _key(ctx, sid)
        n  = len(children)
        await q.edit_message_text(
            f"⚠️ ¿Borrar esta sesión?\n\n"
            f"OpenCode creó {n} sub-sesión{'es' if n != 1 else ''} interna{'s' if n != 1 else ''} "
            f"que también se eliminarán.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🗑 Borrar", callback_data=f"delconfirm:{sk}:{pk or 0}")],
                [InlineKeyboardButton("❌ Cancelar", callback_data=f"os:{pk or 0}")],
            ]),
            parse_mode="Markdown",
        )
        return

    await _do_delete_session(q, ctx, sid, cwd, pk)


async def cb_delconfirm(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Confirmed deletion of a session with children."""
    q = update.callback_query; await q.answer()
    parts = q.data.split(":")
    sid   = _val(ctx, int(parts[1]))
    pk    = int(parts[2]) if len(parts) > 2 else None
    cwd   = _val(ctx, pk) if pk is not None else ""
    await _do_delete_session(q, ctx, sid, cwd, pk)


async def _do_delete_session(q, ctx, sid: str, cwd: str, pk):
    """Actually delete a session and refresh the picker."""
    try:
        await oc.delete_session(sid, directory=cwd or None)
    except Exception as exc:
        await q.edit_message_text(f"❌ Error: {exc}", parse_mode="Markdown")
        return

    ctx.application.bot_data.get("statuses", {}).pop(sid, None)

    active = await db.get_active()
    if active and active.get("session_id") == sid:
        await db.clear_active()

    # Refresh picker if we know the cwd
    if cwd:
        try:
            sessions = await oc.list_sessions(directory=cwd)
        except Exception:
            sessions = []
        if sessions:
            await _show_session_picker(q, ctx, cwd, sessions)
        else:
            pk_key = _key(ctx, cwd)
            await q.edit_message_text(
                f"✅ Sesión borrada. No quedan sesiones en `{Path(cwd).name}`.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("➕ Nueva sesión", callback_data=f"newsess:{pk_key}")],
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
    opt_str    = _val(ctx, ok)
    label      = opt_str.split("|", 3)[-1] if "|" in opt_str else opt_str

    pending = ctx.bot_data.get("pending_questions", {})
    q_data  = pending.get(req_id)
    if not q_data:
        await q.edit_message_text("⚠️ Pregunta ya respondida o expirada.")
        return

    questions = q_data["questions"]
    n_questions = len(questions)

    # Record answer
    q_data["answers"][q_idx] = [label]

    if n_questions == 1 or _all_questions_answered(q_data):
        # Single question or all answered — send immediately
        filled = [a if a is not None else [] for a in q_data["answers"]]
        await q.edit_message_text(f"✅ *{label}*", parse_mode="Markdown")
        await _send_question_answer(ctx.application, req_id, session_id, filled)
    else:
        # Multiple questions — just mark this one and refresh remaining buttons
        await q.edit_message_text(f"✅ *{label}*", parse_mode="Markdown")
        await _refresh_question_buttons(ctx.application, req_id)


async def cb_qsendnow(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """User wants to send with only the answered questions, ignoring the rest."""
    q = update.callback_query; await q.answer()
    parts      = q.data.split(":")
    req_id     = _val(ctx, int(parts[1]))
    session_id = _val(ctx, int(parts[2]))

    pending = ctx.bot_data.get("pending_questions", {})
    q_data  = pending.get(req_id)
    if not q_data:
        await q.edit_message_text("⚠️ Pregunta ya respondida o expirada.")
        return

    answered = sum(1 for a in q_data["answers"] if a is not None)
    total = len(q_data["answers"])
    filled_answers = [a if a is not None else [] for a in q_data["answers"]]
    await q.edit_message_text(f"📨 Enviando {answered}/{total} respuestas...")
    await _send_question_answer(ctx.application, req_id, session_id, filled_answers)


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
    loading_msg = await update.message.reply_text("⏳ Cargando proyectos...")

    try:
        projects = await oc.list_projects()
        all_sessions = await oc.list_sessions()
    except Exception as exc:
        await _delete_msg(ctx.bot, ADMIN_ID, loading_msg.message_id)
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
        await _delete_msg(ctx.bot, ADMIN_ID, loading_msg.message_id)
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
    await _delete_msg(ctx.bot, ADMIN_ID, loading_msg.message_id)
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

    active = await db.get_active()
    if active and active.get("directory") == directory:
        await db.clear_active()

    await q.edit_message_text(
        f"✅ `{Path(directory).name}` cerrado — {deleted} sesiones borradas de OpenCode.",
        parse_mode="Markdown",
    )


async def cb_closebot(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Only clear active session in bot, keep OpenCode sessions intact."""
    q = update.callback_query; await q.answer()
    ck        = int(q.data.split(":")[1])
    directory = _val(ctx, ck)

    active = await db.get_active()
    if active and active.get("directory") == directory:
        await db.clear_active()

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
        await q.edit_message_text(f"❌ Error al listar sesiones: {exc}")
        return

    deleted = 0
    failed  = []
    for s in all_sessions:
        sid = s.get("id", "")
        directory = s.get("directory") or s.get("_worktree") or None
        try:
            await oc.delete_session(sid, directory=directory)
            ctx.application.bot_data.get("statuses", {}).pop(sid, None)
            deleted += 1
        except Exception as exc:
            proj = Path(directory).name if directory else "?"
            failed.append(f"`{proj}` / `{sid[:12]}` — {exc}")

    await db.clear_active()

    lines = [f"✅ {deleted} sesiones borradas del server."]
    if failed:
        lines.append("")
        lines.append(f"⚠️ {len(failed)} no se pudieron borrar:")
        for f in failed[:5]:
            lines.append(f"• {f}")
        if len(failed) > 5:
            lines.append(f"...y {len(failed)-5} más")

    await q.edit_message_text("\n".join(lines), parse_mode="Markdown")


# ---------------------------------------------------------------------------
# /sessions — manage sessions of any project
# ---------------------------------------------------------------------------

@admin_only
async def cmd_sessions(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Show all projects so the user can pick one to manage its sessions."""
    if _clear_send_mode(ctx.bot_data):
        await update.message.reply_text("📤 Modo send desactivado.", parse_mode="Markdown")
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

    active     = await db.get_active()
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

    active     = await db.get_active()
    active_sid = (active or {}).get("session_id", "")
    pk         = _key(ctx, directory)

    # Filter to root sessions only; child sessions are internal to OpenCode
    by_id = {s["id"]: s for s in sessions}
    roots = [s for s in sessions if not (s.get("parentID") and s["parentID"] in by_id)]

    btns = [[InlineKeyboardButton("➕ Nueva sesión", callback_data=f"newsess:{pk}")]]
    if roots:
        btns.append([InlineKeyboardButton("🗑 Borrar todas", callback_data=f"sda:{pk}")])
    for s in roots[:8]:
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

    active = await db.get_active()
    if active and active.get("directory") == directory:
        await db.clear_active()

    await q.edit_message_text(
        f"✅ Todas las sesiones de `{Path(directory).name}` borradas.",
        parse_mode="Markdown",
    )


# ---------------------------------------------------------------------------
# /models — change model for any session of any project
# ---------------------------------------------------------------------------

@admin_only
async def cmd_models(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Change model for the active session."""
    active = await db.get_active()
    if not active:
        await update.message.reply_text("⚠️ No hay sesión activa. Usa /open primero.")
        return

    sid       = active["session_id"]
    directory = active["directory"]
    cwd_name  = Path(directory).name

    sess_title = sid[:12]
    try:
        sess_info = await oc.get_session(sid, directory=directory)
        sess_title = sess_info.get("title") or sid[:12]
    except Exception:
        pass

    # Sanitize sess_title to avoid Markdown parse errors
    sess_title_md = sess_title.replace("`", "'").replace("*", "").replace("_", " ")

    loading_msg = await update.message.reply_text(f"📂 `{cwd_name}` · `{sess_title_md}`\n⏳ Cargando modelos...", parse_mode="Markdown")

    try:
        models = await _get_models(ctx)
        logger.info(f"Loaded {len(models)} models for /models command")
    except asyncio.TimeoutError:
        await _delete_msg(ctx.bot, ADMIN_ID, loading_msg.message_id)
        await update.message.reply_text(
            "❌ Timeout al cargar modelos. El servidor OpenCode no responde.\n"
            f"Verifica que `opencode serve` está corriendo en `{OC_HOST}:{OC_PORT}`.",
            parse_mode="Markdown",
        )
        return
    except BaseException as exc:
        logger.error(f"cmd_models: error loading models ({type(exc).__name__}): {exc}", exc_info=True)
        await _delete_msg(ctx.bot, ADMIN_ID, loading_msg.message_id)
        await update.message.reply_text(f"❌ Error al cargar modelos: {type(exc).__name__}: {exc}")
        return

    if not models:
        await _delete_msg(ctx.bot, ADMIN_ID, loading_msg.message_id)
        await update.message.reply_text(
            "⚠️ No hay modelos disponibles. Configura al menos un proveedor en OpenCode.",
            parse_mode="Markdown",
        )
        return

    groups: dict[str, list] = defaultdict(list)
    for m in models:
        groups[m.get("providerID", "?")].append(m.get("id") or m.get("modelID", "?"))

    sk = _key(ctx, sid)
    btns = []
    for pid in sorted(groups):
        btns.append([InlineKeyboardButton(
            f"🔹 {pid}",
            callback_data=f"modprov:{sk}:{_key(ctx, pid)}:0",
        )])
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])

    await _delete_msg(ctx.bot, ADMIN_ID, loading_msg.message_id)
    await update.message.reply_text(
        f"📂 `{cwd_name}` · `{sess_title_md}`\n📦 Elige proveedor:",
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
    except asyncio.TimeoutError:
        await q.edit_message_text("❌ Timeout al cargar modelos.", parse_mode="Markdown")
        return
    except Exception as exc:
        await q.edit_message_text(f"❌ Error: {exc}", parse_mode="Markdown")
        return

    mids = sorted([m.get("id") or m.get("modelID", "?") for m in models if m.get("providerID") == pid])
    if not mids:
        await q.edit_message_text(f"⚠️ No hay modelos para el proveedor `{pid}`.", parse_mode="Markdown")
        return
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
    active = await db.get_active()
    if not active:
        await update.message.reply_text("⚠️ No hay sesión activa.")
        return
    msg = await _do_abort(ctx.application, active["session_id"], active["directory"])
    await update.message.reply_text(msg)


async def cb_abort(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    active = await db.get_active()
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

    active = await db.get_active()
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

    active   = await db.get_active()
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
        try:
            shutil.move(str(tmp_path), str(final_path))
        except Exception as exc:
            await msg.reply_text(f"❌ Error al mover el audio: {exc}")
            return
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

    # Pending mkdir?
    mkdir_pending = ctx.bot_data.pop("mkdir_pending", None)
    if mkdir_pending:
        parent_path = Path(mkdir_pending["path"])
        new_dir     = parent_path / text.strip()
        try:
            new_dir.mkdir(parents=False, exist_ok=False)
        except FileExistsError:
            await update.message.reply_text(f"⚠️ Ya existe `{text}`.", parse_mode="Markdown")
            # Restore so user can try again or navigate
            txt, kbd = _folder_kbd(ctx, parent_path, 0)
            await ctx.bot.edit_message_text(
                chat_id=ADMIN_ID, message_id=mkdir_pending["msg_id"],
                text=txt, reply_markup=kbd, parse_mode="Markdown",
            )
            return
        except Exception as exc:
            await update.message.reply_text(f"❌ Error al crear carpeta: {exc}")
            return
        # Navigate into the new folder
        pk  = _key(ctx, str(new_dir))
        txt, kbd = _folder_kbd(ctx, new_dir, 0)
        await ctx.bot.edit_message_text(
            chat_id=ADMIN_ID, message_id=mkdir_pending["msg_id"],
            text=txt, reply_markup=kbd, parse_mode="Markdown",
        )
        await update.message.reply_text(f"✅ Carpeta `{text}` creada.", parse_mode="Markdown")
        return

    # Pending custom permission response?
    perm_input = ctx.bot_data.get("perm_input")
    if perm_input:
        # Only consume if the user is NOT replying to a different session's message
        reply = update.message.reply_to_message
        is_reply_to_other = False
        if reply and reply.from_user and reply.from_user.is_bot:
            store = ctx.bot_data.get("msg_to_session", {})
            target_info = store.get(reply.message_id)
            if target_info and target_info.get("session_id") != perm_input["session_id"]:
                is_reply_to_other = True

        if not is_reply_to_other:
            ctx.bot_data.pop("perm_input", None)
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
    q_custom = ctx.bot_data.get("question_custom_input")
    if q_custom:
        # Only consume if the user is NOT replying to a different session's message
        reply = update.message.reply_to_message
        is_reply_to_other = False
        if reply and reply.from_user and reply.from_user.is_bot:
            store = ctx.bot_data.get("msg_to_session", {})
            target_info = store.get(reply.message_id)
            if target_info and target_info.get("session_id") != q_custom["session_id"]:
                is_reply_to_other = True

        if not is_reply_to_other:
            ctx.bot_data.pop("question_custom_input", None)
            req_id     = q_custom["req_id"]
            session_id = q_custom["session_id"]
            q_idx      = q_custom["q_idx"]
            pending    = ctx.bot_data.get("pending_questions", {})
            q_data     = pending.get(req_id)
            if q_data:
                q_data["answers"][q_idx] = [text]
                n_questions = len(q_data["questions"])

                if n_questions == 1 or _all_questions_answered(q_data):
                    filled = [a if a is not None else [] for a in q_data["answers"]]
                    await update.message.reply_text(f"✅ Respuesta enviada: `{text}`", parse_mode="Markdown")
                    await _send_question_answer(ctx.application, req_id, session_id, filled)
                else:
                    await update.message.reply_text(f"✅ Respuesta registrada. Quedan {n_questions - sum(1 for a in q_data['answers'] if a is not None)} pregunta(s) por responder.", parse_mode="Markdown")
                    await _refresh_question_buttons(ctx.application, req_id)
            else:
                await update.message.reply_text("⚠️ La pregunta ya fue respondida o expiró.")
            return

    # Check if this message is a reply to a bot message — always takes priority
    _reply_msg = update.message.reply_to_message
    _reply_target = None
    _untracked_reply = False
    if _reply_msg and _reply_msg.from_user and _reply_msg.from_user.is_bot:
        _reply_target = ctx.bot_data.get("msg_to_session", {}).get(_reply_msg.message_id)
        if _reply_target is None:
            _untracked_reply = True

    if _reply_target:
        # Reply always bypasses send_mode and send_target
        sid       = _reply_target["session_id"]
        directory = _reply_target["directory"]
    else:
        if _untracked_reply:
            await update.message.reply_text(
                "⚠️ No puedo enrutar este reply (mensaje antiguo o no rastreado). Usando la sesión activa."
            )
        # /send flow: if in send mode, show wizard for each message
        send_mode = ctx.bot_data.get("send_mode")
        if send_mode:
            # Store the pending text and show project picker
            ctx.bot_data["send_pending_text"] = text
            await cmd_send(update, ctx)
            return

        # Normal flow: explicit target from picker or resolve target
        send_target = ctx.bot_data.get("send_target")
        if send_target:
            sid       = send_target["session_id"]
            directory = send_target["directory"]
        else:
            target = await _resolve_target(update, ctx)
            if not target:
                await update.message.reply_text(
                    "❌ No hay sesión activa. Usa /open para seleccionar un proyecto."
                )
                return
            sid       = target["session_id"]
            directory = target["directory"]
    cwd_name = Path(directory).name or "?"

    # If session is currently busy, queue the message locally and notify.
    # We double-check with the server to avoid stale statuses (e.g. lost SSE events).
    statuses = ctx.bot_data.get("statuses", {})
    if sid in statuses:
        # Verify the server actually thinks the session is still busy
        server_busy = True
        try:
            sess_info  = await oc.get_session(sid, directory=directory)
            srv_status = sess_info.get("status") or {}
            srv_type   = srv_status.get("type") if isinstance(srv_status, dict) else str(srv_status)
            if srv_type not in ("busy", "retry"):
                # Server says idle — our status is stale, clean it up
                server_busy = False
                logger.info(f"Stale status for {sid[:12]}, server is {srv_type!r} — clearing")
                await _finish_status(ctx.application, sid)
        except Exception:
            pass  # If we can't reach the server, assume busy (safe default)

        if server_busy:
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

    send_mode = ctx.bot_data.get("send_target")
    
    try:
        sess_info = await oc.get_session(sid, directory=directory)
        sess_title = sess_info.get("title") or sid[:12]
        model_obj = sess_info.get("model", {})
        model_short = ""
        if model_obj:
            model_full = f"{model_obj.get('providerID','')}/{model_obj.get('id','')}"
            model_short = model_full.split("/")[-1] if "/" in model_full else model_full
    except Exception:
        sess_title = sid[:12]
        model_short = ""

    send_indicator = " 📤" if send_mode else ""
    session_info = f"📦 `{sess_title[:16]}`{send_indicator}" if send_mode else ""
    
    status_text = f"⚪ *WAITING* | 📂 `{cwd_name}`\n"
    if session_info:
        status_text += f"{session_info}\n"
    status_text += f"🧩 `{model_short or '...'}` | ⏱ `00:00`\n\n_Pulsa_ /esc _para cancelar_"
    
    sent = await update.message.reply_text(
        status_text,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("❌ Cancelar", callback_data="abort:")
        ]]),
    )
    _start_status(ctx.application, sid, directory, sent.message_id, model=model_short, session_title=sess_title, pending=True)
    _track_msg(ctx.application, sent.message_id, sid, directory)

    try:
        await oc.send_message_async(sid, text, directory=directory,
                                    provider_id=provider_id, model_id=model_id)
    except Exception as exc:
        # Clean up the status message if send failed
        await _delete_msg(ctx.bot, ADMIN_ID, sent.message_id)
        ctx.bot_data.get("statuses", {}).pop(sid, None)
        await update.message.reply_text(f"❌ Error al enviar: {exc}")
        return


# ---------------------------------------------------------------------------
# /send — send a prompt to a specific project's active session
# ---------------------------------------------------------------------------

RESTART_FLAG = Path("/tmp/opencode-bot-restarting.flag")

@admin_only
async def cmd_restart(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Restart opencode-bot.service with feedback."""
    msg = await update.message.reply_text("🔄 *Reiniciando opencode-bot...*", parse_mode="Markdown")

    RESTART_FLAG.parent.mkdir(parents=True, exist_ok=True)
    RESTART_FLAG.write_text(str(msg.message_id))

    bot_root = Path(__file__).parent.parent.resolve()
    git_proc = await asyncio.create_subprocess_exec(
        "git", "-C", str(bot_root), "pull",
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    await git_proc.communicate()
    svc_proc = await asyncio.create_subprocess_exec(
        "sudo", "systemctl", "restart", "opencode-bot.service",
    )
    await svc_proc.wait()


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
    active     = await db.get_active()
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
    """Enter send mode or show project picker."""
    pending_text = ctx.bot_data.pop("send_pending_text", None)
    
    if pending_text:
        # User sent text while in send mode - store it and show picker
        ctx.bot_data["send_mode_text"] = pending_text
    elif not ctx.bot_data.get("send_mode"):
        # First call to /send - activate mode
        ctx.bot_data["send_mode"] = True
        await update.message.reply_text(
            "📤 *Modo send activado*\n\n"
            "Cada mensaje que envíes requerirá elegir proyecto y sesión.\n"
            "Usa /endsend para salir del modo.",
            parse_mode="Markdown",
        )
        return
    else:
        # Already in send mode — just acknowledge, don't show picker
        await update.message.reply_text("📤 Modo send activo.")
        return
    
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

    active     = await db.get_active()
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

    text_hint = ""
    if ctx.bot_data.get("send_mode_text"):
        preview = ctx.bot_data["send_mode_text"][:50]
        text_hint = f"📝 `{preview}{'...' if len(preview) < len(ctx.bot_data['send_mode_text']) else ''}`\n\n"
    
    await update.message.reply_text(
        f"📤 *Elige destino*\n\n{text_hint}¿A qué proyecto?",
        reply_markup=InlineKeyboardMarkup(btns),
        parse_mode="Markdown",
    )


async def _show_send_session_picker(q, ctx, directory: str, sessions: list[dict]):
    """Session picker for /send flow — same UI as _show_session_picker but uses send callbacks."""
    cwd_path  = Path(directory)
    dk        = _key(ctx, directory)
    active    = await db.get_active()
    active_sid = (active or {}).get("session_id")

    # Build parent→children map
    by_id = {s["id"]: s for s in sessions}
    roots = []
    for s in sessions:
        pid = s.get("parentID")
        if pid and pid in by_id:
            pass  # child session, skip
        else:
            roots.append(s)

    pending_text = ctx.bot_data.get("send_mode_text")
    text_hint = ""
    if pending_text:
        preview = pending_text[:40]
        text_hint = f"📝 `{preview}{'...' if len(pending_text) > 40 else ''}`\n\n"

    btns = [[InlineKeyboardButton("➕ Nueva sesión", callback_data=f"sendnewsess:{dk}")]]
    for s in roots[:10]:
        sid   = s.get("id", "")
        title = s.get("title") or sid[:12]
        mark  = " ✅" if sid == active_sid else ""
        sk    = _key(ctx, sid)
        btns.append([
            InlineKeyboardButton(
                f"{title[:24]}{mark}",
                callback_data=f"sendsess:{sk}:{dk}",
            ),
            InlineKeyboardButton("🗑", callback_data=f"senddelsess:{sk}:{dk}"),
        ])
    btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])

    await q.edit_message_text(
        f"📂 `{cwd_path.name}` — {len(roots)} sesión{'es' if len(roots) != 1 else ''}\n\n{text_hint}Elige sesión:",
        reply_markup=InlineKeyboardMarkup(btns),
        parse_mode="Markdown",
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

    await _show_send_session_picker(q, ctx, directory, sessions)


async def cb_sendsess(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Session selected for /send → send pending text or ask for text."""
    q = update.callback_query; await q.answer()
    parts = q.data.split(":")
    sid   = _val(ctx, int(parts[1]))
    dk    = int(parts[2])
    directory = _val(ctx, dk)

    pending_text = ctx.bot_data.pop("send_mode_text", None)
    
    if pending_text:
        await q.edit_message_text(
            f"📤 Enviando a `{Path(directory).name}`...",
            parse_mode="Markdown",
        )
        await _do_send_text(ctx.application, pending_text, sid, directory, q.message.chat_id)
    else:
        ctx.bot_data["send_target"] = {"session_id": sid, "directory": directory}
        try:
            sess_info = await oc.get_session(sid, directory=directory)
            title     = sess_info.get("title") or sid[:12]
        except Exception:
            title = sid[:12]
        await q.edit_message_text(
            f"📂 `{Path(directory).name}` · `{title}`\n\n"
            f"📤 *Sesión seleccionada*\n\n"
            f"Escribe el mensaje:",
            parse_mode="Markdown",
        )


async def _do_send_text(app: Application, text: str, sid: str, directory: str, chat_id: int):
    """Send text to session without going through handle_text."""
    cwd_name = Path(directory).name or "?"
    
    try:
        sess_info = await oc.get_session(sid, directory=directory)
        sess_title = sess_info.get("title") or sid[:12]
        model_obj = sess_info.get("model", {})
        model_short = ""
        if model_obj:
            model_full = f"{model_obj.get('providerID','')}/{model_obj.get('id','')}"
            model_short = model_full.split("/")[-1] if "/" in model_full else model_full
    except Exception:
        sess_title = sid[:12]
        model_short = ""

    statuses = app.bot_data.get("statuses", {})
    if sid in statuses:
        queues = app.bot_data.setdefault("queues", {})
        q = queues.setdefault(sid, deque())
        q.append({"text": text, "directory": directory})
        await app.bot.send_message(
            chat_id,
            f"⏳ `{cwd_name}` ocupado. Mensaje encolado.",
            parse_mode="Markdown",
        )
        return

    pending_models = app.bot_data.get("pending_model", {})
    pending = pending_models.pop(sid, None)
    provider_id = pending["providerID"] if pending else None
    model_id    = pending["modelID"]    if pending else None

    sent = await app.bot.send_message(
        chat_id,
        f"⚪ *WAITING* | 📂 `{cwd_name}`\n"
        f"📦 `{sess_title[:16]}` 📤\n"
        f"🧩 `{model_short or '...'}` | ⏱ `00:00`\n\n"
        f"_Pulsa_ /esc _para cancelar_",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("❌ Cancelar", callback_data="abort:")
        ]]),
    )
    _start_status(app, sid, directory, sent.message_id, model=model_short, session_title=sess_title, pending=True)
    _track_msg(app, sent.message_id, sid, directory)

    try:
        await oc.send_message_async(sid, text, directory=directory,
                                    provider_id=provider_id, model_id=model_id)
    except Exception as exc:
        statuses = app.bot_data.get("statuses", {})
        statuses.pop(sid, None)
        await app.bot.delete_message(chat_id=chat_id, message_id=sent.message_id)
        await app.bot.send_message(chat_id, f"❌ Error al enviar: {exc}")


async def cb_sendnewsess(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Nueva sesión desde el picker de /send → muestra selector de proveedor/modelo."""
    q = update.callback_query; await q.answer()
    dk        = int(q.data.split(":")[1])
    directory = _val(ctx, dk)

    # Reuse provider picker; pk == dk (cwd key), after creation send_target will be set
    # We store the send context so cb_provmodel knows to set send_target instead of active
    ctx.bot_data["send_new_sess_dir"] = directory
    await _show_provider_picker(q, ctx, directory)


async def cb_senddelsess(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Delete a session from the /send session picker, then refresh the picker."""
    q = update.callback_query; await q.answer()
    parts = q.data.split(":")
    sid   = _val(ctx, int(parts[1]))
    dk    = int(parts[2])
    directory = _val(ctx, dk)

    # Check for children
    children = await oc.get_session_children(sid, directory=directory or None)
    if children:
        sk = _key(ctx, sid)
        n  = len(children)
        await q.edit_message_text(
            f"⚠️ ¿Borrar esta sesión?\n\n"
            f"OpenCode creó {n} sub-sesión{'es' if n != 1 else ''} interna{'s' if n != 1 else ''} "
            f"que también se eliminarán.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🗑 Borrar", callback_data=f"senddelconfirm:{sk}:{dk}")],
                [InlineKeyboardButton("❌ Cancelar", callback_data=f"sendpick:{dk}")],
            ]),
            parse_mode="Markdown",
        )
        return

    await _do_send_delete_session(q, ctx, sid, directory, dk)


async def cb_senddelconfirm(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Confirmed deletion of session with children from /send picker."""
    q = update.callback_query; await q.answer()
    parts = q.data.split(":")
    sid   = _val(ctx, int(parts[1]))
    dk    = int(parts[2])
    directory = _val(ctx, dk)
    await _do_send_delete_session(q, ctx, sid, directory, dk)


async def _do_send_delete_session(q, ctx, sid: str, directory: str, dk: int):
    """Delete session and refresh the /send session picker."""
    try:
        await oc.delete_session(sid, directory=directory or None)
    except Exception as exc:
        await q.edit_message_text(f"❌ Error: {exc}", parse_mode="Markdown")
        return

    ctx.application.bot_data.get("statuses", {}).pop(sid, None)
    active = await db.get_active()
    if active and active.get("session_id") == sid:
        await db.clear_active()

    try:
        sessions = await oc.list_sessions(directory=directory)
    except Exception:
        sessions = []

    if sessions:
        await _show_send_session_picker(q, ctx, directory, sessions)
    else:
        await q.edit_message_text(
            f"✅ Sesión borrada. No quedan sesiones en `{Path(directory).name}`.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ Nueva sesión", callback_data=f"sendnewsess:{dk}")],
                [InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")],
            ]),
            parse_mode="Markdown",
        )


def _clear_send_mode(bot_data: dict) -> bool:
    """Clear all send mode state. Returns True if send mode was active."""
    was_active = bool(bot_data.pop("send_mode", None) or bot_data.pop("send_target", None))
    bot_data.pop("send_pending_text", None)
    bot_data.pop("send_mode_text", None)
    return was_active


@admin_only
async def cmd_endsend(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Exit send mode."""
    send_mode = ctx.bot_data.get("send_mode")
    send_target = ctx.bot_data.get("send_target")
    _clear_send_mode(ctx.bot_data)
    
    if send_mode:
        await update.message.reply_text(
            "📤 *Modo send desactivado*\n\n"
            "Los mensajes directos ahora van a la sesión activa normal.",
            parse_mode="Markdown",
        )
    elif send_target:
        directory = send_target.get("directory", "")
        cwd_name = Path(directory).name if directory else "?"
        await update.message.reply_text(
            f"📤 Sesión de send liberada (`{cwd_name}`)\n\n"
            f"Los mensajes directos ahora van a la sesión activa normal.",
            parse_mode="Markdown",
        )
    else:
        await update.message.reply_text("⚠️ No estás en modo send.")


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
    active      = await db.get_active()

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
            f"/send — enviar prompt a proyecto (modo persistente)\n"
            f"/endsend — salir del modo send persistente\n"
            f"/sessions — gestionar sesiones (todas o por proyecto)\n"
            f"/models — ver y cambiar modelos disponibles\n"
            f"/close — borrar todas las sesiones de un proyecto\n"
            f"/restart — reiniciar el bot\n"
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
            f"/send — enviar prompt a proyecto (modo persistente)\n"
            f"/endsend — salir del modo send persistente\n"
            f"/sessions — gestionar sesiones (todas o por proyecto)\n"
            f"/models — ver y cambiar modelos disponibles\n"
            f"/close — borrar todas las sesiones de un proyecto\n"
            f"/restart — reiniciar el bot\n"
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
    app.add_handler(CommandHandler("send",     cmd_send))
    app.add_handler(CommandHandler("endsend",  cmd_endsend))
    app.add_handler(CommandHandler("restart",  cmd_restart))

    app.add_handler(CallbackQueryHandler(cb_ob,        pattern=r"^ob:"))
    app.add_handler(CallbackQueryHandler(cb_mkdir,     pattern=r"^mkdir:"))
    app.add_handler(CallbackQueryHandler(cb_os,        pattern=r"^os:"))
    app.add_handler(CallbackQueryHandler(cb_prov,      pattern=r"^prov:"))
    app.add_handler(CallbackQueryHandler(cb_provmodel, pattern=r"^provmodel:"))
    app.add_handler(CallbackQueryHandler(cb_newsess,   pattern=r"^newsess:"))
    app.add_handler(CallbackQueryHandler(cb_actsess,   pattern=r"^actsess:"))
    app.add_handler(CallbackQueryHandler(cb_delsess,    pattern=r"^delsess:"))
    app.add_handler(CallbackQueryHandler(cb_delconfirm, pattern=r"^delconfirm:"))
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
    app.add_handler(CallbackQueryHandler(cb_qsendnow,  pattern=r"^qsendnow:"))
    app.add_handler(CallbackQueryHandler(cb_sendpick,    pattern=r"^sendpick:"))
    app.add_handler(CallbackQueryHandler(cb_sendsess,    pattern=r"^sendsess:"))
    app.add_handler(CallbackQueryHandler(cb_sendnewsess,   pattern=r"^sendnewsess:"))
    app.add_handler(CallbackQueryHandler(cb_senddelsess,   pattern=r"^senddelsess:"))
    app.add_handler(CallbackQueryHandler(cb_senddelconfirm, pattern=r"^senddelconfirm:"))
    app.add_handler(CallbackQueryHandler(cb_sesspick,  pattern=r"^sesspick:"))
    app.add_handler(CallbackQueryHandler(cb_modprov,   pattern=r"^modprov:"))
    app.add_handler(CallbackQueryHandler(cb_setmodel,  pattern=r"^setmodel:"))

    app.add_handler(MessageHandler(filters.Document.ALL | filters.PHOTO | filters.VIDEO, handle_file_upload))
    app.add_handler(MessageHandler(filters.AUDIO | filters.VOICE, handle_audio_upload))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    def _on_sse_done(fut):
        # Relaunch the SSE listener if it ever exits unexpectedly (not on shutdown).
        if fut.cancelled() or app.bot_data.get("_sse_stop"):
            return
        exc = fut.exception()
        if exc is not None:
            logger.error(f"SSE listener crashed: {exc!r}; relaunching in 5s")
        else:
            logger.warning("SSE listener exited unexpectedly; relaunching in 5s")

        async def _relaunch():
            await asyncio.sleep(5)
            if app.bot_data.get("_sse_stop"):
                return
            new_task = asyncio.ensure_future(sse_listener(app))
            new_task.add_done_callback(_on_sse_done)
            app.bot_data["_sse_task"] = new_task

        asyncio.ensure_future(_relaunch())

    async def _start_sse(c):
        task = asyncio.ensure_future(sse_listener(app))
        task.add_done_callback(_on_sse_done)
        app.bot_data["_sse_task"] = task

    app.job_queue.run_once(_start_sse, when=1)

    async def post_init(application: Application):
        await application.bot.delete_my_commands()
        await application.bot.set_my_commands([
            BotCommand("start",    "Estado y menú"),
            BotCommand("open",     "Abrir proyecto / sesión"),
            BotCommand("sessions", "Gestionar sesiones de cualquier proyecto"),
            BotCommand("send",     "Enviar prompt a proyecto (modo persistente)"),
            BotCommand("endsend",  "Salir del modo send persistente"),
            BotCommand("close",    "Cerrar proyecto"),
            BotCommand("models",   "Cambiar modelo de cualquier sesión"),
            BotCommand("restart",  "Reiniciar el bot"),
            BotCommand("esc",      "Cancelar tarea actual"),
        ])
        
        if RESTART_FLAG.exists():
            try:
                old_msg_id = int(RESTART_FLAG.read_text().strip())
                RESTART_FLAG.unlink(missing_ok=True)
                
                active = await db.get_active()
                if active:
                    sid       = active["session_id"]
                    directory = active["directory"]
                    cwd_name  = Path(directory).name
                    
                    session_title = sid[:12]
                    model_label   = "default"
                    try:
                        sess_info   = await oc.get_session(sid, directory=directory)
                        session_title = sess_info.get("title") or sid[:12]
                        model_obj     = sess_info.get("model", {})
                        if model_obj:
                            model_label = f"{model_obj.get('providerID','')}/{model_obj.get('id','')}"
                    except Exception:
                        pass
                    
                    await application.bot.edit_message_text(
                        chat_id=ADMIN_ID,
                        message_id=old_msg_id,
                        text=f"✅ *Bot reiniciado*\n\n"
                             f"📂 `{cwd_name}`\n"
                             f"📦 `{session_title}`\n"
                             f"🧩 `{model_label}`",
                        parse_mode="Markdown",
                    )
                else:
                    await application.bot.edit_message_text(
                        chat_id=ADMIN_ID,
                        message_id=old_msg_id,
                        text="✅ *Bot reiniciado*\n\n⚠️ Sin sesión activa",
                        parse_mode="Markdown",
                    )
            except Exception as e:
                logger.warning(f"Could not send restart notification: {e}")
                RESTART_FLAG.unlink(missing_ok=True)

    app.post_init = post_init

    async def post_shutdown(application: Application):
        application.bot_data["_sse_stop"] = True
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
