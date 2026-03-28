#!/usr/bin/env python3
# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalSubscript=false
# pyright: reportArgumentType=false
# pyright: reportAttributeAccessIssue=false
"""
CryptoAgent Telegram Bot — Versión Robusta

Comandos:
  /start    — Saludos + check server
  /status   — Ver estado de tarea en curso
  /esc      — Cancelar tarea en curso
  /restart  — Reinicia el bot (systemd) con feedback
  /new      — Crea nueva sesión y la guarda en config.json
  /models   — Muestra modelos, elige y guarda en config.json
  /app      — Abre la Mini App dentro de Telegram

Características:
  - Sin timeouts duros (tareas de larga duración)
  - Heartbeat informativo cada 1 min (archivos, tools, progreso)
  - Cancelación explícita con /esc
  - Estado persistente de tareas
"""

import asyncio
import json
import logging
import os
import signal
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import aiohttp
from dotenv import load_dotenv
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, Update, BotCommand
from telegram.constants import ChatAction
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from opencode_server import get_server, OPENCODE_PORT
from opencode_client import get_client, get_models_from_cli, get_last_sse_event_time

load_dotenv(Path(__file__).parent.parent / ".env")

# ─────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ADMIN_ID = int(os.getenv("TELEGRAM_ADMIN_ID", "0"))
BOT_DIR = Path(__file__).parent.parent.resolve()
CONFIG_FILE = BOT_DIR / "config.json"

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.DEBUG,
)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)
logging.getLogger("aiohttp").setLevel(logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.WARNING)


def load_config() -> dict:
    try:
        with open(CONFIG_FILE) as f:
            return json.load(f)
    except Exception:
        return {"session_id": None, "model": None}


def save_config(config: dict):
    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=2)


_config = load_config()

# ─────────────────────────────────────────────────────────
# SEGURIDAD
# ─────────────────────────────────────────────────────────
def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID


# ─────────────────────────────────────────────────────────
# ESTADO GLOBAL - Cola de mensajes y tareas activas
# ─────────────────────────────────────────────────────────
class TaskState:
    """Estado de una tarea en progreso"""
    def __init__(self, session_id: str, chat_id: int, msg_id: int, prompt: str):
        self.session_id = session_id
        self.chat_id = chat_id
        self.msg_id = msg_id
        self.prompt = prompt
        self.start_time = datetime.now()
        self.files_modified: set = set()
        self.tools_used: dict = {}
        self.last_thought = ""
        self.is_cancelled = False
        self.message_count = 0

# Cola de mensajes y tareas activas
_message_queue: asyncio.Queue = asyncio.Queue()
_active_tasks: dict[str, TaskState] = {}  # session_id -> TaskState


# ─────────────────────────────────────────────────────────
# HEARTBEAT MEJORADO (sin timeout, informativo)
# ─────────────────────────────────────────────────────────
class Heartbeat:
    INTERVAL = 60  # 1 minuto - solo informativo, NO cancela tareas

    def __init__(self, bot: Bot, chat_id: int, session_id: str, task_state: TaskState):
        self.bot = bot
        self.chat_id = chat_id
        self.session_id = session_id
        self.task_state = task_state
        self.status_msg_id: Optional[int] = None
        self._task: Optional[asyncio.Task] = None
        self._start_time: float = 0.0

    async def start(self, status_msg_id: int):
        import time
        self.status_msg_id = status_msg_id
        self._start_time = time.monotonic()
        self._task = asyncio.create_task(self._loop())

    async def stop(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    async def _loop(self):
        while not self.task_state.is_cancelled:
            await asyncio.sleep(self.INTERVAL)
            await self._tick()

    async def _tick(self):
        if not self.status_msg_id or self.task_state.is_cancelled:
            return

        import time
        elapsed = int(time.monotonic() - self._start_time)
        mins = elapsed // 60
        secs = elapsed % 60
        
        # === POLLING ACTIVO: Obtener estado de la sesión cada tick ===
        # Esto es lo que hace opencode-telegram: fetch a /session/{id}/message
        try:
            client = get_client()
            messages = await asyncio.wait_for(client.get_messages(self.session_id), timeout=10)
            
            if messages:
                self.task_state.message_count = len(messages)
                
                # Buscar último mensaje del asistente
                for msg in reversed(messages):
                    info = msg.get("info", msg)
                    if info.get("role") != "assistant":
                        continue
                    
                    parts = msg.get("parts", [])
                    for part in parts:
                        part_type = part.get("type", "")
                        
                        # Tool invocation
                        if part_type == "tool-invocation":
                            tool = part.get("tool", {})
                            tool_name = tool.get("name", "unknown")
                            self.task_state.tools_used[tool_name] = self.task_state.tools_used.get(tool_name, 0) + 1
                            
                            # Archivos modificados
                            if tool_name in ["edit", "write", "patch", "multiedit"]:
                                args = tool.get("args", {})
                                file_path = args.get("path", "")
                                if file_path:
                                    short_name = file_path.split("/")[-1]
                                    self.task_state.files_modified.add(short_name)
                        
                        # Reasoning/thinking
                        elif part_type == "reasoning":
                            text = part.get("text", "")
                            if text:
                                self.task_state.last_thought = " ".join(text.split())[:200]
                        
                        # Text
                        elif part_type == "text":
                            text = part.get("text", "")
                            if text:
                                self.task_state.last_thought = " ".join(text.split())[:200]
                    break  # Solo el último mensaje
        except asyncio.TimeoutError:
            logger.warning(f"[Heartbeat] Timeout obteniendo mensajes")
        except Exception as e:
            logger.debug(f"[Heartbeat] Error obteniendo mensajes: {e}")
        
        # Construir estado detallado
        status_lines = [f"⏳ Trabajando... ({mins}:{secs:02d})"]
        
        # Agregar herramientas usadas
        if self.task_state.tools_used:
            tools_str = ", ".join(f"{k}:{v}" for k, v in self.task_state.tools_used.items())
            status_lines.append(f"🔧 {tools_str}")
        
        # Agregar archivos modificados
        if self.task_state.files_modified:
            files = list(self.task_state.files_modified)[:5]
            status_lines.append(f"📁 {len(files)} archivos: {', '.join(files)}")
        
        # Agregar último pensamiento
        if self.task_state.last_thought:
            compact = " ".join(self.task_state.last_thought.split())
            compact = compact.replace("`", "'").replace("[", "(").replace("]", ")").replace("_", " ")
            status_lines.append(f"💬 {compact[:150]}")
        else:
            status_lines.append("🔄 Consultando estado...")
        
        status = "\n".join(status_lines)
        logger.debug(f"[Heartbeat] Status: {status[:100]}")
        
        try:
            await self.bot.edit_message_text(
                chat_id=self.chat_id,
                message_id=self.status_msg_id,
                text=status,
            )
        except BadRequest as e:
            logger.error(f"Heartbeat BadRequest: {e}")
            # Reintentar sin parse_mode
            try:
                await self.bot.edit_message_text(
                    chat_id=self.chat_id,
                    message_id=self.status_msg_id,
                    text=status.replace("*", ""),
                )
            except Exception as e2:
                logger.error(f"Heartbeat fallback error: {e2}")
        except Exception as e:
            logger.error(f"Heartbeat error: {e}")

    async def update_state(self, files: Optional[set] = None, tools: Optional[dict] = None, thought: str = ""):
        """Actualiza el estado de la tarea"""
        if files:
            self.task_state.files_modified.update(files)
        if tools:
            for k, v in tools.items():
                self.task_state.tools_used[k] = self.task_state.tools_used.get(k, 0) + v
        if thought:
            # Limpiar y formatear pensamiento (reasoning)
            clean_thought = " ".join(thought.split())
            self.task_state.last_thought = clean_thought[:200]


# ─────────────────────────────────────────────────────────
# FETCH RESPUESTA FINAL
# ─────────────────────────────────────────────────────────
async def fetch_final_response(session_id: str) -> tuple[str, Optional[str]]:
    """
    Fetch todos los mensajes y extrae el último del asistente.
    Retorna: (texto, error)
    """
    client = get_client()
    try:
        messages = await client.get_messages(session_id)
        
        if not messages:
            return "", "Sin mensajes"
        
        logger.debug(f"fetch_final_response: {len(messages)} mensajes, último role={messages[-1].get('role')} parts={messages[-1].get('parts', [])[:1]}")
        
        # Buscar último mensaje del asistente
        # La API devuelve: {"info": {"role": "assistant", ...}, "parts": [...]}
        for msg in reversed(messages):
            info = msg.get("info", msg)  # compatibilidad con ambas estructuras
            role = info.get("role")
            if role != "assistant":
                continue
            parts = msg.get("parts", [])
            texts = [
                p.get("text", "")
                for p in parts
                if p.get("type") == "text" and p.get("text", "").strip()
            ]
            full_text = "".join(texts).strip()
            if full_text:
                return full_text, None

        return "", "Sin respuesta del asistente"
    except Exception as e:
        return "", str(e)


# ─────────────────────────────────────────────────────────
# COMANDOS
# ─────────────────────────────────────────────────────────
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Saludos + check server (sin reiniciar)"""
    if not update.message or not is_admin(update.effective_user.id):
        if update.message:
            await update.message.reply_text("⛔ Acceso denegado.")
        return

    server = get_server()
    client = get_client()
    
    # Check server
    server_ok = server.is_running
    api_ok = await client.health_check() if server_ok else False
    
    config = load_config()
    session_id = config.get("session_id")
    model = config.get("model") or "automático"
    
    # Check si hay tarea en curso
    task = _active_tasks.get(session_id) if session_id else None
    task_info = ""
    if task:
        elapsed = (datetime.now() - task.start_time).total_seconds()
        mins = int(elapsed // 60)
        task_info = f"\n⚠️ *Tarea en curso:* {mins} min\n"
        if task.files_modified:
            task_info += f"  📁 {len(task.files_modified)} archivos\n"
    
    status_lines = [
        "🤖 *CryptoAgent*",
        "━━━━━━━━━━━━━━━━",
        f"🖥️ Server: {'✅' if server_ok else '❌'} (puerto {OPENCODE_PORT})",
        f"🌐 API: {'✅' if api_ok else '❌'}",
        f"🧠 Modelo: `{model}`",
        f"📝 Sesión: `{session_id[:20] if session_id else 'Ninguna'}...`",
        task_info,
        "",
        "Comandos:",
        "• /status - Ver progreso de tarea",
        "• /esc - Cancelar tarea en curso",
        "• /models - Cambiar modelo",
        "• /new - Nueva sesión",
        "• /sessions - Ver y gestionar sesiones",
        "",
        "Envía cualquier mensaje para procesar.",
    ]

    keyboard = [
        [
            InlineKeyboardButton("🔄 Nueva sesión", callback_data="new_session"),
            InlineKeyboardButton("🧠 Modelos", callback_data="models_menu"),
        ],
        [
            InlineKeyboardButton("📋 Sesiones", callback_data="sessions_menu"),
        ],
    ]

    await update.message.reply_text(
        "\n".join(status_lines),
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def cmd_sessions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Muestra lista de sesiones con botones inline"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Acceso denegado.")
        return

    client = get_client()
    config = load_config()
    current_session = config.get("session_id", "")
    
    msg = await update.message.reply_text("📋 Cargando sesiones...")
    
    try:
        sessions = await asyncio.wait_for(client.list_sessions(), timeout=15)
    except asyncio.TimeoutError:
        await msg.edit_text("❌ Timeout cargando sesiones")
        return
    except Exception as e:
        await msg.edit_text(f"❌ Error: {e}")
        return
    
    if not sessions:
        await msg.edit_text("📭 No hay sesiones")
        return
    
    # Ordenar por fecha (más reciente primero)
    sessions.sort(key=lambda s: s.get("createdAt", ""), reverse=True)
    
    text_lines = [f"📋 *Sesiones* ({len(sessions)})\n", f"📍 Activa: `{current_session[:15]}...`\n"]
    
    keyboard = []
    for i, s in enumerate(sessions[:10]):
        sid = s.get("id", "unknown")
        title = s.get("title", "sin título")
        created = s.get("createdAt", "")
        
        # Marcar sesión actual
        is_current = "✅ " if sid == current_session else ""
        
        # Texto para lista (ID corto + título)
        text_lines.append(f"{is_current}`{sid[:8]}` {title[:30]}")
        
        # Botón: solo título truncado (máx 40 chars para Telegram)
        button_text = title[:40] if len(title) > 40 else title
        if len(title) > 40:
            button_text = title[:37] + "..."
        
        # Agregar check si es activa (solo si hay espacio)
        if is_current and len(button_text) < 35:
            button_text = "✅ " + button_text
        
        keyboard.append([
            InlineKeyboardButton(
                button_text,
                callback_data=f"session:{sid[:20]}"
            )
        ])
    
    # Botones de acción
    keyboard.append([
        InlineKeyboardButton("🗑️ Borrar todas", callback_data="sessions_delete_all"),
        InlineKeyboardButton("← Volver", callback_data="sessions_cancel"),
    ])
    
    await msg.edit_text(
        "\n".join(text_lines),
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def _show_session_menu(bot: Bot, chat_id: int, message_id: int, session_id: str):
    """Muestra menú de acciones para una sesión"""
    client = get_client()
    config = load_config()
    current_session = config.get("session_id", "")
    
    is_current = session_id == current_session
    
    # Obtener título de la sesión
    sessions = await client.list_sessions()
    session_data = next((s for s in sessions if s.get("id") == session_id), None)
    title = session_data.get("title", "sin título") if session_data else "desconocida"
    
    text = f"📋 *Sesión*\n\n"
    text += f"*Nombre:* {title[:50]}\n"
    text += f"*ID:* `{session_id[:20]}...`\n"
    text += f"*Estado:* {'✅ Activa' if is_current else '⚪ Inactiva'}"
    
    keyboard = []
    
    if not is_current:
        keyboard.append([
            InlineKeyboardButton("✅ Activar", callback_data=f"session_activate:{session_id[:20]}"),
        ])
    
    keyboard.append([
        InlineKeyboardButton("🗑️ Borrar", callback_data=f"session_delete:{session_id[:20]}"),
    ])
    keyboard.append([
        InlineKeyboardButton("← Volver", callback_data="sessions_menu"),
    ])
    
    await bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=text,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def cmd_restart(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Reinicia el bot (systemd) con feedback"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Acceso denegado.")
        return

    await update.message.reply_text("🔄 Reiniciando bot (systemd lo relanzará)...")
    
    # Forzar salida limpia - systemd con Restart=always lo relanzará
    os._exit(0)


async def cmd_new(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Crea una nueva sesión y la guarda en config.json"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Acceso denegado.")
        return

    client = get_client()
    msg = await update.message.reply_text("🆕 Creando nueva sesión...")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    try:
        session = await asyncio.wait_for(client.create_session(title=f"session_{timestamp}"), timeout=15)
        session_id = session.get("id") or session.get("sessionID")
        
        if session_id:
            config = load_config()
            config["session_id"] = session_id
            save_config(config)
            
            await msg.edit_text(
                f"✅ Nueva sesión creada\n"
                f"🆔 `{session_id[:20]}...`",
                parse_mode="Markdown",
            )
        else:
            await msg.edit_text("❌ Error: no se obtuvo ID")
    except asyncio.TimeoutError:
        await msg.edit_text("❌ Timeout creando sesión")
    except Exception as e:
        await msg.edit_text(f"❌ Error: `{e}`", parse_mode="Markdown")


async def cmd_models(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Muestra modelos agrupados por proveedor"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Acceso denegado.")
        return

    msg = await update.message.reply_text("🔍 Cargando modelos...")
    await _show_providers_keyboard(context.bot, update.message.chat_id, msg.message_id)


async def cmd_web(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Abre la web de OpenCode en Telegram"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Acceso denegado.")
        return
    
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("🌐 Abrir OpenCode Web", url="http://10.0.0.8:13001"),
    ]])
    
    await update.message.reply_text(
        "🌐 *OpenCode Web*\n\n"
        "Accede a la interfaz web de OpenCode:",
        parse_mode="Markdown",
        reply_markup=keyboard,
    )


async def cmd_app(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Abre la Mini App en el navegador"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Acceso denegado.")
        return
    
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("📊 Abrir Mini App", url="http://10.0.0.8:5000"),
    ]])
    
    await update.message.reply_text(
        "📊 *Mini App*\n\n"
        "Portfolio, alerts y herramientas:",
        parse_mode="Markdown",
        reply_markup=keyboard,
    )


async def cmd_esc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancela la tarea en curso"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Acceso denegado.")
        return
    
    config = load_config()
    session_id = config.get("session_id")
    
    if not session_id:
        await update.message.reply_text("⚠️ No hay sesión activa")
        return
    
    task = _active_tasks.get(session_id)
    if not task:
        await update.message.reply_text("ℹ️ No hay tareas en progreso")
        return
    
    # Marcar como cancelada
    task.is_cancelled = True
    
    # Editar mensaje de heartbeat
    try:
        await context.bot.edit_message_text(
            chat_id=task.chat_id,
            message_id=task.msg_id,
            text=f"❌ Cancelado por usuario\n⏱️ Duración: {(datetime.now() - task.start_time).total_seconds():.0f}s",
        )
    except Exception:
        pass
    
    logger.info(f"Tarea cancelada por usuario: {session_id[:20]}")
    await update.message.reply_text("✅ Tarea cancelada")


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Muestra el estado de la tarea en curso"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Acceso denegado.")
        return
    
    config = load_config()
    session_id = config.get("session_id")
    
    if not session_id:
        await update.message.reply_text("⚠️ No hay sesión activa")
        return
    
    task = _active_tasks.get(session_id)
    if not task:
        await update.message.reply_text("ℹ️ No hay tareas en progreso")
        return
    
    elapsed = (datetime.now() - task.start_time).total_seconds()
    mins = int(elapsed // 60)
    
    status = f"⏳ *Tarea en progreso*\n\n"
    status += f"⏱️ Duración: {mins} min\n"
    status += f"📊 Mensajes: {task.message_count}\n"
    
    if task.files_modified:
        files = ", ".join(list(task.files_modified)[:10])
        status += f"📁 Archivos: {files}\n"
    
    if task.tools_used:
        tools = ", ".join(f"{k}:{v}" for k, v in task.tools_used.items())
        status += f"🔧 Tools: {tools}\n"
    
    if task.last_thought:
        status += f"\n💬 {task.last_thought[:200]}"
    
    await update.message.reply_text(status, parse_mode="Markdown")


async def cmd_rename(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cambia el nombre de la sesión actual"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Acceso denegado.")
        return
    
    client = get_client()
    config = load_config()
    session_id = config.get("session_id")
    
    if not session_id:
        await update.message.reply_text("⚠️ No hay sesión activa. Usa /new para crear una.")
        return
    
    # Obtener nombre actual
    try:
        sessions = await client.list_sessions()
        current = next((s for s in sessions if s.get("id") == session_id), None)
        current_name = current.get("title", "sin nombre") if current else "desconocido"
    except Exception:
        current_name = "desconocido"
    
    # Verificar si hay argumento
    if context.args and len(context.args) > 0:
        new_name = " ".join(context.args)
        
        # Actualizar sesión
        try:
            await client.update_session(session_id, new_name)
            await update.message.reply_text(
                f"✅ Nombre actualizado\n\n"
                f"*Antes:* {current_name}\n"
                f"*Ahora:* {new_name}",
                parse_mode="Markdown",
            )
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {e}")
        return
    
    # Sin argumento: pedir nuevo nombre
    await update.message.reply_text(
        f"✏️ *Renombrar sesión*\n\n"
        f"Sesión actual: `{session_id[:15]}...`\n"
        f"Nombre actual: *{current_name}*\n\n"
        f"Envía el nuevo nombre o usa:\n"
        f"`/rename NuevoNombre`",
        parse_mode="Markdown",
    )
    context.user_data["waiting_rename"] = True


# ─────────────────────────────────────────────────────────
# HANDLER MENSAJES
# ─────────────────────────────────────────────────────────
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    text = update.message.text
    if not text or text.startswith("/"):
        return
    
    # Check si es rename pendiente
    if context.user_data.get("waiting_rename"):
        context.user_data["waiting_rename"] = False
        client = get_client()
        config = load_config()
        session_id = config.get("session_id")
        
        if session_id:
            try:
                await client.update_session(session_id, text)
                await update.message.reply_text(
                    f"✅ Nombre actualizado\n\n"
                    f"*Ahora:* {text}",
                    parse_mode="Markdown",
                )
            except Exception as e:
                await update.message.reply_text(f"❌ Error: {e}")
        else:
            await update.message.reply_text("⚠️ No hay sesión activa")
        return
    
    # Check si es comando personalizado pendiente
    if context.user_data.get("waiting_custom_command"):
        context.user_data["waiting_custom_command"] = False
        logger.info(f"Comando personalizado: {text[:80]}")
        await _process_message(context.bot, update.message.chat_id, text)
        return

    chat_id = update.message.chat_id
    logger.info(f"Mensaje: {text[:80]}")

    # Check si hay tarea en curso
    config = load_config()
    session_id = config.get("session_id")
    if session_id and session_id in _active_tasks:
        task = _active_tasks[session_id]
        elapsed = int((datetime.now() - task.start_time).total_seconds())
        mins = elapsed //60
        await update.message.reply_text(
            f"⏳ Hay una tarea en curso ({ mins} min)\n"
            f"Espera a que termine o usa /esc para cancelarla."
        )
        return

    await _process_message(context.bot, chat_id, text)


async def _process_message(bot: Bot, chat_id: int, text: str):
    client = get_client()
    config = load_config()

    # Asegurar server
    server = get_server()
    if not server.is_running:
        await bot.send_message(chat_id, "⚠️ Server no está corriendo. Usa /start")
        return

    # Asegurar sesión válida
    session_id = config.get("session_id")
    if not session_id:
        await bot.send_message(chat_id, "⚠️ Sin sesión. Usa /new")
        return

    # Verificar sesión existe
    try:
        await client._get(f"/session/{session_id}")
    except Exception:
        logger.warning(f"Sesión {session_id} no existe, creando nueva")
        session = await client.create_session(title=f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        session_id = session.get("id") or session.get("sessionID")
        config["session_id"] = session_id
        save_config(config)

    # Crear estado de tarea
    task_state = TaskState(session_id, chat_id, 0, text)
    _active_tasks[session_id] = task_state

    # Enviar mensaje de status
    status_msg = await bot.send_message(
        chat_id=chat_id,
        text="⏳ Trabajando...",
    )
    task_state.msg_id = status_msg.message_id

    # Iniciar heartbeat
    heartbeat = Heartbeat(bot, chat_id, session_id, task_state)
    await heartbeat.start(status_msg.message_id)

    # Construir payload
    model = config.get("model")
    payload: dict = {"parts": [{"type": "text", "text": text}]}
    if model:
        if "/" in model:
            provider_id, model_id = model.split("/", 1)
            payload["providerID"] = provider_id
            payload["modelID"] = model_id
        else:
            payload["modelID"] = model

    # SSE: escuchar eventos en vivo - SIN TIMEOUT (tareas de larga duración)
    ready_event = asyncio.Event()
    stop_event = asyncio.Event()
    post_error: Optional[str] = None
    post_result: Optional[dict] = None
    assistant_message_id: Optional[str] = None
    part_types: dict[str, str] = {}
    part_order: list[str] = []
    part_buffers: dict[str, str] = {}
    seen_busy = False

    async def sse_listener():
        nonlocal seen_busy, post_error, assistant_message_id
        try:
            # Sin timeout de inactividad - permitir tareas largas (24h+)
            async for evt in client.stream_session_events(session_id, ready_event=ready_event, inactivity_timeout=0):
                if task_state.is_cancelled:
                    break
                    
                etype = evt.get("type", "")
                props = evt.get("properties", {})

                if etype == "session.error":
                    error_msg = props.get("error") or "session.error"
                    if not error_msg or len(error_msg) < 3 or error_msg.lower() in ["que es", "none", "null", "error"]:
                        logger.warning(f"Error inválido del servidor: {error_msg}")
                        post_error = "Error interno del servidor OpenCode"
                    else:
                        post_error = error_msg
                    logger.error(f"Session error: {post_error}")
                    stop_event.set()
                    break

                if etype == "session.status":
                    status_type = props.get("status", {}).get("type", "")
                    if status_type == "busy":
                        seen_busy = True
                    elif status_type == "idle" and seen_busy:
                        stop_event.set()
                        break
                    continue

                if etype == "message.updated":
                    info = props.get("info", {})
                    if info.get("role") == "assistant" and info.get("id"):
                        assistant_message_id = info.get("id")
                        task_state.message_count += 1
                    continue

                if etype == "message.part.updated":
                    part = props.get("part", {})
                    part_id = part.get("id")
                    part_type = part.get("type")
                    message_id = part.get("messageID")
                    logger.debug(f"[SSE] message.part.updated: type={part_type}, id={part_id}")
                    if assistant_message_id and message_id != assistant_message_id:
                        continue
                    if part_id and part_type:
                        part_types[part_id] = part_type
                        if part_id not in part_order:
                            part_order.append(part_id)
                        
                        # Texto normal (respuesta final)
                        if part_type == "text" and part.get("text"):
                            part_buffers[part_id] = part.get("text", "")
                        
                        # Reasoning/thinking (proceso mental del LLM)
                        elif part_type == "reasoning" and part.get("text"):
                            reasoning_text = part.get("text", "")
                            logger.info(f"[Heartbeat] Reasoning received: {reasoning_text[:100]}...")
                            await heartbeat.update_state(thought=reasoning_text[:200])
                        
                        # Extraer herramienta usada
                        if part_type == "tool-invocation":
                            tool = part.get("tool", {})
                            tool_name = tool.get("name", "unknown")
                            task_state.tools_used[tool_name] = task_state.tools_used.get(tool_name, 0) + 1
                            
                            # Extraer archivo si es edit/write
                            if tool_name in ["edit", "write", "patch"]:
                                args = tool.get("args", {})
                                file_path = args.get("path", "")
                                if file_path:
                                    short_name = file_path.split("/")[-1]
                                    task_state.files_modified.add(short_name)
                                    await heartbeat.update_state(files={short_name})
                    continue

                if etype == "message.part.delta":
                    part_id = props.get("partID")
                    message_id = props.get("messageID")
                    field = props.get("field")
                    delta = props.get("delta", "")
                    logger.debug(f"[SSE] message.part.delta: field={field}, part_id={part_id}")
                    if assistant_message_id and message_id != assistant_message_id:
                        continue
                    if not part_id:
                        continue
                    
# Delta de texto
                    if field == "text" and delta:
                        part_buffers[part_id] = part_buffers.get(part_id, "") + delta
                        # También acumular thought para heartbeat
                        if len(delta) > 20:  # Solo frases más largas
                            await heartbeat.update_state(thought=delta[:200])
                    
                    # Delta de reasoning
                    elif field == "reasoning" and delta:
                        await heartbeat.update_state(thought=delta[:200])
        except Exception as e:
            logger.error(f"SSE error: {e}")
            post_error = str(e)
            stop_event.set()

    sse_task = asyncio.create_task(sse_listener())

    try:
        # Esperar a que SSE esté listo (sin timeout duro)
        try:
            await asyncio.wait_for(ready_event.wait(), timeout=30)
        except asyncio.TimeoutError:
            post_error = "Timeout inicial de conexión (30s)"
            logger.error(post_error)
            await heartbeat.stop()
            try:
                await bot.delete_message(chat_id, status_msg.message_id)
            except Exception:
                pass
            sse_task.cancel()
            if session_id in _active_tasks:
                del _active_tasks[session_id]
            await bot.send_message(chat_id, f"❌ Error: `{post_error}`", parse_mode="Markdown")
            return
        
        logger.info(f"Enviando mensaje a sesión {session_id[:20]}... (modelo: {model})")
        post_task = asyncio.create_task(client.send_message(session_id, payload))

        # SIN TIMEOUT - esperar indefinidamente hasta que la tarea termine o sea cancelada
        await stop_event.wait()

        post_result = await post_task
    except asyncio.TimeoutError as e:
        post_error = "Timeout de conexión inicial"
        logger.error(f"Error procesando mensaje: {post_error}", exc_info=True)
        await heartbeat.stop()
        try:
            await bot.delete_message(chat_id, status_msg.message_id)
        except Exception:
            pass
        sse_task.cancel()
        if session_id in _active_tasks:
            del _active_tasks[session_id]
        await bot.send_message(chat_id, f"❌ Error: `{post_error}`", parse_mode="Markdown")
        return
    except Exception as e:
        post_error = str(e)
        logger.error(f"Error procesando mensaje: {post_error}", exc_info=True)
        await heartbeat.stop()
        try:
            await bot.delete_message(chat_id, status_msg.message_id)
        except Exception:
            pass
        sse_task.cancel()
        if session_id in _active_tasks:
            del _active_tasks[session_id]
        
        if not post_error or len(post_error) < 3 or post_error.lower() in ["que es", "none", "null"]:
            post_error = "Error de comunicación con el servidor"
        
        await bot.send_message(chat_id, f"❌ Error: `{post_error}`", parse_mode="Markdown")
        return
    finally:
        sse_task.cancel()
        await heartbeat.stop()
        if session_id in _active_tasks:
            del _active_tasks[session_id]

    # Prioridad de salida:
    # 1. Texto capturado por SSE desde parts de tipo text.
    # 2. Texto final devuelto por el POST.
    # 3. Fetch final de mensajes como último fallback.
    logger.info(f"[Resumen] part_order: {len(part_order)} parts, part_types: {list(part_types.values())[:5]}")
    logger.info(f"[Resumen] part_buffers keys: {list(part_buffers.keys())[:5]}")
    
    sse_text = "".join(
        part_buffers[part_id]
        for part_id in part_order
        if part_types.get(part_id) == "text" and part_buffers.get(part_id, "").strip()
    ).strip()
    
    logger.info(f"[Resumen] sse_text length: {len(sse_text)}")

    final_text = ""

    if post_result:
        parts = post_result.get("parts", [])
        texts = [p.get("text", "") for p in parts if p.get("type") == "text" and p.get("text", "").strip()]
        final_text = "".join(texts).strip()

    if not final_text and not post_error:
        final_text, post_error = await fetch_final_response(session_id)

    if not final_text:
        final_text = sse_text

    # Borrar status y enviar respuesta
    try:
        await bot.delete_message(chat_id, status_msg.message_id)
    except Exception:
        pass

    if post_error:
        # Validar y limpiar error
        if not post_error or len(post_error.strip()) < 3 or post_error.lower() in ["que es", "none", "null", "error"]:
            logger.warning(f"Error inválido capturado: '{post_error}', usando mensaje genérico")
            post_error = "Timeout de comunicación. Intenta de nuevo."
        else:
            logger.error(f"Error final: {post_error}")
        await bot.send_message(chat_id, f"❌ Error: `{post_error}`", parse_mode="Markdown")
    elif final_text:
        await _send_long_message(bot, chat_id, final_text)
    else:
        logger.warning("Sin respuesta del asistente ni error capturado")
        await bot.send_message(chat_id, "❌ Error: Sin respuesta del asistente")


async def _send_long_message(bot: Bot, chat_id: int, text: str, parse_mode: str = "Markdown"):
    max_len = 4096
    
    # Sanitizar texto para Markdown problemático
    def sanitize_markdown(t: str) -> str:
        # Si tiene backticks sin cerrar o brackets raros, quitarlos
        backtick_count = t.count('`')
        if backtick_count % 2 != 0:
            t = t.replace('`', "'")
        return t
    
    text = sanitize_markdown(text)
    
    if len(text) <= max_len:
        try:
            await bot.send_message(chat_id, text, parse_mode=parse_mode)
        except BadRequest:
            # Fallback sin Markdown
            text_clean = text.replace('*', '').replace('_', '').replace('`', "'").replace('[', '(').replace(']', ')')
            await bot.send_message(chat_id, text_clean)
        return

    for i in range(0, len(text), max_len):
        chunk = text[i:i + max_len]
        chunk = sanitize_markdown(chunk)
        try:
            await bot.send_message(chat_id, chunk, parse_mode=parse_mode)
        except BadRequest:
            chunk_clean = chunk.replace('*', '').replace('_', '').replace('`', "'").replace('[', '(').replace(']', ')')
            await bot.send_message(chat_id, chunk_clean)


# ─────────────────────────────────────────────────────────
# CALLBACKS
# ─────────────────────────────────────────────────────────
async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if not is_admin(query.from_user.id):
        return

    data = query.data
    chat_id = query.message.chat_id
    bot = context.bot

    if data == "new_session":
        await cmd_new(update, context)
        return

    if data == "sessions_menu":
        await cmd_sessions(update, context)
        return

    if data == "sessions_cancel":
        await bot.delete_message(chat_id, query.message.message_id)
        return

    if data == "sessions_delete_all":
        client = get_client()
        await query.edit_message_text("🗑️ Borrando todas las sesiones...")
        try:
            sessions = await asyncio.wait_for(client.list_sessions(), timeout=15)
            deleted = 0
            for s in sessions:
                sid = s.get("id")
                if sid:
                    try:
                        await client.delete_session(sid)
                        deleted += 1
                    except Exception:
                        pass
            # Crear nueva sesión
            session = await client.create_session(title=f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
            new_sid = session.get("id") or session.get("sessionID")
            config = load_config()
            config["session_id"] = new_sid
            save_config(config)
            await query.edit_message_text(f"✅ Borradas {deleted} sesiones\n🆕 Nueva sesión: `{new_sid[:15]}...`", parse_mode="Markdown")
        except Exception as e:
            await query.edit_message_text(f"❌ Error: {e}")
        return

    if data.startswith("session:"):
        session_id = data.split(":", 1)[1]
        await _show_session_menu(bot, chat_id, query.message.message_id, session_id)
        return

    if data.startswith("session_activate:"):
        session_id = data.split(":", 1)[1]
        config = load_config()
        config["session_id"] = session_id
        save_config(config)
        await query.edit_message_text(f"✅ Sesión activada: `{session_id[:15]}...`", parse_mode="Markdown")
        return

    if data.startswith("session_delete:"):
        session_id = data.split(":", 1)[1]
        client = get_client()
        config = load_config()
        current = config.get("session_id", "")
        
        await query.edit_message_text(f"🗑️ Borrando sesión...")
        try:
            # Obtener lista de sesiones antes de borrar
            sessions = await client.list_sessions()
            await client.delete_session(session_id)
            
            # Si era la sesión actual, seleccionar la más reciente o crear nueva
            if session_id == current:
                # Filtrar la sesión borrada y ordenar por fecha
                remaining = [s for s in sessions if s.get("id") != session_id]
                
                if remaining:
                    # Seleccionar la más reciente (ya están ordenadas por createdAt)
                    remaining.sort(key=lambda s: s.get("createdAt", ""), reverse=True)
                    new_sid = remaining[0].get("id")
                    config["session_id"] = new_sid
                    save_config(config)
                    await query.edit_message_text(f"✅ Sesión borrada\n📋 Activada: `{new_sid[:15]}...`", parse_mode="Markdown")
                else:
                    # No hay sesiones, crear nueva
                    session = await client.create_session(title=f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
                    new_sid = session.get("id") or session.get("sessionID")
                    config["session_id"] = new_sid
                    save_config(config)
                    await query.edit_message_text(f"✅ Sesión borrada\n🆕 Nueva sesión: `{new_sid[:15]}...`", parse_mode="Markdown")
            else:
                await query.edit_message_text(f"✅ Sesión borrada: `{session_id[:15]}...`", parse_mode="Markdown")
        except Exception as e:
            await query.edit_message_text(f"❌ Error: {e}")
        return

    if data == "models_menu":
        await _show_providers_keyboard(bot, chat_id, query.message.message_id)
        return

    if data.startswith("provider:"):
        provider = data.split(":", 1)[1]
        models_by_provider = get_models_from_cli()
        models = models_by_provider.get(provider, [])
        
        if not models:
            return

        keyboard = []
        for model in models:
            mid = model["id"]
            config = load_config()
            is_current = config.get("model") == mid
            label = f"✅ {model['name']}" if is_current else model["name"]
            keyboard.append([InlineKeyboardButton(label, callback_data=f"model_select:{mid}")])

        keyboard.append([InlineKeyboardButton("← Volver", callback_data="models_menu")])

        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=query.message.message_id,
            text=f"🔷 *{provider}* — elige modelo:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    if data.startswith("model_select:"):
        model_id = data.split(":", 1)[1]
        client = get_client()

        msg = await bot.send_message(chat_id, "🔄 Cambiando modelo...")

        # Borrar sesiones
        await client.delete_all_sessions()

        # Crear nueva
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        try:
            session = await client.create_session(title=f"session_{timestamp}")
            session_id = session.get("id") or session.get("sessionID")

            config = load_config()
            config["model"] = model_id
            config["session_id"] = session_id
            save_config(config)

            await msg.edit_text(
                f"✅ Modelo: `{model_id}`\n"
                f"🆕 Sesión: `{session_id[:20]}...`",
                parse_mode="Markdown",
            )
        except Exception as e:
            await msg.edit_text(f"❌ Error: `{e}`", parse_mode="Markdown")
        return


async def _show_providers_keyboard(bot: Bot, chat_id: int, message_id: int):
    models_by_provider = get_models_from_cli()

    if not models_by_provider:
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text="⚠️ No se pudieron cargar modelos.",
        )
        return

    keyboard = []
    for provider in sorted(models_by_provider.keys()):
        count = len(models_by_provider[provider])
        keyboard.append([
            InlineKeyboardButton(f"{provider.capitalize()} ({count})", callback_data=f"provider:{provider}")
        ])

    config = load_config()
    current = config.get("model") or "automático"

    keyboard.append([InlineKeyboardButton("← Volver", callback_data="models_cancel")])

    await bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=f"🧠 *Proveedor* (actual: `{current}`)",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


# ─────────────────────────────────────────────────────────
# ERROR HANDLER
# ─────────────────────────────────────────────────────────
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Error: {context.error}", exc_info=context.error)


# ─────────────────────────────────────────────────────────
# STARTUP / SHUTDOWN
# ─────────────────────────────────────────────────────────
async def post_init(application: Application):
    server = get_server()
    client = get_client()
    bot = application.bot
    
    # Comprobar server y levantarlo si no está
    logger.info(f"🚀 Iniciando OpenCode server en puerto {OPENCODE_PORT}...")
    if not await server.start():
        logger.error(f"❌ No se pudo iniciar server en puerto {OPENCODE_PORT}")
        return

    # Check sesión en config
    config = load_config()
    session_id = config.get("session_id")
    
    if session_id:
        valid = False
        for attempt in range(3):
            try:
                await asyncio.sleep(0.5 * attempt)
                await client._get(f"/session/{session_id}")
                valid = True
                logger.info(f"✅ Sesión válida: {session_id[:20]}")
                break
            except Exception as e:
                logger.warning(f"Intento {attempt+1}: Sesión inválida {session_id[:20]}: {e}")
        
        if not valid:
            logger.warning(f"⚠️ Sesión inválida tras 3 intentos: {session_id[:20]}, se creará una nueva en /new")
            # No borrar la sesión del config, solo marcarla como inválida temporalmente
            # El usuario puede usar /new para crear una nueva

   
    # Notificar restart pendiente
    restart_info = config.get("restart_pending")
    if restart_info:
        try:
            chat_id = restart_info["chat_id"]
            msg_id = restart_info["message_id"]
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg_id,
                text="✅ Bot reiniciado correctamente.",
            )
        except Exception:
            pass
        config.pop("restart_pending", None)
        save_config(config)
    
    # Configurar menú de comandos (autocomplete)
    commands = [
        BotCommand("start", "🤖 Estado del bot y menú principal"),
        BotCommand("status", "📊 Ver estado de tarea en curso"),
        BotCommand("esc", "❌ Cancelar tarea en curso"),
        BotCommand("restart", "🔄 Reiniciar el bot"),
        BotCommand("new", "🗑️ Crear nueva sesión"),
        BotCommand("models", "🧠 Seleccionar modelo"),
        BotCommand("sesiones", "📋 Gestionar sesiones (activar/borrar)"),
        BotCommand("rename", "✏️ Renombrar sesión actual"),
        BotCommand("web", "🌐 Abrir interfaz web de OpenCode"),
        BotCommand("app", "📊 Abrir Mini App (portfolio, alerts)"),
    ]
    
    try:
        await bot.set_my_commands(commands)
        logger.info(f"✅ Comandos registrados: {[c.command for c in commands]}")
    except Exception as e:
        logger.error(f"Error registrando comandos: {e}")


async def post_shutdown(application: Application):
    pass



# ─────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────
def main():
    logger.info(f"🚀 CryptoAgent Bot | Admin: {ADMIN_ID}")
    config = load_config()
    server = get_server()
    client = get_client()

    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .read_timeout(120)
        .write_timeout(120)
        .build()
    )

    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("status", cmd_status))
    application.add_handler(CommandHandler("esc", cmd_esc))
    application.add_handler(CommandHandler("restart", cmd_restart))
    application.add_handler(CommandHandler("new", cmd_new))
    application.add_handler(CommandHandler("models", cmd_models))
    application.add_handler(CommandHandler("sesiones", cmd_sessions))
    application.add_handler(CommandHandler("rename", cmd_rename))
    application.add_handler(CommandHandler("web", cmd_web))
    application.add_handler(CommandHandler("app", cmd_app))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_handler(CallbackQueryHandler(handle_callback))
    application.add_error_handler(error_handler)

    
    logger.info("✅ Bot iniciado. Esperando mensajes...")
    application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

    

if __name__ == "__main__":
    main()
