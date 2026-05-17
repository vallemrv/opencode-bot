#!/usr/bin/env python3
# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalSubscript=false
# pyright: reportArgumentType=false
# pyright: reportAttributeAccessIssue=false
"""
OpencodeAgent Telegram Bot — Versión Robusta

Comandos:
  /start    — Saludos + check server
  /status   — Ver estado de tarea en curso
  /esc      — Cancelar tarea en curso
  /restart  — Reinicia el bot (systemd) con feedback
  /new      — Crea nuevo proyecto
  /models   — Muestra modelos, elige y guarda en config.json

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
from logging.handlers import RotatingFileHandler
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

from opencode_server import get_server, get_workspace, set_workspace
from opencode_client import get_client, get_models_from_cli, get_last_sse_event_time, OPENCODE_PORT
from project_manager import get_manager, ProjectInfo, SessionInfo
from bot_server import get_server as get_bot_server, BOT_PORT, ensure_server_running, DEFAULT_WORKSPACE

load_dotenv(Path(__file__).parent.parent / ".env")

# ─────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ADMIN_ID = int(os.getenv("TELEGRAM_ADMIN_ID", "0"))
BOT_DIR = Path(__file__).parent.parent.resolve()
CONFIG_FILE = BOT_DIR / "config.json"
LOG_DIR = BOT_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[
        RotatingFileHandler(
            LOG_DIR / "bot.log",
            maxBytes=5*1024*1024,
            backupCount=3,
        ),
        logging.StreamHandler(),
    ],
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
        f.flush()
        os.fsync(f.fileno())  # Forzar escritura a disco


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
    def __init__(self, session_id: str, chat_id: int, msg_id: int, prompt: str, project_name: str = "", session_title: str = ""):
        self.session_id = session_id
        self.chat_id = chat_id
        self.msg_id = msg_id
        self.prompt = prompt
        self.project_name = project_name
        self.session_title = session_title
        self.start_time = datetime.now()
        self.files_modified: set = set()
        self.tools_used: dict = {}
        self.last_thought = ""
        self.is_cancelled = False
        self.message_count = 0
        self.tokens_used: dict = {}  # {input, output, total, cache}
        self.context_percent: float = 0.0

# Tareas activas - solo para tracking local
# OpenCode maneja internamente la cola de mensajes por sesión
_active_tasks: dict[str, TaskState] = {}  # session_id -> TaskState


# ─────────────────────────────────────────────────────────
# HEARTBEAT MEJORADO (sin timeout, informativo)
# ─────────────────────────────────────────────────────────
class Heartbeat:
    INTERVAL = 60  # 1 minuto - solo informativo, NO cancela tareas

    # Model context windows (tokens)
    CONTEXT_WINDOWS = {
        "claude": 200000,
        "anthropic": 200000,
        "gpt-4": 128000,
        "openai": 128000,
        "gemini": 1000000,
        "google": 1000000,
        "qwen": 128000,
        "alibaba": 128000,
        "default": 128000,
    }

    def __init__(self, bot: Bot, chat_id: int, session_id: str, task_state: TaskState, model_id: str = ""):
        self.bot = bot
        self.chat_id = chat_id
        self.session_id = session_id
        self.task_state = task_state
        self.model_id = model_id
        self.status_msg_id: Optional[int] = None
        self._task: Optional[asyncio.Task] = None
        self._start_time: float = 0.0

    def _get_context_window(self) -> int:
        """Get context window for current model"""
        if not self.model_id:
            return self.CONTEXT_WINDOWS["default"]
        
        for key, window in self.CONTEXT_WINDOWS.items():
            if key in self.model_id.lower():
                return window
        
        return self.CONTEXT_WINDOWS["default"]

    def _calc_context_percent(self, tokens: dict) -> float:
        """Calculate context usage percentage"""
        total = tokens.get("total", 0) or tokens.get("input", 0) + tokens.get("output", 0)
        if total <= 0:
            return 0.0
        
        context_window = self._get_context_window()
        return min(100.0, (total / context_window) * 100)

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
        
        manager = get_manager()
        project_name = self.task_state.project_name
        session_title = self.task_state.session_title[:15] if self.task_state.session_title else self.session_id[:12]
        
        try:
            client = get_client()
            messages = await asyncio.wait_for(client.get_messages(self.session_id), timeout=10)
            
            if messages:
                self.task_state.message_count = len(messages)
                
                for msg in reversed(messages):
                    info = msg.get("info", msg)
                    if info.get("role") != "assistant":
                        continue
                    
                    parts = msg.get("parts", [])
                    for part in parts:
                        part_type = part.get("type", "")
                        
                        if part_type == "tool-invocation":
                            tool = part.get("tool", {})
                            tool_name = tool.get("name", "unknown")
                            self.task_state.tools_used[tool_name] = self.task_state.tools_used.get(tool_name, 0) + 1
                            
                            if tool_name in ["edit", "write", "patch", "multiedit"]:
                                args = tool.get("args", {})
                                file_path = args.get("path", "")
                                if file_path:
                                    short_name = file_path.split("/")[-1]
                                    self.task_state.files_modified.add(short_name)
                        
                        elif part_type == "reasoning":
                            text = part.get("text", "")
                            if text:
                                self.task_state.last_thought = " ".join(text.split())[:150]
                        
                        elif part_type == "text":
                            text = part.get("text", "")
                            if text:
                                self.task_state.last_thought = " ".join(text.split())[:150]
                        
                        elif part_type == "step-finish":
                            tokens = part.get("tokens", {})
                            if tokens:
                                self.task_state.tokens_used = tokens
                                self.task_state.context_percent = self._calc_context_percent(tokens)
                    
                    break
        except asyncio.TimeoutError:
            logger.warning(f"[Heartbeat] Timeout")
        except Exception as e:
            logger.debug(f"[Heartbeat] Error: {e}")
        
        context_pct = self.task_state.context_percent
        context_str = f"{context_pct:.0f}%" if context_pct > 0 else "---"
        
        header = f"⏳ {project_name} | {session_title} ({context_str})"
        elapsed_str = f"{mins}:{secs:02d}"
        
        status_lines = [header, elapsed_str]
        
        if self.task_state.tools_used:
            top_tools = list(self.task_state.tools_used.items())[:3]
            tools_str = ", ".join(f"{k}" for k, v in top_tools)
            status_lines.append(f"🔧 {tools_str}")
        
        if self.task_state.files_modified:
            files = list(self.task_state.files_modified)[:3]
            status_lines.append(f"📁 {', '.join(files)}")
        
        if self.task_state.last_thought:
            compact = " ".join(self.task_state.last_thought.split())
            compact = compact.replace("`", "'").replace("[", "(").replace("]", ")")
            status_lines.append(f"💬 {compact[:100]}")
        else:
            status_lines.append("🔄 Procesando...")
        
        status = "\n".join(status_lines)
        
        try:
            await self.bot.edit_message_text(
                chat_id=self.chat_id,
                message_id=self.status_msg_id,
                text=status,
            )
        except BadRequest as e:
            logger.error(f"Heartbeat BadRequest: {e}")
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

    async def update_state(self, files: Optional[set] = None, tools: Optional[dict] = None, thought: str = "", tokens: dict = None):
        """Actualiza el estado de la tarea"""
        if files:
            self.task_state.files_modified.update(files)
        if tools:
            for k, v in tools.items():
                self.task_state.tools_used[k] = self.task_state.tools_used.get(k, 0) + v
        if thought:
            clean_thought = " ".join(thought.split())
            self.task_state.last_thought = clean_thought[:150]
        if tokens:
            self.task_state.tokens_used = tokens
            self.task_state.context_percent = self._calc_context_percent(tokens)


# ─────────────────────────────────────────────────────────
# FETCH RESPUESTA FINAL
# ─────────────────────────────────────────────────────────
async def fetch_final_response(session_id: str) -> tuple[str, Optional[str]]:
    """
    Fetch todos los mensajes y extrae el último del asistente.
    Busca texto en CUALQUIER tipo de parte (text, reasoning, etc.).
    Retorna: (texto, error)
    """
    client = get_client()
    try:
        messages = await client.get_messages(session_id)
        
        if not messages:
            return "", "Sin mensajes"
        
        logger.debug(f"fetch_final_response: {len(messages)} mensajes")
        
        # Buscar último mensaje del asistente
        for msg in reversed(messages):
            info = msg.get("info", msg)
            role = info.get("role")
            if role != "assistant":
                continue
            
            parts = msg.get("parts", [])
            logger.debug(f"  Assistant msg parts: {[p.get('type') for p in parts]}")
            
            # Buscar texto en TODOS los tipos de partes
            texts = []
            for p in parts:
                ptype = p.get("type", "")
                # Texto normal
                if p.get("text"):
                    texts.append(p["text"])
                # Reasoning
                elif p.get("reasoning"):
                    texts.append(p["reasoning"])
                # Contenido alternativo
                elif p.get("content"):
                    texts.append(p["content"])
            
            full_text = "".join(texts).strip()
            if full_text:
                logger.info(f"fetch_final_response: encontrado {len(full_text)} chars")
                return full_text, None
            
            # Si hay tokens de output pero no texto, el asistente procesó pero no respondió
            for p in parts:
                if p.get("type") == "step-finish":
                    tokens = p.get("tokens", {})
                    if tokens.get("output", 0) > 0:
                        logger.info(f"fetch_final_response: hay tokens ({tokens}) pero no texto")
                        return "", None  # Sin error, pero sin texto

        return "", "Sin respuesta del asistente"
    except Exception as e:
        logger.error(f"fetch_final_response error: {e}")
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
        "• /new - Nuevo proyecto",
        "• /sessions - Ver y gestionar sesiones",
        "",
        "Envía cualquier mensaje para procesar.",
    ]

    keyboard = [
        [
            InlineKeyboardButton("📁 Abrir proyecto", callback_data=f"explorer:{DEFAULT_WORKSPACE}"),
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
    """
    Muestra lista de sesiones del workspace actual con botones inline.
    Muestra sesiones truncadas + botones: Borrar todas, Nueva
    """
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Acceso denegado.")
        return

    client = get_client()
    config = load_config()
    current_session = config.get("session_id", "")
    workspace = config.get("workspace", "N/A")
    
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
        await msg.edit_text(
            "📭 No hay sesiones\n\n"
            "Usa /new para crear un proyecto nuevo.",
            parse_mode="Markdown",
        )
        return
    
    # Ordenar por fecha (más reciente primero)
    sessions.sort(key=lambda s: s.get("time", {}).get("created", 0), reverse=True)
    
    # Filtrar sesiones del workspace actual (si tenemos projectID)
    # Opencode no filtra por workspace, mostramos todas pero indicamos workspace
    
    text_lines = [
        f"📋 *Sesiones* ({len(sessions)})\n",
        f"📍 Activa: `{current_session[:15]}...`\n",
        f"📁 Workspace: `{workspace}`\n",
    ]
    
    keyboard = []
    for i, s in enumerate(sessions[:10]):
        sid = s.get("id", "unknown")
        title = s.get("title", "sin título")
        
        # Marcar sesión actual
        is_current = "✅ " if sid == current_session else ""
        
        # Botón: título truncado (máx 30 chars para inline)
        button_text = title[:30] if len(title) > 30 else title
        if len(title) > 30:
            button_text = title[:27] + "..."
        
        # Agregar check si es activa
        if is_current:
            button_text = "✅ " + button_text
        
        keyboard.append([
            InlineKeyboardButton(
                button_text,
                callback_data=f"session:{sid[:20]}"
            )
        ])
    
    # Botones de acción
    keyboard.append([
        InlineKeyboardButton("🆕 Nueva", callback_data="new_session"),
        InlineKeyboardButton("🗑️ Borrar todas", callback_data="sessions_delete_all"),
    ])
    keyboard.append([
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
    """
    Reinicia el bot con proceso completo:
    1. Git pull
    2. Build/install dependencies
    3. Restart systemd service
    Con feedback en cada paso
    """
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Acceso denegado.")
        return

    status_msg = await update.message.reply_text("🔄 *Iniciando restart...*", parse_mode="Markdown")
    
    async def run_step(name: str, cmd: str, cwd: str = None) -> tuple[bool, str]:
        """Run a command and return (success, output)"""
        try:
            proc = await asyncio.create_subprocess_shell(
                cmd,
                cwd=cwd or str(BOT_DIR),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await proc.communicate()
            output = (stdout.decode() + stderr.decode()).strip()
            success = proc.returncode == 0
            return success, output[:500]  # Limit output
        except Exception as e:
            return False, str(e)
    
    # Step 1: Git pull
    await status_msg.edit_text("🔄 *Step 1: Git pull...*", parse_mode="Markdown")
    success, output = await run_step("Git pull", "git pull")
    if success:
        if "Already up to date" in output or "Actualizado" in output:
            pull_result = "✅ No hay cambios nuevos"
        else:
            pull_result = f"✅ Pull exitoso\n{output[:200]}"
    else:
        pull_result = f"⚠️ Pull: {output[:200]}"
    logger.info(f"Restart: git pull -> {pull_result[:100]}")
    
    # Step 2: Build/install
    await status_msg.edit_text("🔄 *Step 2: Build...*", parse_mode="Markdown")
    
    has_package_json = (BOT_DIR / "package.json").exists()
    has_requirements = (BOT_DIR / "requirements.txt").exists()
    
    if has_package_json:
        success, output = await run_step("npm install", "npm install --production")
        build_result = f"✅ npm install: {output[:100]}" if success else f"⚠️ npm: {output[:100]}"
    elif has_requirements:
        success, output = await run_step("pip install", "source .venv/bin/activate && pip install -q -r requirements.txt")
        build_result = f"✅ pip install: OK" if success else f"⚠️ pip: {output[:100]}"
    else:
        build_result = "✅ No build required"
    logger.info(f"Restart: build -> {build_result[:100]}")
    
    # Step 3: Restart systemd
    await status_msg.edit_text("🔄 *Step 3: Restart service...*", parse_mode="Markdown")
    success, output = await run_step("systemctl restart", "sudo systemctl restart opencode-bot.service")
    restart_result = "✅ Service restarted" if success else f"❌ Restart failed: {output[:100]}"
    logger.info(f"Restart: systemctl -> {restart_result[:100]}")
    
    # Final status
    await asyncio.sleep(2)
    success, status_output = await run_step("systemctl status", "sudo systemctl is-active opencode-bot.service")
    is_active = success and "active" in status_output.lower()
    
    config = load_config()
    config["restart_pending"] = {
        "chat_id": status_msg.chat_id,
        "message_id": status_msg.message_id,
        "pull_result": pull_result,
        "build_result": build_result,
    }
    save_config(config)
    
    final_status = "✅ *Bot reiniciado*" if is_active else "❌ *Bot no arrancó*"
    final_msg = f"{final_status}\n\n"
    final_msg += f"📥 Git: {pull_result[:100]}\n"
    final_msg += f"📦 Build: {build_result[:100]}\n"
    final_msg += f"🔄 Service: {restart_result[:100]}"
    
    await status_msg.edit_text(final_msg, parse_mode="Markdown")
    
    if is_active:
        await asyncio.sleep(3)
        try:
            health_check = await run_step("health", f"curl -s http://localhost:{BOT_PORT}/health")
            if health_check[0]:
                await status_msg.edit_text(f"{final_msg}\n\n✅ Health: OK", parse_mode="Markdown")
        except Exception:
            pass


# Estados para el wizard de nuevo proyecto
NEW_PROJECT_STATES = {
    "waiting_path": {},  # user_id -> {"step": "path"}
}

async def cmd_new(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Wizard para crear nuevo proyecto:
    1. Pregunta ruta del proyecto
    2. Crea sesión
    3. Establece modelo por defecto
    4. Marca como nuevo workspace
    """
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Acceso denegado.")
        return
    
    user_id = update.effective_user.id
    
    # Guardar estado para el wizard
    NEW_PROJECT_STATES["waiting_path"][user_id] = {"step": "path"}
    
    keyboard = [
        [InlineKeyboardButton("❌ Cancelar", callback_data="new_project_cancel")],
    ]
    
    await update.message.reply_text(
        "🆕 *Crear Nuevo Proyecto*\n\n"
        "Envíame la *ruta completa* del proyecto:\n"
        "Ejemplo: `/home/valle/Documentos/proyectos/mi-proyecto`\n\n"
        "El proyecto se creará si no existe.",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def handle_new_project_path(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Maneja la ruta enviada por el usuario en el wizard"""
    if not update.message or not update.message.text:
        return
    
    user_id = update.effective_user.id
    if user_id not in NEW_PROJECT_STATES.get("waiting_path", {}):
        return
    
    path = update.message.text.strip()
    
    # Validar ruta
    if not path.startswith("/"):
        await update.message.reply_text("❌ La ruta debe ser absoluta (empezar con /)")
        return
    
    # Limpiar estado
    if user_id in NEW_PROJECT_STATES["waiting_path"]:
        del NEW_PROJECT_STATES["waiting_path"][user_id]
    
    client = get_client()
    msg = await update.message.reply_text("🔧 Configurando proyecto...")
    
    try:
        # 1. Crear nueva sesión
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        session = await asyncio.wait_for(
            client.create_session(title=f"project_{timestamp}"),
            timeout=15
        )
        session_id = session.get("id") or session.get("sessionID")
        
        if not session_id:
            await msg.edit_text("❌ Error: no se obtuvo ID de sesión")
            return
        
        # 2. Guardar configuración
        config = load_config()
        config["session_id"] = session_id
        config["workspace"] = path
        
        # Mantener modelo actual o usar por defecto
        if "model" not in config:
            config["model"] = "alibaba-coding-plan/qwen3.5-plus"
        
        save_config(config)
        
        # 3. Confirmar
        await msg.edit_text(
            f"✅ *Proyecto configurado*\n\n"
            f"📁 Workspace: `{path}`\n"
            f"🆔 Sesión: `{session_id[:20]}...`\n"
            f"🧠 Modelo: `{config['model']}`\n\n"
            f"Ahora puedes enviar tu primera tarea.",
            parse_mode="Markdown",
        )
        
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


async def cmd_workspace(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Lista proyectos disponibles y permite cambiar el workspace"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Acceso denegado.")
        return
    
    projects_dir = Path("/home/valle/Documentos/proyectos")
    
    try:
        projects = [
            p for p in projects_dir.iterdir()
            if p.is_dir() and not p.name.startswith(".")
        ]
        projects.sort(key=lambda x: x.name.lower())
    except Exception as e:
        await update.message.reply_text(f"❌ Error listando proyectos: {e}")
        return
    
    current_workspace = get_workspace()
    text = f"📁 *Workspaces Disponibles*\n\n"
    text += f"*Actual:* `{current_workspace.name}`\n"
    text += f"*Ruta:* `{current_workspace}`\n"
    text += f"*Total:* {len(projects)} proyectos\n\n"
    text += "Selecciona uno para cambiar:"
    
    keyboard = []
    for proj in projects[:20]:
        is_current = proj == current_workspace
        mark = "✅ " if is_current else ""
        keyboard.append([
            InlineKeyboardButton(
                f"{mark}{proj.name}",
                callback_data=f"workspace_set:{proj}"
            )
        ])
    
    if len(projects) > 20:
        text += f"\n\n(... y {len(projects) - 20} más)"
    
    keyboard.append([
        InlineKeyboardButton("🔄 Recargar", callback_data="workspace_refresh"),
        InlineKeyboardButton("❌ Cancelar", callback_data="workspace_cancel"),
    ])
    
    await update.message.reply_text(
        text,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def cmd_proyectos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Lista proyectos abiertos/activos.
    Solo muestra nombre del folder, no ID ni path completo.
    """
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Acceso denegado.")
        return
    
    manager = get_manager()
    projects = manager.list_projects()
    
    if not projects:
        await update.message.reply_text(
            "📭 *No hay proyectos abiertos*\n\n"
            "Usa `/open` para explorar y abrir un proyecto.",
            parse_mode="Markdown",
        )
        return
    
    text = "📁 *Proyectos*\n\n"
    
    keyboard = []
    for proj in projects[:10]:
        is_active = proj.project_id == manager.active_project_id
        mark = "✅ " if is_active else ""
        sessions_count = len(proj.sessions)
        
        keyboard.append([
            InlineKeyboardButton(
                f"{mark}{proj.name} ({sessions_count})",
                callback_data=f"project_select:{proj.project_id}"
            )
        ])
    
    keyboard.append([
        InlineKeyboardButton("🆕 Abrir", callback_data=f"explorer:{DEFAULT_WORKSPACE}"),
    ])
    
    await update.message.reply_text(
        text,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def cmd_open(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Explorador de archivos para crear/abrir proyectos.
    """
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Acceso denegado.")
        return
    
    start_path = context.user_data.get("explorer_path", str(DEFAULT_WORKSPACE))
    
    await _show_file_explorer(context.bot, update.message.chat_id, start_path, None)


async def _show_file_explorer(bot: Bot, chat_id: int, current_path: str, message_id: Optional[int]):
    """
    Muestra el explorador de archivos.
    """
    manager = get_manager()
    current_dir = Path(current_path)
    
    if not current_dir.exists():
        current_dir = DEFAULT_WORKSPACE
    
    try:
        dirs = sorted([
            d for d in current_dir.iterdir()
            if d.is_dir() and not d.name.startswith(".")
        ], key=lambda x: x.name.lower())
    except PermissionError:
        text = "❌ No tienes permiso para acceder a este directorio"
        keyboard = [[InlineKeyboardButton("← Volver", callback_data=f"explorer:{current_dir.parent}")]]
        if message_id:
            await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text, reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await bot.send_message(chat_id, text, reply_markup=InlineKeyboardMarkup(keyboard))
        return
    except Exception as e:
        text = f"❌ Error: {e}"
        keyboard = [[InlineKeyboardButton("← Volver", callback_data=f"explorer:{current_dir.parent}")]]
        if message_id:
            await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text, reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await bot.send_message(chat_id, text, reply_markup=InlineKeyboardMarkup(keyboard))
        return
    
    text = f"📁 *Explorador*\n\n"
    text += f"📍 `{current_dir}`\n"
    
    existing_project = None
    for proj in manager.list_projects():
        if proj.workspace == str(current_dir):
            existing_project = proj
            text += f"\n✅ *Proyecto existente:* {proj.name}\n"
            text += f"   Modelo: `{proj.model}`\n"
            text += f"   Sesiones: {len(proj.sessions)}\n"
            break
    
    keyboard = []
    
    for d in dirs[:12]:
        keyboard.append([
            InlineKeyboardButton(f"📂 {d.name}", callback_data=f"explorer:{d}")
        ])
    
    if len(dirs) > 12:
        text += f"\n... y {len(dirs) - 12} más"
    
    keyboard.append([
        InlineKeyboardButton("📂 Crear folder", callback_data=f"explorer_create:{current_dir}"),
    ])
    
    action_buttons = []
    if existing_project:
        action_buttons.append(InlineKeyboardButton("✅ Abrir proyecto", callback_data=f"project_open:{existing_project.project_id}"))
    else:
        action_buttons.append(InlineKeyboardButton("🆕 Crear proyecto aquí", callback_data=f"project_create_here:{current_dir}"))
    action_buttons.append(InlineKeyboardButton("❌ Cancelar", callback_data="explorer_cancel"))
    keyboard.append(action_buttons)
    
    if current_dir != current_dir.parent:
        keyboard.append([
            InlineKeyboardButton("← Atrás", callback_data=f"explorer:{current_dir.parent}")
        ])
    
    if message_id:
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await bot.send_message(chat_id, text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))


async def cmd_close(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Cierra el proyecto activo.
    """
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Acceso denegado.")
        return
    
    manager = get_manager()
    context = manager.get_current_context()
    
    if not context:
        await update.message.reply_text("ℹ️ No hay proyecto activo")
        return
    
    project_id = context["project_id"]
    project_name = context["project_name"]
    
    manager.close_project(project_id)
    
    await update.message.reply_text(
        f"✅ *Proyecto cerrado*\n\n"
        f"📁 {project_name}\n"
        f"📝 Sesión: `{context['session_id'][:15] if context['session_id'] else 'N/A'}...`\n\n"
        f"Usa `/proyectos` para abrir otro.",
        parse_mode="Markdown",
    )


async def cmd_esc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancela la tarea en curso usando abort_session() de OpenCode"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Acceso denegado.")
        return
    
    manager = get_manager()
    context_info = manager.get_current_context()
    
    if not context_info:
        await update.message.reply_text("⚠️ No hay proyecto/sesión activa")
        return
    
    session_id = context_info.get("session_id")
    if not session_id:
        await update.message.reply_text("⚠️ No hay sesión activa")
        return
    
    client = get_client()
    
    # Check si la sesión está busy usando OpenCode API
    is_busy = await client.is_session_busy(session_id)
    
    if not is_busy:
        await update.message.reply_text("ℹ️ La sesión no está procesando")
        return
    
    # Abortar usando OpenCode API
    aborted = await client.abort_session(session_id)
    
    # Limpiar estado local
    task = _active_tasks.get(session_id)
    if task:
        task.is_cancelled = True
        
        try:
            await context.bot.edit_message_text(
                chat_id=task.chat_id,
                message_id=task.msg_id,
                text=f"❌ Cancelado\n⏱️ {(datetime.now() - task.start_time).total_seconds():.0f}s",
            )
        except Exception:
            pass
        
        del _active_tasks[session_id]
    
    if aborted:
        logger.info(f"Sesión abortada: {session_id[:20]}")
        await update.message.reply_text("✅ Tarea cancelada")
    else:
        await update.message.reply_text("⚠️ No se pudo cancelar (ya terminó)")


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Muestra el estado de la sesión usando OpenCode API"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Acceso denegado.")
        return
    
    manager = get_manager()
    context_info = manager.get_current_context()
    
    if not context_info:
        await update.message.reply_text("⚠️ No hay proyecto activo. Usa `/open`")
        return
    
    session_id = context_info.get("session_id")
    if not session_id:
        await update.message.reply_text("⚠️ No hay sesión activa")
        return
    
    client = get_client()
    
    # Obtener estado desde OpenCode
    status = await client.get_session_status(session_id)
    is_busy = status.get("type") == "busy"
    
    task = _active_tasks.get(session_id)
    
    if not is_busy and not task:
        await update.message.reply_text("✅ Sesión idle (sin tareas)")
        return
    
    elapsed = 0
    if task:
        elapsed = (datetime.now() - task.start_time).total_seconds()
    
    mins = int(elapsed // 60)
    
    status_text = f"📊 *Estado de sesión*\n\n"
    status_text += f"📁 Proyecto: {context_info.get('project_name', 'N/A')}\n"
    status_text += f"🆔 `{session_id[:15]}...`\n"
    status_text += f"⚡ Status: {'busy' if is_busy else 'idle'}\n"
    
    if task:
        status_text += f"⏱️ Duración: {mins} min\n"
        status_text += f"📊 Mensajes: {task.message_count}\n"
        status_text += f"📈 Context: {task.context_percent:.0f}%\n"
        
        if task.files_modified:
            files = ", ".join(list(task.files_modified)[:5])
            status_text += f"📁 Archivos: {files}\n"
        
        if task.tools_used:
            tools = ", ".join(f"{k}" for k, v in list(task.tools_used.items())[:3])
            status_text += f"🔧 Tools: {tools}\n"
        
        if task.last_thought:
            status_text += f"\n💬 {task.last_thought[:100]}"
    
    await update.message.reply_text(status_text, parse_mode="Markdown")


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
    
    manager = get_manager()
    
    if context.user_data.get("explorer_create_path"):
        parent_path = context.user_data.pop("explorer_create_path")
        folder_name = text.strip()
        
        try:
            new_folder = Path(parent_path) / folder_name
            new_folder.mkdir(parents=True, exist_ok=True)
            await update.message.reply_text(
                f"✅ Folder creado: `{folder_name}`\n"
                f"📍 `{new_folder}`",
                parse_mode="Markdown",
            )
            await _show_file_explorer(context.bot, update.message.chat_id, str(new_folder), None)
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {e}")
        return
    
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
    
    user_id = update.effective_user.id
    if user_id in NEW_PROJECT_STATES.get("waiting_path", {}):
        await handle_new_project_path(update, context)
        return
    
    if context.user_data.get("waiting_custom_command"):
        context.user_data["waiting_custom_command"] = False
        logger.info(f"Comando personalizado: {text[:80]}")
        await _process_message(context.bot, update.message.chat_id, text)
        return

    chat_id = update.message.chat_id
    logger.info(f"Mensaje: {text[:80]}")
    
    reply_context = None
    if update.message.reply_to_message:
        reply_to_id = update.message.reply_to_message.message_id
        reply_context = manager.lookup_reply(reply_to_id)
        
        if reply_context:
            project_id, session_id, project_name = reply_context
            logger.info(f"Reply detected: msg {reply_to_id} -> project {project_id}/{session_id[:15]}")
            
            manager.set_active_project(project_id)
            manager.set_active_session(project_id, session_id)
            
            config = load_config()
            config["session_id"] = session_id
            project = manager.get_project(project_id)
            if project:
                config["workspace"] = project.workspace
                config["model"] = project.model
            save_config(config)
            
            await update.message.reply_text(
                f"📎 *Reply a:* {project_name}\n"
                f"🆔 `{session_id[:15]}...`",
                parse_mode="Markdown",
            )

    config = load_config()
    session_id = config.get("session_id")
    
    # Verificar si sesión está busy usando OpenCode API
    if session_id:
        client = get_client()
        is_busy = await client.is_session_busy(session_id)
        
        if is_busy:
            task = _active_tasks.get(session_id)
            elapsed = 0
            if task:
                elapsed = int((datetime.now() - task.start_time).total_seconds())
            mins = elapsed // 60
            
            await update.message.reply_text(
                f"⏳ Sesión ocupada ({mins} min)\n"
                f"Usa `/esc` para cancelar o espera."
            )
            return

    await _process_message(context.bot, chat_id, text)


async def _process_message(bot: Bot, chat_id: int, text: str):
    client = get_client()
    config = load_config()
    manager = get_manager()

    server = get_server()
    if not server.is_running:
        await bot.send_message(chat_id, "⚠️ Server no está corriendo. Usa /start")
        return

    session_id = config.get("session_id")
    if not session_id:
        await bot.send_message(chat_id, "⚠️ Sin sesión activa. Usa `/open` para abrir un proyecto.")
        return
    
    project_id = manager.active_project_id
    if not project_id:
        await bot.send_message(chat_id, "⚠️ No hay proyecto activo. Usa `/open` para abrir uno.")
        return
    
    project = manager.get_project(project_id)
    if not project:
        await bot.send_message(chat_id, "⚠️ Proyecto no encontrado. Usa `/open` para abrir uno.")
        return
    
    project_name = project.name
    session_info = project.sessions.get(session_id)
    session_title = session_info.title if session_info else ""
    model_id = project.model

    task_state = TaskState(
        session_id, 
        chat_id, 
        0, 
        text,
        project_name=project_name,
        session_title=session_title
    )
    _active_tasks[session_id] = task_state

    status_msg = await bot.send_message(
        chat_id=chat_id,
        text=f"⏳ {project_name} | {session_title[:15] if session_title else session_id[:12]}",
    )
    task_state.msg_id = status_msg.message_id
    
    manager.register_bot_message(status_msg.message_id, project_id, session_id)

    heartbeat = Heartbeat(bot, chat_id, session_id, task_state, model_id=model_id)
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
                        
                        elif part_type == "step-finish":
                            tokens = part.get("tokens", {})
                            if tokens:
                                await heartbeat.update_state(tokens=tokens)
                        
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
    post_tokens = None

    if post_result:
        parts = post_result.get("parts", [])
        texts = [p.get("text", "") for p in parts if p.get("type") == "text" and p.get("text", "").strip()]
        final_text = "".join(texts).strip()
        # Extraer info de tokens si existe
        for p in parts:
            if p.get("type") == "step-finish":
                post_tokens = p.get("tokens", {})

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
        # No hay texto pero quizás opencode hizo algo (tokens, files, etc.)
        logger.warning("Sin respuesta de texto del asistente")
        
        # Construir mensaje informativo
        info_msg = "⚠️ *Opencode procesó tu solicitud*\n\n"
        
        if post_tokens:
            total = post_tokens.get("total", 0)
            output = post_tokens.get("output", 0)
            if total > 0:
                info_msg += f"📊 Tokens usados: {total} (output: {output})\n"
        
        if task_state.files_modified:
            files = ", ".join(list(task_state.files_modified)[:5])
            info_msg += f"📁 Archivos modificados: {files}\n"
        
        if task_state.tools_used:
            tools = ", ".join(f"{k}:{v}" for k, v in task_state.tools_used.items())
            info_msg += f"🔧 Tools: {tools}\n"
        
        # Si no hay info adicional, dar contexto
        if len(info_msg.split("\n")) <= 2:
            info_msg += "\nEl asistente no generó una respuesta de texto.\n"
            info_msg += "Esto puede pasar cuando:\n"
            info_msg += "• La tarea requiere más contexto\n"
            info_msg += "• Opencode está esperando confirmación\n"
            info_msg += "• La respuesta está en archivos modificados\n\n"
            info_msg += "Probá con:\n"
            info_msg += "• Más detalles en tu pregunta\n"
            info_msg += "• `/status` para ver el estado\n"
        
        await bot.send_message(chat_id, info_msg, parse_mode="Markdown")


async def _send_long_message(bot: Bot, chat_id: int, text: str, parse_mode: str = "Markdown", project_id: str = None, session_id: str = None):
    max_len = 4096
    manager = get_manager()
    
    def sanitize_markdown(t: str) -> str:
        backtick_count = t.count('`')
        if backtick_count % 2 != 0:
            t = t.replace('`', "'")
        return t
    
    text = sanitize_markdown(text)
    
    if len(text) <= max_len:
        try:
            msg = await bot.send_message(chat_id, text, parse_mode=parse_mode)
            if project_id and session_id:
                manager.register_bot_message(msg.message_id, project_id, session_id)
            return msg
        except BadRequest:
            text_clean = text.replace('*', '').replace('_', '').replace('`', "'").replace('[', '(').replace(']', ')')
            msg = await bot.send_message(chat_id, text_clean)
            if project_id and session_id:
                manager.register_bot_message(msg.message_id, project_id, session_id)
            return msg

    last_msg = None
    for i in range(0, len(text), max_len):
        chunk = text[i:i + max_len]
        chunk = sanitize_markdown(chunk)
        try:
            msg = await bot.send_message(chat_id, chunk, parse_mode=parse_mode)
            if project_id and session_id:
                manager.register_bot_message(msg.message_id, project_id, session_id)
            last_msg = msg
        except BadRequest:
            chunk_clean = chunk.replace('*', '').replace('_', '').replace('`', "'").replace('[', '(').replace(']', ')')
            msg = await bot.send_message(chat_id, chunk_clean)
            if project_id and session_id:
                manager.register_bot_message(msg.message_id, project_id, session_id)
            last_msg = msg
    
    return last_msg


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

    if data.startswith("workspace_set:"):
        workspace_path = data.split(":", 1)[1]
        new_workspace = Path(workspace_path)
        
        if not new_workspace.exists():
            await query.edit_message_text(f"❌ El directorio no existe: `{new_workspace}`", parse_mode="Markdown")
            return
        
        # Guardar workspace en config.json (sobrevive a reinicios)
        set_workspace(new_workspace)
        
        # Reiniciar server con nuevo workspace
        server = get_server()
        client = get_client()
        msg = await query.message.reply_text(f"🔄 Reiniciando server con nuevo workspace...")
        
        # Terminar server actual
        if server.process:
            server.process.terminate()
            await asyncio.sleep(2)
        
        # Iniciar con nuevo workspace
        await server.start(new_workspace)
        
        # Crear nueva sesión para este workspace
        try:
            config = load_config()
            model = config.get("model")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            session = await asyncio.wait_for(
                client.create_session(title=f"session_{timestamp}", model=model),
                timeout=15
            )
            session_id = session.get("id") or session.get("sessionID")
            
            config["session_id"] = session_id
            save_config(config)
            
            logger.info(f"✅ Nueva sesión creada para workspace: {session_id[:20]} (modelo: {model})")
        except Exception as e:
            logger.error(f"Error creando sesión: {e}")
            session_id = None
        
        await msg.edit_text(
            f"✅ Workspace cambiado\n"
            f"📁 `{new_workspace.name}`\n"
            f"🆕 Sesión: `{session_id[:15] if session_id else 'N/A'}...`",
            parse_mode="Markdown",
        )
        return

    if data == "workspace_refresh":
        await cmd_workspace(update, context)
        return

    if data == "workspace_cancel":
        await bot.delete_message(chat_id, query.message.message_id)
        return
    
    if data == "new_project_cancel":
        user_id = query.from_user.id
        if user_id in NEW_PROJECT_STATES.get("waiting_path", {}):
            del NEW_PROJECT_STATES["waiting_path"][user_id]
        await bot.delete_message(chat_id, query.message.message_id)
        return
    
    if data.startswith("explorer:"):
        path = data.split(":", 1)[1]
        await _show_file_explorer(bot, chat_id, path, query.message.message_id)
        return
    
    if data == "explorer_cancel":
        await bot.delete_message(chat_id, query.message.message_id)
        return
    
    if data.startswith("explorer_create:"):
        parent_path = data.split(":", 1)[1]
        context.user_data["explorer_create_path"] = parent_path
        await query.edit_message_text(
            f"📂 *Crear nuevo folder*\n\n"
            f"📍 `{parent_path}`\n\n"
            f"Envía el nombre del nuevo folder:",
            parse_mode="Markdown",
        )
        return
    
    if data.startswith("project_create_here:"):
        workspace_path = data.split(":", 1)[1]
        manager = get_manager()
        
        ws_dir = Path(workspace_path)
        project_name = ws_dir.name
        
        config = load_config()
        model = config.get("model", "alibaba-coding-plan/qwen3.5-plus")
        
        project = manager.create_project(
            workspace=workspace_path,
            name=project_name,
            model=model,
        )
        
        client = get_client()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        try:
            session = await asyncio.wait_for(
                client.create_session(title=f"{project_name}_{timestamp}"),
                timeout=15
            )
            session_id = session.get("id") or session.get("sessionID")
            
            if session_id:
                manager.add_session_to_project(project.project_id, session_id, f"{project_name}_{timestamp}")
                manager.set_active_project(project.project_id)
                manager.set_active_session(project.project_id, session_id)
                
                config["session_id"] = session_id
                config["workspace"] = workspace_path
                config["model"] = model
                save_config(config)
                
                await query.edit_message_text(
                    f"✅ *Proyecto creado*\n\n"
                    f"📁 `{project_name}`\n"
                    f"📍 `{workspace_path}`\n"
                    f"🧠 `{model}`\n"
                    f"🆔 `{session_id[:20]}...`\n\n"
                    f"Ahora puedes enviar tareas.",
                    parse_mode="Markdown",
                )
            else:
                await query.edit_message_text("❌ Error: no se obtuvo ID de sesión")
        except asyncio.TimeoutError:
            await query.edit_message_text("❌ Timeout creando sesión")
        except Exception as e:
            await query.edit_message_text(f"❌ Error: {e}")
        return
    
    if data.startswith("project_open:"):
        project_id = data.split(":", 1)[1]
        manager = get_manager()
        project = manager.get_project(project_id)
        
        if not project:
            await query.edit_message_text("❌ Proyecto no encontrado")
            return
        
        manager.set_active_project(project_id)
        
        if project.active_session_id:
            config = load_config()
            config["session_id"] = project.active_session_id
            config["workspace"] = project.workspace
            config["model"] = project.model
            save_config(config)
            
            await query.edit_message_text(
                f"✅ *Proyecto abierto*\n\n"
                f"📁 `{project.name}`\n"
                f"📍 `{project.workspace}`\n"
                f"🧠 `{project.model}`\n"
                f"🆔 `{project.active_session_id[:20]}...`\n\n"
                f"Sesiones: {len(project.sessions)}",
                parse_mode="Markdown",
            )
        else:
            client = get_client()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            try:
                session = await asyncio.wait_for(
                    client.create_session(title=f"{project.name}_{timestamp}"),
                    timeout=15
                )
                session_id = session.get("id") or session.get("sessionID")
                
                if session_id:
                    manager.add_session_to_project(project_id, session_id, f"{project.name}_{timestamp}")
                    
                    config = load_config()
                    config["session_id"] = session_id
                    config["workspace"] = project.workspace
                    config["model"] = project.model
                    save_config(config)
                    
                    await query.edit_message_text(
                        f"✅ *Proyecto abierto*\n\n"
                        f"📁 `{project.name}`\n"
                        f"📍 `{project.workspace}`\n"
                        f"🧠 `{project.model}`\n"
                        f"🆔 `{session_id[:20]}...`\n\n"
                        f"(Nueva sesión creada)",
                        parse_mode="Markdown",
                    )
            except Exception as e:
                await query.edit_message_text(f"❌ Error creando sesión: {e}")
        return
    
    if data.startswith("project_delete:"):
        project_id = data.split(":", 1)[1]
        manager = get_manager()
        
        if manager.delete_project(project_id):
            await query.edit_message_text(f"✅ Proyecto eliminado")
        else:
            await query.edit_message_text("❌ Proyecto no encontrado")
        return
    
    if data.startswith("project_sessions:"):
        project_id = data.split(":", 1)[1]
        manager = get_manager()
        project = manager.get_project(project_id)
        
        if not project:
            await query.edit_message_text("❌ Proyecto no encontrado")
            return
        
        sessions = manager.list_project_sessions(project_id)
        
        text = f"📋 *Sesiones de {project.name}*\n\n"
        text += f"Total: {len(sessions)}\n\n"
        
        keyboard = []
        for session in sessions[:10]:
            is_active = session.is_active
            mark = "✅ " if is_active else ""
            title_short = session.title[:25] if len(session.title) > 25 else session.title
            
            keyboard.append([
                InlineKeyboardButton(
                    f"{mark}{title_short}",
                    callback_data=f"project_session:{project_id}:{session.session_id}"
                ),
                InlineKeyboardButton(
                    "🗑️",
                    callback_data=f"project_session_delete:{project_id}:{session.session_id}"
                ),
            ])
        
        keyboard.append([
            InlineKeyboardButton("🆕 Nueva sesión", callback_data=f"project_session_new:{project_id}"),
        ])
        keyboard.append([
            InlineKeyboardButton("← Volver", callback_data="projects_menu"),
        ])
        
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))
        return
    
    if data.startswith("project_session:"):
        parts = data.split(":")
        if len(parts) < 3:
            return
        project_id = parts[1]
        session_id = parts[2]
        
        manager = get_manager()
        project = manager.get_project(project_id)
        session = project.sessions.get(session_id) if project else None
        
        if not project or not session:
            await query.edit_message_text("❌ Sesión no encontrada")
            return
        
        text = f"📋 *Sesión*\n\n"
        text += f"*Proyecto:* {project.name}\n"
        text += f"*Título:* {session.title}\n"
        text += f"*ID:* `{session_id[:20]}...`\n"
        text += f"*Estado:* {'✅ Activa' if session.is_active else '⚪ Inactiva'}\n"
        text += f"*Mensajes:* {len(session.message_ids)} registrados\n"
        
        keyboard = []
        if not session.is_active:
            keyboard.append([
                InlineKeyboardButton("✅ Activar", callback_data=f"project_session_activate:{project_id}:{session_id}"),
            ])
        keyboard.append([
            InlineKeyboardButton("🗑️ Borrar", callback_data=f"project_session_delete:{project_id}:{session_id}"),
        ])
        keyboard.append([
            InlineKeyboardButton("← Volver", callback_data=f"project_sessions:{project_id}"),
        ])
        
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))
        return
    
    if data.startswith("project_session_activate:"):
        parts = data.split(":")
        if len(parts) < 3:
            return
        project_id = parts[1]
        session_id = parts[2]
        
        manager = get_manager()
        
        if manager.set_active_session(project_id, session_id):
            config = load_config()
            config["session_id"] = session_id
            project = manager.get_project(project_id)
            if project:
                config["workspace"] = project.workspace
                config["model"] = project.model
            save_config(config)
            
            await query.edit_message_text(
                f"✅ Sesión activada\n\n"
                f"🆔 `{session_id[:20]}...`",
                parse_mode="Markdown",
            )
        else:
            await query.edit_message_text("❌ Error activando sesión")
        return
    
    if data.startswith("project_session_delete:"):
        parts = data.split(":")
        if len(parts) < 3:
            return
        project_id = parts[1]
        session_id = parts[2]
        
        manager = get_manager()
        client = get_client()
        
        try:
            await client.delete_session(session_id)
        except Exception as e:
            logger.warning(f"Error deleting session from opencode: {e}")
        
        manager.delete_session(project_id, session_id)
        
        project = manager.get_project(project_id)
        if project and project.active_session_id:
            config = load_config()
            config["session_id"] = project.active_session_id
            save_config(config)
        elif project and not project.active_session_id:
            config = load_config()
            config.pop("session_id", None)
            save_config(config)
        
        sessions = manager.list_project_sessions(project_id)
        text = f"📋 *Sesiones de {project.name if project else 'Proyecto'}*\n\n"
        text += f"Total: {len(sessions)}\n"
        text += f"✅ Sesión eliminada: `{session_id[:15]}...`\n\n"
        
        keyboard = []
        for session in sessions[:10]:
            is_active = session.is_active
            mark = "✅ " if is_active else ""
            title_short = session.title[:25] if len(session.title) > 25 else session.title
            
            keyboard.append([
                InlineKeyboardButton(
                    f"{mark}{title_short}",
                    callback_data=f"project_session:{project_id}:{session.session_id}"
                ),
                InlineKeyboardButton(
                    "🗑️",
                    callback_data=f"project_session_delete:{project_id}:{session.session_id}"
                ),
            ])
        
        keyboard.append([
            InlineKeyboardButton("🆕 Nueva sesión", callback_data=f"project_session_new:{project_id}"),
        ])
        keyboard.append([
            InlineKeyboardButton("← Volver", callback_data="projects_menu"),
        ])
        
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))
        return
    
    if data.startswith("project_session_new:"):
        project_id = data.split(":", 1)[1]
        manager = get_manager()
        project = manager.get_project(project_id)
        
        if not project:
            await query.edit_message_text("❌ Proyecto no encontrado")
            return
        
        client = get_client()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        try:
            session = await asyncio.wait_for(
                client.create_session(title=f"{project.name}_{timestamp}"),
                timeout=15
            )
            session_id = session.get("id") or session.get("sessionID")
            
            if session_id:
                manager.add_session_to_project(project_id, session_id, f"{project.name}_{timestamp}")
                manager.set_active_session(project_id, session_id)
                
                config = load_config()
                config["session_id"] = session_id
                save_config(config)
                
                await query.edit_message_text(
                    f"✅ Nueva sesión creada\n\n"
                    f"🆔 `{session_id[:20]}...`",
                    parse_mode="Markdown",
                )
            else:
                await query.edit_message_text("❌ Error: no se obtuvo ID de sesión")
        except Exception as e:
            await query.edit_message_text(f"❌ Error: {e}")
        return
    
    if data == "projects_menu":
        manager = get_manager()
        projects = manager.list_projects()
        
        text = "📁 *Proyectos*\n\n"
        
        keyboard = []
        for proj in projects[:10]:
            is_active = proj.project_id == manager.active_project_id
            mark = "✅ " if is_active else ""
            sessions_count = len(proj.sessions)
            
            keyboard.append([
                InlineKeyboardButton(
                    f"{mark}{proj.name} ({sessions_count})",
                    callback_data=f"project_select:{proj.project_id}"
                )
            ])
        
        keyboard.append([
            InlineKeyboardButton("🆕 Explorar", callback_data=f"explorer:{DEFAULT_WORKSPACE}"),
        ])
        
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))
        return
    
    if data.startswith("project_select:"):
        project_id = data.split(":", 1)[1]
        manager = get_manager()
        project = manager.get_project(project_id)
        
        if not project:
            await query.edit_message_text("❌ Proyecto no encontrado")
            return
        
        text = f"📁 *{project.name}*\n"
        text += f"📋 Sesiones: {len(project.sessions)}\n"
        
        keyboard = [
            [InlineKeyboardButton("✅ Abrir", callback_data=f"project_open:{project_id}")],
            [InlineKeyboardButton("📋 Sesiones", callback_data=f"project_sessions:{project_id}")],
            [InlineKeyboardButton("🗑️ Eliminar", callback_data=f"project_delete:{project_id}")],
            [InlineKeyboardButton("← Volver", callback_data="projects_menu")],
        ]
        
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))
        return
    
    if data == "start_menu":
        await cmd_start(update, context)
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
    manager = get_manager()
    
    await ensure_server_running()
    logger.info(f"🚀 Bot server iniciado en puerto {BOT_PORT}")
    
    workspace = get_workspace()
    
    logger.info(f"🚀 Iniciando OpenCode server en puerto {OPENCODE_PORT} (workspace: {workspace.name})...")
    if not await server.start(workspace):
        logger.error(f"❌ No se pudo iniciar server en puerto {OPENCODE_PORT}")
        return

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
            logger.warning(f"⚠️ Sesión inválida tras 3 intentos: {session_id[:20]}, se creará una nueva en /proyectos")
    
    logger.info(f"🔍 Check restart_pending: {config.get('restart_pending')}")
    restart_info = config.get("restart_pending")
    if restart_info:
        logger.info(f"📩 Restart pendiente detectado! chat_id={restart_info.get('chat_id')}, msg_id={restart_info.get('message_id')}")
        try:
            chat_id = restart_info["chat_id"]
            msg_id = restart_info["message_id"]
            
            model = config.get("model") or "automático"
            workspace = config.get("workspace")
            workspace_name = Path(workspace).name if workspace else BOT_DIR.name
            session_id = config.get("session_id")
            
            logger.info(f"📝 Info: model={model}, workspace={workspace_name}, session={session_id[:20] if session_id else None}")
            
            status_lines = [
                "✅ *Bot Reiniciado*",
                "━━━━━━━━━━━━━━━━",
                f"🧠 Modelo: `{model}`",
                f"📁 Workspace: `{workspace_name}`",
                f"📝 Sesión: `{session_id[:20] if session_id else 'Ninguna'}`.",
                f"🌐 Bot Server: `{BOT_PORT}`",
                "━━━━━━━━━━━━━━━━",
            ]
            
            logger.info(f"✏️ Editando mensaje {msg_id} en chat {chat_id}...")
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg_id,
                text="\n".join(status_lines),
                parse_mode="Markdown",
            )
            logger.info(f"✅ Mensaje editado exitosamente!")
        except Exception as e:
            logger.error(f"❌ Error notificando restart: {e}", exc_info=True)
        config.pop("restart_pending", None)
        save_config(config)
        logger.info(f"🗑️ restart_pending eliminado del config")
    
    commands = [
        BotCommand("start", "🤖 Estado del bot y menú principal"),
        BotCommand("status", "📊 Ver estado de tarea en curso"),
        BotCommand("esc", "❌ Cancelar tarea en curso"),
        BotCommand("restart", "🔄 Reiniciar el bot"),
        BotCommand("open", "📁 Explorar y abrir proyecto"),
        BotCommand("projects", "📋 Ver proyectos abiertos"),
        BotCommand("close", "🔒 Cerrar proyecto activo"),
        BotCommand("sessions", "📋 Gestión de sesiones"),
        BotCommand("models", "🧠 Seleccionar modelo"),
        BotCommand("rename", "✏️ Renombrar sesión actual"),
        BotCommand("web", "🌐 Abrir interfaz web de OpenCode"),
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
    logger.info(f"🚀 OpencodeAgent Bot | Admin: {ADMIN_ID}")
    config = load_config()
    server = get_server()
    client = get_client()
    manager = get_manager()

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
    application.add_handler(CommandHandler("open", cmd_open))
    application.add_handler(CommandHandler("projects", cmd_proyectos))
    application.add_handler(CommandHandler("close", cmd_close))
    application.add_handler(CommandHandler("sessions", cmd_sessions))
    application.add_handler(CommandHandler("rename", cmd_rename))
    application.add_handler(CommandHandler("models", cmd_models))
    application.add_handler(CommandHandler("web", cmd_web))
    application.add_handler(CommandHandler("workspace", cmd_workspace))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_handler(CallbackQueryHandler(handle_callback))
    application.add_error_handler(error_handler)

    
    logger.info("✅ Bot iniciado. Esperando mensajes...")
    application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

    

if __name__ == "__main__":
    main()
