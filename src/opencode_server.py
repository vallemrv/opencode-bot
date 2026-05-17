#!/usr/bin/env python3
"""
opencode_server.py — Gestiona el proceso opencode serve
"""

import asyncio
import json
import logging
import os
import subprocess
import socket
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

OPENCODE_PORT = int(os.getenv("OPENCODE_PORT", "4096"))
BOT_DIR = Path(__file__).parent.parent.resolve()
CONFIG_FILE = BOT_DIR / "config.json"
DEFAULT_WORKSPACE = Path.home() / "proyectos" / "config_system_tmp"


def _load_config() -> dict:
    """Carga config.json"""
    try:
        with open(CONFIG_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def _save_config(config: dict):
    """Guarda config.json"""
    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=2)


def get_workspace() -> Path:
    """Obtiene el workspace desde config.json o usa el default"""
    config = _load_config()
    workspace_str = config.get("workspace")
    if workspace_str:
        workspace_path = Path(workspace_str)
        if workspace_path.exists():
            logger.info(f"Workspace cargado desde config: {workspace_path}")
            return workspace_path
    
    logger.info(f"Usando workspace por defecto: {DEFAULT_WORKSPACE}")
    return DEFAULT_WORKSPACE


def set_workspace(path: Path):
    """Guarda el workspace en config.json"""
    config = _load_config()
    config["workspace"] = str(path)
    _save_config(config)
    logger.info(f"Workspace guardado: {path}")


class OpenCodeServer:
    def __init__(self):
        self.process: Optional[subprocess.Popen] = None
        self._start_time: Optional[float] = None

    @property
    def is_running(self) -> bool:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            result = sock.connect_ex(('0.0.0.0', OPENCODE_PORT))
            sock.close()
            return result == 0
        except Exception:
            return False

    async def start(self, workspace: Optional[Path] = None) -> bool:
        if self.is_running:
            logger.info(f"Server ya está corriendo en puerto {OPENCODE_PORT}")
            return True

        work_dir = workspace if workspace else get_workspace()
        logger.info(f"Arrancando opencode serve en puerto {OPENCODE_PORT} (workspace: {work_dir.name})...")
        
        try:
            self.process = subprocess.Popen(
                ["opencode", "serve", "--port", str(OPENCODE_PORT), "--hostname", "127.0.0.1"],
                cwd=str(work_dir),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            self._start_time = time.time()
            
            for _ in range(30):
                await asyncio.sleep(1)
                if self.is_running:
                    logger.info("✅ Server listo")
                    return True
            
            logger.warning("⚠️ Server no respondió en 30s")
            return False
        except Exception as e:
            logger.error(f"Error arrancando server: {e}")
            return False


_server_instance: Optional[OpenCodeServer] = None


def get_server() -> OpenCodeServer:
    global _server_instance
    if _server_instance is None:
        _server_instance = OpenCodeServer()
    return _server_instance