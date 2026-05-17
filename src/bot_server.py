#!/usr/bin/env python3
"""
bot_server.py - Servidor HTTP propio del bot

Puerto configurable desde .env (BOT_PORT) o fallback 13002
Endpoints:
- GET /health - Health check
- GET /status - Estado de proyectos/sesiones
- GET /projects - Lista proyectos
- GET /project/{id} - Info de un proyecto
- POST /project/create - Crear proyecto
- DELETE /project/{id} - Eliminar proyecto
- GET /project/{id}/sessions - Sesiones de un proyecto
"""

import asyncio
import json
import logging
import os
import socket
import subprocess
import time
from pathlib import Path
from typing import Optional

import aiohttp
from aiohttp import web

logger = logging.getLogger(__name__)

BOT_DIR = Path(__file__).parent.parent.resolve()
DEFAULT_BOT_PORT = 13002
DEFAULT_OPENCODE_PORT = 4096

BOT_PORT = int(os.getenv("BOT_PORT", str(DEFAULT_BOT_PORT)))
OPENCODE_PORT = int(os.getenv("OPENCODE_PORT", str(DEFAULT_OPENCODE_PORT)))
DEFAULT_WORKSPACE = Path(os.getenv("DEFAULT_WORKSPACE", "~/.proyectos")).expanduser()


class BotServer:
    """
    Servidor HTTP del bot.
    
    - Comprueba si está up
    - Si no, lo levanta
    - Endpoints para gestión de proyectos/sesiones
    """
    
    def __init__(self, port: int = BOT_PORT):
        self.port = port
        self.app: Optional[web.Application] = None
        self.runner: Optional[web.AppRunner] = None
        self.site: Optional[web.TCPSite] = None
        self._start_time: Optional[float] = None
    
    @property
    def is_running(self) -> bool:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            result = sock.connect_ex(('0.0.0.0', self.port))
            sock.close()
            return result == 0
        except Exception:
            return False
    
    async def start(self) -> bool:
        if self.is_running:
            logger.info(f"Bot server ya está corriendo en puerto {self.port}")
            return True
        
        logger.info(f"Arrancando bot server en puerto {self.port}...")
        
        try:
            self.app = web.Application()
            self._setup_routes()
            
            self.runner = web.AppRunner(self.app)
            await self.runner.setup()
            
            self.site = web.TCPSite(self.runner, '0.0.0.0', self.port)
            await self.site.start()
            
            self._start_time = time.time()
            logger.info(f"✅ Bot server listo en puerto {self.port}")
            return True
        except Exception as e:
            logger.error(f"Error arrancando bot server: {e}")
            return False
    
    async def stop(self):
        if self.site:
            await self.site.stop()
        if self.runner:
            await self.runner.cleanup()
        logger.info(f"Bot server detenido")
    
    def _setup_routes(self):
        self.app.router.add_get('/health', self._handle_health)
        self.app.router.add_get('/status', self._handle_status)
        self.app.router.add_get('/projects', self._handle_projects)
        self.app.router.add_get('/project/{project_id}', self._handle_project_info)
        self.app.router.add_post('/project/create', self._handle_project_create)
        self.app.router.add_delete('/project/{project_id}', self._handle_project_delete)
        self.app.router.add_post('/project/{project_id}/close', self._handle_project_close)
        self.app.router.add_get('/project/{project_id}/sessions', self._handle_project_sessions)
        self.app.router.add_post('/project/{project_id}/session/create', self._handle_session_create)
        self.app.router.add_delete('/project/{project_id}/session/{session_id}', self._handle_session_delete)
        self.app.router.add_post('/project/{project_id}/session/{session_id}/activate', self._handle_session_activate)
    
    async def _handle_health(self, request: web.Request) -> web.Response:
        return web.json_response({
            "status": "ok",
            "port": self.port,
            "uptime": time.time() - self._start_time if self._start_time else 0,
        })
    
    async def _handle_status(self, request: web.Request) -> web.Response:
        from project_manager import get_manager
        
        manager = get_manager()
        context = manager.get_current_context()
        
        return web.json_response({
            "bot_server": {
                "port": self.port,
                "uptime": time.time() - self._start_time if self._start_time else 0,
            },
            "opencode_server": {
                "port": OPENCODE_PORT,
                "status": self._check_opencode_server(),
            },
            "current_context": context,
            "total_projects": len(manager.projects),
        })
    
    async def _handle_projects(self, request: web.Request) -> web.Response:
        from project_manager import get_manager
        
        manager = get_manager()
        projects = [p.to_dict() for p in manager.list_projects()]
        
        return web.json_response({
            "projects": projects,
            "active_project_id": manager.active_project_id,
        })
    
    async def _handle_project_info(self, request: web.Request) -> web.Response:
        from project_manager import get_manager
        
        project_id = request.match_info['project_id']
        manager = get_manager()
        project = manager.get_project(project_id)
        
        if not project:
            return web.json_response({"error": "Project not found"}, status=404)
        
        return web.json_response(project.to_dict())
    
    async def _handle_project_create(self, request: web.Request) -> web.Response:
        from project_manager import get_manager
        
        try:
            data = await request.json()
        except json.JSONDecodeError:
            return web.json_response({"error": "Invalid JSON"}, status=400)
        
        workspace = data.get('workspace')
        if not workspace:
            return web.json_response({"error": "workspace required"}, status=400)
        
        ws_path = Path(workspace)
        if not ws_path.exists():
            try:
                ws_path.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                return web.json_response({"error": f"Cannot create workspace: {e}"}, status=400)
        
        manager = get_manager()
        project = manager.create_project(
            workspace=workspace,
            name=data.get('name'),
            model=data.get('model'),
        )
        
        manager.set_active_project(project.project_id)
        
        return web.json_response({
            "project": project.to_dict(),
            "is_active": True,
        })
    
    async def _handle_project_delete(self, request: web.Request) -> web.Response:
        from project_manager import get_manager
        
        project_id = request.match_info['project_id']
        manager = get_manager()
        
        if manager.delete_project(project_id):
            return web.json_response({"deleted": True})
        return web.json_response({"error": "Project not found"}, status=404)
    
    async def _handle_project_close(self, request: web.Request) -> web.Response:
        from project_manager import get_manager
        
        project_id = request.match_info['project_id']
        manager = get_manager()
        
        if manager.close_project(project_id):
            return web.json_response({"closed": True})
        return web.json_response({"error": "Project not active"}, status=400)
    
    async def _handle_project_sessions(self, request: web.Request) -> web.Response:
        from project_manager import get_manager
        
        project_id = request.match_info['project_id']
        manager = get_manager()
        sessions = [s.to_dict() for s in manager.list_project_sessions(project_id)]
        
        project = manager.get_project(project_id)
        active_session_id = project.active_session_id if project else None
        
        return web.json_response({
            "sessions": sessions,
            "active_session_id": active_session_id,
        })
    
    async def _handle_session_create(self, request: web.Request) -> web.Response:
        from project_manager import get_manager
        from opencode_client import get_client
        
        project_id = request.match_info['project_id']
        manager = get_manager()
        project = manager.get_project(project_id)
        
        if not project:
            return web.json_response({"error": "Project not found"}, status=404)
        
        try:
            data = await request.json()
        except json.JSONDecodeError:
            data = {}
        
        title = data.get('title', f"session_{int(time.time())}")
        
        client = get_client()
        session = await client.create_session(title=title)
        session_id = session.get('id') or session.get('sessionID')
        
        if not session_id:
            return web.json_response({"error": "Failed to create session"}, status=500)
        
        session_info = manager.add_session_to_project(project_id, session_id, title)
        
        return web.json_response({
            "session": session_info.to_dict() if session_info else None,
            "opencode_session": session,
        })
    
    async def _handle_session_delete(self, request: web.Request) -> web.Response:
        from project_manager import get_manager
        from opencode_client import get_client
        
        project_id = request.match_info['project_id']
        session_id = request.match_info['session_id']
        
        manager = get_manager()
        client = get_client()
        
        await client.delete_session(session_id)
        manager.delete_session(project_id, session_id)
        
        return web.json_response({"deleted": True})
    
    async def _handle_session_activate(self, request: web.Request) -> web.Response:
        from project_manager import get_manager
        
        project_id = request.match_info['project_id']
        session_id = request.match_info['session_id']
        
        manager = get_manager()
        
        if manager.set_active_session(project_id, session_id):
            return web.json_response({"activated": True})
        return web.json_response({"error": "Session not found"}, status=404)
    
    def _check_opencode_server(self) -> str:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            result = sock.connect_ex(('0.0.0.0', OPENCODE_PORT))
            sock.close()
            return "running" if result == 0 else "stopped"
        except Exception:
            return "unknown"


_server_instance: Optional[BotServer] = None


def get_server() -> BotServer:
    global _server_instance
    if _server_instance is None:
        _server_instance = BotServer()
    return _server_instance


async def ensure_server_running() -> bool:
    """
    Comprueba si el bot server está up, si no lo levanta.
    """
    server = get_server()
    if server.is_running:
        return True
    return await server.start()