#!/usr/bin/env python3
"""
project_manager.py - Gestión multi-proyecto y multi-sesión

Estructura:
- Projects: cada proyecto tiene workspace (ruta), modelo, y múltiples sesiones
- Sessions: cada sesión pertenece a un proyecto
- Reply tracking: mapeo message_id -> (project_id, session_id)

Persistencia en projects.json
"""

import json
import logging
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List, Set
from collections import defaultdict

logger = logging.getLogger(__name__)

BOT_DIR = Path(__file__).parent.parent.resolve()
PROJECTS_FILE = BOT_DIR / "projects.json"

DEFAULT_MODEL = "alibaba-coding-plan/qwen3.5-plus"


@dataclass
class SessionInfo:
    session_id: str
    title: str
    created_at: float
    is_active: bool = False
    message_ids: Set[int] = field(default_factory=set)
    
    def to_dict(self) -> dict:
        d = asdict(self)
        d["message_ids"] = list(self.message_ids)
        return d
    
    @classmethod
    def from_dict(cls, d: dict) -> "SessionInfo":
        d["message_ids"] = set(d.get("message_ids", []))
        return cls(**d)


@dataclass
class ProjectInfo:
    project_id: str
    name: str
    workspace: str
    model: str
    sessions: Dict[str, SessionInfo] = field(default_factory=dict)
    active_session_id: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    
    def to_dict(self) -> dict:
        d = asdict(self)
        d["sessions"] = {k: v.to_dict() for k, v in self.sessions.items()}
        return d
    
    @classmethod
    def from_dict(cls, d: dict) -> "ProjectInfo":
        d["sessions"] = {k: SessionInfo.from_dict(v) for k, v in d.get("sessions", {}).items()}
        return cls(**d)


class ReplyTracker:
    """
    Sistema de tracking para replies de Telegram.
    
    Mapea message_id (mensaje del bot) -> (project_id, session_id)
    Esto permite saber a qué proyecto/sesión corresponde una reply.
    """
    
    def __init__(self):
        self._mapping: Dict[int, tuple[str, str]] = {}
        self._max_size = 10000
        self._cleanup_threshold = 8000
    
    def register_message(self, message_id: int, project_id: str, session_id: str):
        self._mapping[message_id] = (project_id, session_id)
        self._cleanup_if_needed()
        logger.debug(f"ReplyTracker: registered msg {message_id} -> {project_id}/{session_id[:15]}")
    
    def lookup(self, message_id: int) -> Optional[tuple[str, str]]:
        return self._mapping.get(message_id)
    
    def unregister(self, message_id: int):
        self._mapping.pop(message_id, None)
    
    def cleanup_old(self, keep_ids: Set[int]):
        self._mapping = {k: v for k, v in self._mapping.items() if k in keep_ids}
    
    def _cleanup_if_needed(self):
        if len(self._mapping) > self._cleanup_threshold:
            sorted_ids = sorted(self._mapping.keys(), reverse=True)
            keep_ids = set(sorted_ids[:self._max_size])
            self._mapping = {k: v for k, v in self._mapping.items() if k in keep_ids}
            logger.info(f"ReplyTracker: cleanup, kept {len(self._mapping)} entries")


class ProjectManager:
    """
    Gestiona múltiples proyectos y sesiones.
    
    Cada proyecto tiene:
    - Un workspace (ruta)
    - Un modelo asociado
    - Múltiples sesiones
    
    El usuario puede:
    - Crear proyectos desde el explorador de archivos
    - Switch entre proyectos
    - Gestionar sesiones dentro de un proyecto
    - Hacer replies a mensajes del bot y mantener el contexto
    """
    
    def __init__(self):
        self.projects: Dict[str, ProjectInfo] = {}
        self.active_project_id: Optional[str] = None
        self.reply_tracker = ReplyTracker()
        self._load()
    
    def _load(self):
        try:
            with open(PROJECTS_FILE) as f:
                data = json.load(f)
            
            self.projects = {
                k: ProjectInfo.from_dict(v) 
                for k, v in data.get("projects", {}).items()
            }
            self.active_project_id = data.get("active_project_id")
            
            logger.info(f"Loaded {len(self.projects)} projects, active: {self.active_project_id}")
        except FileNotFoundError:
            logger.info("No projects.json found, starting fresh")
            self.projects = {}
            self.active_project_id = None
        except Exception as e:
            logger.error(f"Error loading projects.json: {e}")
            self.projects = {}
    
    def _save(self):
        data = {
            "projects": {k: v.to_dict() for k, v in self.projects.items()},
            "active_project_id": self.active_project_id,
            "saved_at": datetime.now().isoformat(),
        }
        with open(PROJECTS_FILE, "w") as f:
            json.dump(data, f, indent=2)
        logger.debug(f"Saved projects.json")
    
    def create_project(
        self, 
        workspace: str, 
        name: Optional[str] = None,
        model: Optional[str] = None,
        project_id: Optional[str] = None
    ) -> ProjectInfo:
        """
        Crea un nuevo proyecto.
        
        Args:
            workspace: Ruta absoluta del workspace
            name: Nombre del proyecto (por defecto: nombre del directorio)
            model: Modelo a usar (por defecto: DEFAULT_MODEL)
            project_id: ID custom (por defecto: generado)
        
        Returns:
            ProjectInfo creado
        """
        ws_path = Path(workspace)
        if not name:
            name = ws_path.name
        
        if not model:
            model = DEFAULT_MODEL
        
        if not project_id:
            project_id = f"proj_{int(time.time())}_{ws_path.name[:10]}"
        
        project = ProjectInfo(
            project_id=project_id,
            name=name,
            workspace=workspace,
            model=model,
        )
        
        self.projects[project_id] = project
        self._save()
        
        logger.info(f"Created project {project_id}: {name} @ {workspace}")
        return project
    
    def get_project(self, project_id: str) -> Optional[ProjectInfo]:
        return self.projects.get(project_id)
    
    def get_active_project(self) -> Optional[ProjectInfo]:
        if self.active_project_id:
            return self.projects.get(self.active_project_id)
        return None
    
    def set_active_project(self, project_id: str) -> bool:
        if project_id in self.projects:
            self.active_project_id = project_id
            self._save()
            logger.info(f"Active project set to: {project_id}")
            return True
        return False
    
    def close_project(self, project_id: str) -> bool:
        """
        Cierra un proyecto (lo marca como inactivo, no lo elimina).
        """
        if project_id == self.active_project_id:
            self.active_project_id = None
            self._save()
            logger.info(f"Project {project_id} closed")
            return True
        return False
    
    def delete_project(self, project_id: str) -> bool:
        """
        Elimina un proyecto completamente.
        """
        if project_id in self.projects:
            del self.projects[project_id]
            if self.active_project_id == project_id:
                self.active_project_id = None
            self._save()
            logger.info(f"Project {project_id} deleted")
            return True
        return False
    
    def add_session_to_project(
        self, 
        project_id: str, 
        session_id: str,
        title: Optional[str] = None
    ) -> Optional[SessionInfo]:
        """
        Añade una sesión a un proyecto.
        """
        project = self.projects.get(project_id)
        if not project:
            return None
        
        session = SessionInfo(
            session_id=session_id,
            title=title or f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            created_at=time.time(),
            is_active=True,
        )
        
        project.sessions[session_id] = session
        project.active_session_id = session_id
        
        self._save()
        logger.info(f"Added session {session_id[:15]} to project {project_id}")
        return session
    
    def get_active_session(self, project_id: Optional[str] = None) -> Optional[SessionInfo]:
        """
        Obtiene la sesión activa del proyecto activo o de un proyecto específico.
        """
        if project_id:
            project = self.projects.get(project_id)
        else:
            project = self.get_active_project()
        
        if not project or not project.active_session_id:
            return None
        
        return project.sessions.get(project.active_session_id)
    
    def set_active_session(self, project_id: str, session_id: str) -> bool:
        """
        Marca una sesión como activa dentro de un proyecto.
        """
        project = self.projects.get(project_id)
        if not project or session_id not in project.sessions:
            return False
        
        for sid, session in project.sessions.items():
            session.is_active = (sid == session_id)
        
        project.active_session_id = session_id
        self._save()
        logger.info(f"Session {session_id[:15]} activated in project {project_id}")
        return True
    
    def delete_session(self, project_id: str, session_id: str) -> bool:
        """
        Elimina una sesión de un proyecto.
        """
        project = self.projects.get(project_id)
        if not project:
            return False
        
        if session_id in project.sessions:
            del project.sessions[session_id]
            
            if project.active_session_id == session_id:
                remaining = list(project.sessions.keys())
                project.active_session_id = remaining[0] if remaining else None
            
            self._save()
            logger.info(f"Session {session_id[:15]} deleted from project {project_id}")
            return True
        return False
    
    def register_bot_message(self, message_id: int, project_id: str, session_id: str):
        """
        Registra un mensaje del bot para tracking de replies.
        """
        self.reply_tracker.register_message(message_id, project_id, session_id)
        
        project = self.projects.get(project_id)
        if project and session_id in project.sessions:
            project.sessions[session_id].message_ids.add(message_id)
    
    def lookup_reply(self, reply_to_message_id: int) -> Optional[tuple[str, str, str]]:
        """
        Busca el proyecto/sesión correspondiente a un reply.
        
        Args:
            reply_to_message_id: ID del mensaje al que se está respondiendo
        
        Returns:
            (project_id, session_id, project_name) si encontrado, None si no
        """
        result = self.reply_tracker.lookup(reply_to_message_id)
        if result:
            project_id, session_id = result
            project = self.projects.get(project_id)
            if project:
                return (project_id, session_id, project.name)
        return None
    
    def list_projects(self) -> List[ProjectInfo]:
        """
        Lista todos los proyectos.
        """
        return list(self.projects.values())
    
    def list_project_sessions(self, project_id: str) -> List[SessionInfo]:
        """
        Lista todas las sesiones de un proyecto.
        """
        project = self.projects.get(project_id)
        if project:
            return list(project.sessions.values())
        return []
    
    def get_current_context(self) -> Optional[dict]:
        """
        Obtiene el contexto actual (proyecto y sesión activos).
        
        Returns:
            dict con project_id, session_id, workspace, model, etc.
        """
        project = self.get_active_project()
        if not project:
            return None
        
        session = self.get_active_session()
        
        return {
            "project_id": project.project_id,
            "project_name": project.name,
            "workspace": project.workspace,
            "model": project.model,
            "session_id": project.active_session_id,
            "session_title": session.title if session else None,
        }
    
    def update_project_model(self, project_id: str, model: str) -> bool:
        """
        Actualiza el modelo de un proyecto.
        """
        project = self.projects.get(project_id)
        if project:
            project.model = model
            self._save()
            return True
        return False


_manager_instance: Optional[ProjectManager] = None


def get_manager() -> ProjectManager:
    global _manager_instance
    if _manager_instance is None:
        _manager_instance = ProjectManager()
    return _manager_instance