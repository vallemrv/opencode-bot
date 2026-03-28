#!/usr/bin/env python3
"""
opencode_proxy.py — Proxy para preguntas interactivas del LLM

Cuando OpenCode/LLM hace una pregunta (tool permission, confirmación, etc.),
este módulo detecta la pregunta y genera botones inline para Telegram.
"""

import logging
import re
from dataclasses import dataclass
from typing import List, Optional, Tuple
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

logger = logging.getLogger(__name__)


@dataclass
class LLMQuestion:
    """Representa una pregunta del LLM"""
    text: str
    question_type: str  # permission, confirmation, selection, input
    options: List[Tuple[str, str]]  # (label, callback_data)
    tool_name: Optional[str] = None
    command: Optional[str] = None
    file_path: Optional[str] = None


class OpenCodeProxy:
    """
    Proxy para interacciones LLM ↔ Usuario via Telegram
    
    Detecta preguntas del LLM y genera botones inline apropiados.
    """
    
    # Patrones para detectar tipos de preguntas
    PERMISSION_PATTERNS = [
        (r'¿[Qq]uieres ejecutar este comando?', 'command'),
        (r'[Ee]jecutar.*comando', 'command'),
        (r'[Pp]ermiso.*bash', 'bash'),
        (r'[Pp]ermiso.*shell', 'shell'),
        (r'[Aa]utorización.*ejecutar', 'command'),
        (r'[Aa]llow.*tool', 'tool'),
        (r'[Pp]ermission.*run', 'command'),
    ]
    
    CONFIRMATION_PATTERNS = [
        (r'¿[Qq]uieres continuar\?', 'continue'),
        (r'¿[Cc]ontinuar\?', 'continue'),
        (r'[Cc]onfirm.*continuar', 'continue'),
        (r'[Cc]onfirm.*proceder', 'proceed'),
        (r'[Ss]ure you want', 'confirm'),
    ]
    
    SELECTION_PATTERNS = [
        (r'¿[Qq]ué archivo.*editar\?', 'file_edit'),
        (r'¿[Qq]ué.*quieres.*\?', 'selection'),
        (r'[Cc]hoose.*file', 'file_select'),
        (r'[Ss]elect.*option', 'option'),
        (r'[Pp]ick.*option', 'option'),
    ]
    
    def __init__(self):
        self.pending_questions = {}  # session_id -> LLMQuestion
        self._question_counter = 0
    
    def detect_question(self, text: str, session_id: str, event_context: dict = None) -> Optional[LLMQuestion]:
        """
        Detecta si el texto es una pregunta del LLM
        
        Args:
            text: Texto del mensaje del LLM
            session_id: ID de sesión
            event_context: Contexto adicional del evento SSE
        
        Returns:
            LLMQuestion si es una pregunta, None si no
        """
        # Check patrones de permiso
        for pattern, perm_type in self.PERMISSION_PATTERNS:
            if re.search(pattern, text):
                question = self._create_permission_question(text, perm_type, session_id)
                if question:
                    logger.info(f"Pregunta de permiso detectada: {perm_type}")
                    return question
        
        # Check patrones de confirmación
        for pattern, conf_type in self.CONFIRMATION_PATTERNS:
            if re.search(pattern, text):
                question = self._create_confirmation_question(text, conf_type, session_id)
                if question:
                    logger.info(f"Pregunta de confirmación detectada: {conf_type}")
                    return question
        
        # Check patrones de selección
        for pattern, sel_type in self.SELECTION_PATTERNS:
            if re.search(pattern, text):
                question = self._create_selection_question(text, sel_type, session_id)
                if question:
                    logger.info(f"Pregunta de selección detectada: {sel_type}")
                    return question
        
        # Check si hay comando bash explícito
        bash_match = re.search(r'```(?:bash|sh)?\n(.+?)\n```', text, re.DOTALL)
        if bash_match and ('¿' in text or '?' in text):
            command = bash_match.group(1).strip()
            question = self._create_permission_question(
                text, 
                'command', 
                session_id,
                command=command
            )
            logger.info(f"Comando bash detectado: {command[:50]}")
            return question
        
        return None
    
    def _create_permission_question(
        self, 
        text: str, 
        perm_type: str, 
        session_id: str,
        command: str = None
    ) -> Optional[LLMQuestion]:
        """Crea pregunta de permiso"""
        self._question_counter += 1
        q_id = f"perm_{self._question_counter}"
        
        options = [
            ("✅ Sí, permitir", f"perm_allow:{q_id}:{session_id}"),
            ("❌ No, denegar", f"perm_deny:{q_id}:{session_id}"),
            ("⚠️ Solo esta vez", f"perm_once:{q_id}:{session_id}"),
        ]
        
        if perm_type == 'command' and command:
            options.insert(1, ("📋 Ver comando", f"perm_view:{q_id}:{session_id}"))
        
        question = LLMQuestion(
            text=text,
            question_type="permission",
            options=options,
            tool_name=perm_type,
            command=command,
        )
        
        self.pending_questions[q_id] = question
        return question
    
    def _create_confirmation_question(
        self, 
        text: str, 
        conf_type: str, 
        session_id: str
    ) -> Optional[LLMQuestion]:
        """Crea pregunta de confirmación"""
        self._question_counter += 1
        q_id = f"conf_{self._question_counter}"
        
        options = [
            ("✅ Sí, continuar", f"conf_yes:{q_id}:{session_id}"),
            ("❌ No, cancelar", f"conf_no:{q_id}:{session_id}"),
        ]
        
        if conf_type in ('proceed', 'confirm'):
            options.insert(1, ("⏭️ Saltar", f"conf_skip:{q_id}:{session_id}"))
        
        question = LLMQuestion(
            text=text,
            question_type="confirmation",
            options=options,
        )
        
        self.pending_questions[q_id] = question
        return question
    
    def _create_selection_question(
        self, 
        text: str, 
        sel_type: str, 
        session_id: str
    ) -> Optional[LLMQuestion]:
        """Crea pregunta de selección"""
        self._question_counter += 1
        q_id = f"sel_{self._question_counter}"
        
        # Extraer archivos si es selección de archivos
        files = re.findall(r'`([^`]+\.py|[^`]+\.js|[^`]+\.ts|[^`]+\.json)`', text)
        
        options = []
        if files:
            for f in files[:5]:  # Máximo 5 archivos
                short_name = f.split('/')[-1][:30]
                options.append((f"📄 {short_name}", f"sel_file:{q_id}:{session_id}:{f}"))
            options.append(("✏️ Otro archivo", f"sel_other:{q_id}:{session_id}"))
        else:
            # Opciones genéricas
            options = [
                ("Opción 1", f"sel_opt1:{q_id}:{session_id}"),
                ("Opción 2", f"sel_opt2:{q_id}:{session_id}"),
                ("✏️ Especificar", f"sel_custom:{q_id}:{session_id}"),
            ]
        
        question = LLMQuestion(
            text=text,
            question_type="selection",
            options=options,
        )
        
        self.pending_questions[q_id] = question
        return question
    
    def get_keyboard(self, question: LLMQuestion) -> InlineKeyboardMarkup:
        """Genera teclado inline para una pregunta"""
        keyboard = []
        
        # Agrupar opciones de a 2
        options = question.options
        for i in range(0, len(options), 2):
            row = []
            for label, callback in options[i:i+2]:
                row.append(InlineKeyboardButton(label, callback_data=callback))
            keyboard.append(row)
        
        return InlineKeyboardMarkup(keyboard)
    
    def handle_callback(self, callback_data: str, user_response: str = None) -> Tuple[str, str]:
        """
        Procesa respuesta del usuario a pregunta inline
        
        Args:
            callback_data: El callback_data del botón presionado
            user_response: Texto adicional si el usuario escribió algo
        
        Returns:
            (response_text, action) para enviar al LLM
        """
        parts = callback_data.split(':')
        if len(parts) < 3:
            return "", "unknown"
        
        action_type = parts[0]  # perm_allow, conf_yes, sel_file, etc.
        question_id = parts[1]
        session_id = parts[2] if len(parts) > 2 else None
        
        # Obtener pregunta pendiente
        question = self.pending_questions.get(question_id)
        if not question:
            logger.warning(f"Pregunta {question_id} no encontrada")
            return "", "not_found"
        
        response_text = ""
        action = ""
        
        # Procesar según tipo de acción
        if action_type == "perm_allow":
            response_text = "Sí, permite la ejecución"
            action = "allow"
        elif action_type == "perm_deny":
            response_text = "No, deniega la ejecución"
            action = "deny"
        elif action_type == "perm_once":
            response_text = "Permitir solo esta vez"
            action = "allow_once"
        elif action_type == "perm_view":
            response_text = f"Mostrar comando: {question.command or 'N/A'}"
            action = "view"
        
        elif action_type == "conf_yes":
            response_text = "Sí, confirma"
            action = "confirm"
        elif action_type == "conf_no":
            response_text = "No, cancela"
            action = "cancel"
        elif action_type == "conf_skip":
            response_text = "Saltar esta paso"
            action = "skip"
        
        elif action_type == "sel_file":
            file_path = parts[3] if len(parts) > 3 else "unknown"
            response_text = f"Seleccionar archivo: {file_path}"
            action = "file_select"
        elif action_type == "sel_other":
            response_text = user_response or "Especificar otro archivo"
            action = "file_other"
        elif action_type == "sel_custom":
            response_text = user_response or "Opción personalizada"
            action = "custom"
        
        # Limpiar pregunta
        if action not in ('view',):
            self.pending_questions.pop(question_id, None)
        
        logger.info(f"Respuesta a pregunta {question_id}: {action}")
        return response_text, action
    
    def has_pending_question(self, session_id: str) -> bool:
        """Check si hay pregunta pendiente para una sesión"""
        return any(
            q_id in self.pending_questions 
            for q_id in self.pending_questions
        )
    
    def clear_session(self, session_id: str):
        """Limpia preguntas de una sesión"""
        to_remove = [
            q_id for q_id, q in self.pending_questions.items()
            if session_id in q_id
        ]
        for q_id in to_remove:
            del self.pending_questions[q_id]


# Singleton global
_proxy: Optional[OpenCodeProxy] = None


def get_proxy() -> OpenCodeProxy:
    global _proxy
    if _proxy is None:
        _proxy = OpenCodeProxy()
    return _proxy
