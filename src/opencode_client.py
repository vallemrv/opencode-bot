#!/usr/bin/env python3
"""
opencode_client.py — Cliente HTTP + SSE para OpenCode server

Usa la API nativa de OpenCode:
- /session/status - Obtener estado de sesiones (busy/idle)
- /session/:id/message - Enviar mensaje (blocking)
- /session/:id/prompt_async - Enviar mensaje (async, no wait)
- /session/:id/abort - Cancelar sesión en curso
- /event - SSE stream para eventos real-time

OpenCode maneja internamente la cola de mensajes por sesión.
"""

import asyncio
import json
import logging
import os
import subprocess
import time
from datetime import datetime
from typing import AsyncGenerator, Optional
import aiohttp

logger = logging.getLogger(__name__)

OPENCODE_PORT = int(os.getenv("OPENCODE_PORT", "4096"))
OPENCODE_BASE_URL = f"http://10.0.0.8:{OPENCODE_PORT}"
SSE_TIMEOUT = aiohttp.ClientTimeout(total=None, connect=10, sock_read=None)

_last_sse_event_time: float = 0

def get_last_sse_event_time() -> float:
    return _last_sse_event_time

def set_last_sse_event_time(timestamp: float):
    global _last_sse_event_time
    _last_sse_event_time = timestamp


class SSELogger:
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.event_count = 0
        self.text_buffer = ""
        self.start_time = datetime.now()
    
    def log_event(self, event: dict):
        self.event_count += 1
        etype = event.get("type", "unknown")
        
        if etype == "session.status":
            status = event.get("properties", {}).get("status", {}).get("type", "unknown")
            logger.info(f"[SSE:{self.session_id[:8]}] Status: {status}")
        
        elif etype == "session.error":
            error = event.get("properties", {}).get("error", "unknown")
            logger.error(f"[SSE:{self.session_id[:8]}] Error: {error}")
        
        elif etype == "message.updated":
            info = event.get("properties", {}).get("info", {})
            role = info.get("role", "unknown")
            msg_id = info.get("id", "unknown")[:8]
            logger.debug(f"[SSE:{self.session_id[:8]}] Message: {role} (id: {msg_id})")
        
        elif etype == "message.part.updated":
            part = event.get("properties", {}).get("part", {})
            part_type = part.get("type", "unknown")
            part_id = part.get("id", "unknown")[:8]
            text = part.get("text", "")
            if text:
                logger.debug(f"[SSE:{self.session_id[:8]}] Part {part_type} ({part_id}): {text[:50]}...")
        
        elif etype == "message.part.delta":
            delta = event.get("delta", "")
            part_id = event.get("properties", {}).get("partID", "unknown")[:8]
            if delta:
                self.text_buffer += delta
                logger.debug(f"[SSE:{self.session_id[:8]}] Delta ({part_id}): +{len(delta)} chars")
        
        else:
            logger.debug(f"[SSE:{self.session_id[:8]}] Event: {etype}")
    
    def summary(self):
        elapsed = (datetime.now() - self.start_time).total_seconds()
        return {
            "session_id": self.session_id[:20],
            "duration_sec": round(elapsed, 2),
            "total_events": self.event_count,
            "total_chars": len(self.text_buffer),
        }


def _parse_sse_block(block: str) -> Optional[dict]:
    data_line = None
    for line in block.splitlines():
        line = line.strip()
        if line.startswith("data:"):
            data_line = line[5:].strip()
        elif line.startswith(": ") or line == ":":
            continue
    
    if data_line is None:
        return None
    if data_line == "[DONE]":
        return {"type": "done"}
    
    try:
        return json.loads(data_line)
    except json.JSONDecodeError:
        return {"type": "raw", "data": data_line}


class OpenCodeClient:
    def __init__(self, base_url: str = OPENCODE_BASE_URL):
        self.base_url = base_url.rstrip("/")

    async def _get(self, path: str, timeout: float = 10) -> dict:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{self.base_url}{path}",
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as resp:
                resp.raise_for_status()
                return await resp.json()

    async def _post(self, path: str, payload: dict, timeout: float = 15) -> dict:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self.base_url}{path}",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as resp:
                resp.raise_for_status()
                return await resp.json()
    
    async def _post_no_wait(self, path: str, payload: dict) -> bool:
        """POST que no espera respuesta (async)"""
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self.base_url}{path}",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                return resp.status == 204

    async def _patch(self, path: str, payload: dict, timeout: float = 15) -> dict:
        async with aiohttp.ClientSession() as session:
            async with session.patch(
                f"{self.base_url}{path}",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as resp:
                resp.raise_for_status()
                return await resp.json()

    async def _delete(self, path: str, timeout: float = 10) -> bool:
        async with aiohttp.ClientSession() as session:
            async with session.delete(
                f"{self.base_url}{path}",
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as resp:
                resp.raise_for_status()
                return True

    async def health_check(self) -> bool:
        try:
            await self._get("/global/health")
            return True
        except Exception:
            try:
                await self._get("/session")
                return True
            except Exception:
                return False

    async def list_sessions(self) -> list:
        try:
            data = await self._get("/session")
            return data if isinstance(data, list) else data.get("sessions", [])
        except Exception as e:
            logger.error(f"Error listando sesiones: {e}")
            return []

    async def get_session_status(self, session_id: str) -> dict:
        """
        Obtener estado de una sesión específica.
        
        Returns:
            {"type": "busy"} o {"type": "idle"}
        """
        try:
            statuses = await self._get("/session/status")
            return statuses.get(session_id, {"type": "unknown"})
        except Exception as e:
            logger.error(f"Error obteniendo status: {e}")
            return {"type": "unknown"}

    async def get_all_session_status(self) -> dict:
        """
        Obtener estado de todas las sesiones.
        
        Returns:
            {session_id: {"type": "busy"} | {"type": "idle"}}
        """
        try:
            return await self._get("/session/status")
        except Exception as e:
            logger.error(f"Error obteniendo statuses: {e}")
            return {}

    async def is_session_busy(self, session_id: str) -> bool:
        """Check if session is currently busy (processing)"""
        status = await self.get_session_status(session_id)
        return status.get("type") == "busy"

    async def create_session(self, title: Optional[str] = None, model: Optional[str] = None) -> dict:
        payload: dict = {}
        if title:
            payload["title"] = title
        return await self._post("/session", payload)

    async def get_session(self, session_id: str) -> dict:
        """Obtener detalles de una sesión"""
        return await self._get(f"/session/{session_id}")

    async def delete_session(self, session_id: str) -> bool:
        try:
            return await self._delete(f"/session/{session_id}")
        except Exception as e:
            logger.error(f"Error borrando sesión {session_id}: {e}")
            return False

    async def update_session(self, session_id: str, title: str) -> dict:
        payload = {"title": title}
        return await self._patch(f"/session/{session_id}", payload)

    async def abort_session(self, session_id: str) -> bool:
        """
        Abortar/cancelar una sesión que está en curso.
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.base_url}/session/{session_id}/abort",
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    return resp.status in [200, 204]
        except Exception as e:
            logger.error(f"Error abortando sesión {session_id}: {e}")
            return False

    async def get_messages(self, session_id: str, limit: int = 0) -> list:
        """
        Obtener mensajes de una sesión.
        
        Args:
            session_id: ID de sesión
            limit: Límite de mensajes (0 = todos)
        """
        path = f"/session/{session_id}/message"
        if limit > 0:
            path += f"?limit={limit}"
        
        try:
            data = await self._get(path, timeout=15)
            return data if isinstance(data, list) else []
        except Exception as e:
            logger.error(f"Error obteniendo mensajes: {e}")
            return []

    async def send_message(self, session_id: str, payload: dict, timeout: float = 0) -> dict:
        """
        Enviar mensaje y esperar respuesta (blocking).
        
        Args:
            session_id: ID de sesión
            payload: {parts: [...], model?: {...}}
            timeout: Timeout en segundos (0 = sin límite, 24h)
        """
        actual_timeout = 86400 if timeout <= 0 else timeout
        return await self._post(f"/session/{session_id}/message", payload, timeout=actual_timeout)

    async def send_message_async(self, session_id: str, payload: dict) -> bool:
        """
        Enviar mensaje sin esperar respuesta (async).
        
        El mensaje se encola en OpenCode y se procesa cuando la sesión esté idle.
        
        Returns:
            True si se envió correctamente (204 No Content)
        """
        try:
            return await self._post_no_wait(f"/session/{session_id}/prompt_async", payload)
        except Exception as e:
            logger.error(f"Error enviando mensaje async: {e}")
            return False

    async def stream_session_events(
        self,
        session_id: str,
        ready_event: Optional[asyncio.Event] = None,
        inactivity_timeout: float = 600.0,
    ) -> AsyncGenerator[dict, None]:
        """
        Stream de eventos SSE para una sesión.
        
        OpenCode envía eventos para TODAS las sesiones en /event.
        Filtramos por session_id.
        """
        queue: asyncio.Queue = asyncio.Queue()
        sse_logger = SSELogger(session_id)
        reconnect_count = 0
        max_reconnects = 3

        def on_event(evt: dict):
            props = evt.get("properties", {})
            eid = (
                props.get("sessionID")
                or props.get("session_id")
                or evt.get("sessionID")
            )
            if eid is None or eid == session_id:
                sse_logger.log_event(evt)
                queue.put_nowait(evt)

        async def _subscribe():
            nonlocal reconnect_count
            url = f"{self.base_url}/event"
            logger.info(f"[SSE:{session_id[:8]}] Conectando a {url}")
            try:
                async with aiohttp.ClientSession(timeout=SSE_TIMEOUT) as http_session:
                    async with http_session.get(url) as resp:
                        resp.raise_for_status()
                        logger.info(f"[SSE:{session_id[:8]}] ✅ Conectado")
                        buffer = ""
                        first_chunk = True
                        async for chunk in resp.content.iter_any():
                            if first_chunk:
                                first_chunk = False
                                logger.info(f"[SSE:{session_id[:8]}] Primer chunk")
                                if ready_event is not None:
                                    ready_event.set()
                            text = chunk.decode("utf-8", errors="replace")
                            buffer += text
                            while "\n\n" in buffer:
                                block, buffer = buffer.split("\n\n", 1)
                                event = _parse_sse_block(block)
                                if event is None:
                                    continue
                                try:
                                    on_event(event)
                                except Exception as e:
                                    logger.error(f"[SSE:{session_id[:8]}] Error: {e}")
                        logger.info(f"[SSE:{session_id[:8]}] Stream terminado")
            except asyncio.CancelledError:
                logger.info(f"[SSE:{session_id[:8]}] Cancelado")
                raise
            except Exception as e:
                logger.error(f"[SSE:{session_id[:8]}] Error: {e}")
                reconnect_count += 1
                if reconnect_count <= max_reconnects:
                    logger.warning(f"[SSE:{session_id[:8]}] Reintentando ({reconnect_count}/{max_reconnects})...")
                    await asyncio.sleep(2 ** reconnect_count)
                    await _subscribe()
            finally:
                if ready_event is not None and not ready_event.is_set():
                    ready_event.set()

        sse_task = asyncio.create_task(_subscribe())

        try:
            last_event_time = asyncio.get_event_loop().time()
            set_last_sse_event_time(time.time())
            while True:
                try:
                    evt = await asyncio.wait_for(queue.get(), timeout=1.0)
                    last_event_time = asyncio.get_event_loop().time()
                    set_last_sse_event_time(time.time())
                    yield evt
                except asyncio.TimeoutError:
                    elapsed = asyncio.get_event_loop().time() - last_event_time
                    if inactivity_timeout > 0 and elapsed >= inactivity_timeout:
                        logger.warning(f"[SSE:{session_id[:8]}] Timeout inactividad")
                        break
                    continue
        finally:
            sse_task.cancel()
            try:
                await sse_task
            except asyncio.CancelledError:
                pass
            summary = sse_logger.summary()
            logger.info(f"[SSE:{session_id[:8]}] 📊 {summary}")


def get_models_from_cli() -> dict[str, list[dict]]:
    try:
        result = subprocess.run(
            ["opencode", "models"],
            capture_output=True,
            text=True,
            timeout=15,
        )
        output = result.stdout + result.stderr
        return _parse_models_output(output)
    except FileNotFoundError:
        logger.error("opencode no encontrado")
        return {}
    except subprocess.TimeoutExpired:
        logger.error("Timeout opencode models")
        return {}
    except Exception as e:
        logger.error(f"Error opencode models: {e}")
        return {}


def _parse_models_output(output: str) -> dict[str, list[dict]]:
    try:
        data = json.loads(output)
        if isinstance(data, dict):
            result = {}
            for provider, models in data.items():
                if isinstance(models, list):
                    result[provider] = [
                        {"id": m if isinstance(m, str) else m.get("id", m),
                         "name": m if isinstance(m, str) else m.get("name", m.get("id", m))}
                        for m in models
                    ]
            return result
        if isinstance(data, list):
            result: dict = {}
            for item in data:
                mid = item if isinstance(item, str) else item.get("id", "")
                if "/" in mid:
                    provider, model = mid.split("/", 1)
                else:
                    provider, model = "other", mid
                result.setdefault(provider, []).append({"id": mid, "name": model})
            return result
    except (json.JSONDecodeError, Exception):
        pass

    result = {}
    current_provider = None
    
    for line in output.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        
        if "/" in stripped and not line.startswith(" ") and not line.startswith("\t"):
            parts = stripped.split("/", 1)
            if len(parts) == 2:
                provider = parts[0].lower()
                model = parts[1]
                current_provider = provider
                result.setdefault(provider, [])
                result[provider].append({"id": stripped, "name": model})
            continue
        
        if not line.startswith(" ") and not line.startswith("\t"):
            current_provider = stripped.lower()
            result.setdefault(current_provider, [])
            continue
        
        if current_provider and stripped:
            if "/" in stripped:
                model_id = stripped
                model_name = stripped.split("/", 1)[-1]
            else:
                model_id = f"{current_provider}/{stripped}"
                model_name = stripped
            result[current_provider].append({"id": model_id, "name": model_name})

    return result


_client_instance: Optional[OpenCodeClient] = None


def get_client() -> OpenCodeClient:
    global _client_instance
    if _client_instance is None:
        _client_instance = OpenCodeClient()
    return _client_instance