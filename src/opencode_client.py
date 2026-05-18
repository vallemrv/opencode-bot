"""
OpenCode HTTP + SSE client.
Single persistent connection pool for all HTTP requests.
SSE uses its own connection (long-lived stream).
"""

import json
import asyncio
import aiohttp
import logging
from typing import AsyncIterator, Any

logger = logging.getLogger(__name__)

# Timeouts
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=30, connect=10)
SSE_TIMEOUT = aiohttp.ClientTimeout(total=None, connect=5, sock_read=30)

# Connection limits (conservative)
MAX_CONNECTIONS = 10
MAX_CONNECTIONS_PER_HOST = 5


class OpenCodeClient:
    def __init__(self, host: str, port: int, password: str | None = None):
        self.base_url = f"http://{host}:{port}"
        self._headers = {"Content-Type": "application/json"}
        if password:
            self._headers["Authorization"] = f"Bearer {password}"
        
        # Single session for all HTTP requests (not SSE)
        self._session: aiohttp.ClientSession | None = None
    
    async def close(self):
        """Close the HTTP session. Call on shutdown."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
    
    def _get_session(self) -> aiohttp.ClientSession:
        """Get or create the shared HTTP session."""
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(
                limit=MAX_CONNECTIONS,
                limit_per_host=MAX_CONNECTIONS_PER_HOST,
                ttl_dns_cache=300,
            )
            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=HTTP_TIMEOUT,
                headers=self._headers,
            )
        return self._session

    # ------------------------------------------------------------------ #
    #  HTTP helpers                                                       #
    # ------------------------------------------------------------------ #

    async def _get(self, path: str) -> Any:
        session = self._get_session()
        url = f"{self.base_url}{path}"
        logger.debug(f"GET {url}")
        async with session.get(url) as resp:
            if resp.status != 200:
                text = await resp.text()
                logger.error(f"GET {url} failed: {resp.status} - {text[:200]}")
            resp.raise_for_status()
            return await resp.json()

    async def _post(self, path: str, body: dict | None = None) -> Any:
        session = self._get_session()
        async with session.post(f"{self.base_url}{path}", json=body or {}) as resp:
            resp.raise_for_status()
            if resp.status == 204 or resp.content_length == 0:
                return {}
            if "json" not in (resp.content_type or ""):
                return {}
            return await resp.json()

    async def _delete(self, path: str) -> Any:
        session = self._get_session()
        async with session.delete(f"{self.base_url}{path}") as resp:
            resp.raise_for_status()
            try:
                return await resp.json()
            except:
                return {}

    async def _patch(self, path: str, body: dict) -> Any:
        session = self._get_session()
        async with session.patch(f"{self.base_url}{path}", json=body) as resp:
            resp.raise_for_status()
            return await resp.json()

    # ------------------------------------------------------------------ #
    #  Server                                                            #
    # ------------------------------------------------------------------ #

    async def ping(self) -> bool:
        try:
            await self._get("/session")
            return True
        except:
            return False

    # ------------------------------------------------------------------ #
    #  Sessions                                                          #
    # ------------------------------------------------------------------ #

    async def list_sessions(self) -> list[dict]:
        return await self._get("/experimental/session")

    async def create_session(
        self,
        directory: str | None = None,
        title: str | None = None,
        provider_id: str | None = None,
        model_id: str | None = None,
    ) -> dict:
        body = {}
        if title:
            body["title"] = title
        if provider_id and model_id:
            body["model"] = {"providerID": provider_id, "id": model_id}
        
        path = "/session"
        if directory:
            path = f"{path}?directory={directory}"
        
        return await self._post(path, body)

    async def delete_session(self, session_id: str) -> Any:
        return await self._delete(f"/session/{session_id}")

    async def rename_session(self, session_id: str, title: str) -> dict:
        return await self._patch(f"/session/{session_id}", {"title": title})

    async def update_session_model(self, session_id: str, provider_id: str, model_id: str) -> dict:
        return await self._patch(f"/session/{session_id}", {
            "model": {"providerID": provider_id, "id": model_id}
        })

    async def update_session(self, session_id: str, **kwargs) -> dict:
        return await self._patch(f"/session/{session_id}", kwargs)

    async def get_session_status(self) -> dict:
        return await self._get("/session/status")

    async def is_session_busy(self, session_id: str) -> bool:
        status = await self.get_session_status()
        s = status.get(session_id, {})
        return s.get("type") == "busy" if isinstance(s, dict) else False

    async def abort_session(self, session_id: str) -> Any:
        return await self._post(f"/session/{session_id}/abort", {})

    # ------------------------------------------------------------------ #
    #  Messages                                                          #
    # ------------------------------------------------------------------ #

    async def get_messages(self, session_id: str, directory: str | None = None) -> list[dict]:
        path = f"/session/{session_id}/message"
        if directory:
            path = f"{path}?directory={directory}"
        return await self._get(path)

    async def send_message(
        self,
        session_id: str,
        text: str,
        provider_id: str | None = None,
        model_id: str | None = None,
    ) -> Any:
        body: dict = {"parts": [{"type": "text", "text": text}]}
        if provider_id and model_id:
            body["model"] = {"providerID": provider_id, "modelID": model_id}
        return await self._post(f"/session/{session_id}/message", body)

    async def send_message_async(
        self,
        session_id: str,
        text: str,
        provider_id: str | None = None,
        model_id: str | None = None,
    ) -> Any:
        """Fire-and-forget prompt. Response comes via SSE."""
        body: dict = {"parts": [{"type": "text", "text": text}]}
        if provider_id and model_id:
            body["model"] = {"providerID": provider_id, "modelID": model_id}
        return await self._post(f"/session/{session_id}/prompt_async", body)

    # ------------------------------------------------------------------ #
    #  Models                                                            #
    # ------------------------------------------------------------------ #

    async def list_models(self) -> list[dict]:
        return await self._get("/api/model")

    # ------------------------------------------------------------------ #
    #  Projects                                                          #
    # ------------------------------------------------------------------ #

    async def list_projects(self) -> list[dict]:
        return await self._get("/project")

    async def open_project(self, path: str) -> dict:
        return await self._post("/project", {"path": path})

    async def close_project(self, project_id: str) -> Any:
        return await self._delete(f"/project/{project_id}")

    # ------------------------------------------------------------------ #
    #  SSE event stream                                                  #
    # ------------------------------------------------------------------ #

    async def event_stream(self) -> AsyncIterator[dict]:
        """
        SSE (Server-Sent Events) stream.
        
        How it works:
        - Opens ONE long-lived HTTP connection to /global/event
        - Server sends events as they happen: "data: {...}\n\n"
        - If connection drops, reconnects with exponential backoff
        - Yields each event as a dict: {"type": "...", "properties": {...}}
        
        IMPORTANT: Uses its own session, NOT the shared HTTP session.
        This is intentional - SSE is a persistent stream.
        """
        retry_delay = 2
        max_buffer = 1024 * 1024  # 1MB max buffer
        
        while True:
            try:
                # Create fresh connector for SSE (don't use shared pool)
                connector = aiohttp.TCPConnector(limit=1)
                async with aiohttp.ClientSession(
                    connector=connector,
                    timeout=SSE_TIMEOUT,
                    headers=self._headers,
                ) as session:
                    async with session.get(
                        f"{self.base_url}/global/event",
                        headers={"Accept": "text/event-stream"},
                    ) as resp:
                        resp.raise_for_status()
                        retry_delay = 2
                        logger.info("SSE connected to /global/event")
                        
                        buffer = b""
                        async for chunk in resp.content.iter_chunked(8192):
                            buffer += chunk
                            
                            # Prevent memory exhaustion
                            if len(buffer) > max_buffer:
                                logger.error("SSE buffer overflow, resetting")
                                buffer = b""
                                continue
                            
                            # Process complete lines
                            while b"\n" in buffer:
                                line_bytes, buffer = buffer.split(b"\n", 1)
                                line = line_bytes.decode("utf-8", errors="replace").strip()
                                
                                if not line or not line.startswith("data:"):
                                    continue
                                
                                raw = line[5:].strip()
                                if not raw:
                                    continue
                                
                                try:
                                    yield json.loads(raw)
                                except json.JSONDecodeError:
                                    logger.warning(f"SSE invalid JSON: {raw[:100]}")
                        
                        logger.warning("SSE stream ended, reconnecting...")
                        
            except asyncio.CancelledError:
                logger.info("SSE listener cancelled")
                return
            except Exception as exc:
                logger.warning(f"SSE error: {exc}, retrying in {retry_delay}s")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 30)