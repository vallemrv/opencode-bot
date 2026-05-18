"""
OpenCode HTTP + SSE client.
All communication with the OpenCode server goes through this module.
"""

import json
import asyncio
import aiohttp
import logging
from typing import AsyncIterator, Any

logger = logging.getLogger(__name__)


class OpenCodeClient:
    def __init__(self, host: str, port: int, password: str | None = None):
        self.base_url = f"http://{host}:{port}"
        self.headers = {}
        if password:
            self.headers["Authorization"] = f"Bearer {password}"

    # ------------------------------------------------------------------ #
    #  Generic helpers                                                     #
    # ------------------------------------------------------------------ #

    async def _get(self, path: str) -> Any:
        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with session.get(f"{self.base_url}{path}") as resp:
                resp.raise_for_status()
                return await resp.json()

    async def _post(self, path: str, body: dict) -> Any:
        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with session.post(
                f"{self.base_url}{path}",
                json=body,
                headers={**self.headers, "Content-Type": "application/json"},
            ) as resp:
                resp.raise_for_status()
                if resp.status == 204 or resp.content_length == 0:
                    return {}
                content_type = resp.content_type or ""
                if "json" not in content_type:
                    return {}
                return await resp.json()

    async def _delete(self, path: str) -> Any:
        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with session.delete(f"{self.base_url}{path}") as resp:
                resp.raise_for_status()
                try:
                    return await resp.json()
                except Exception:
                    return {}

    async def _patch(self, path: str, body: dict) -> Any:
        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with session.patch(
                f"{self.base_url}{path}",
                json=body,
                headers={**self.headers, "Content-Type": "application/json"},
            ) as resp:
                resp.raise_for_status()
                return await resp.json()

    # ------------------------------------------------------------------ #
    #  Server                                                              #
    # ------------------------------------------------------------------ #

    async def ping(self) -> bool:
        """Return True if the server is reachable."""
        try:
            await self._get("/session")
            return True
        except Exception:
            return False

    # ------------------------------------------------------------------ #
    #  Sessions                                                            #
    # ------------------------------------------------------------------ #

    async def list_sessions(self) -> list[dict]:
        """
        Returns sessions via /experimental/session which includes
        directory, project.worktree and model per session.
        """
        return await self._get("/experimental/session")

    async def create_session(self, title: str | None = None) -> dict:
        body = {}
        if title:
            body["title"] = title
        return await self._post("/session", body)

    async def delete_session(self, session_id: str) -> Any:
        return await self._delete(f"/session/{session_id}")

    async def rename_session(self, session_id: str, title: str) -> dict:
        return await self._patch(f"/session/{session_id}", {"title": title})

    async def get_session_status(self) -> dict:
        """Returns {sessionID: {type: busy|idle}} for all sessions."""
        return await self._get("/session/status")

    async def is_session_busy(self, session_id: str) -> bool:
        status = await self.get_session_status()
        s = status.get(session_id, {})
        return s.get("type") == "busy" if isinstance(s, dict) else False

    async def abort_session(self, session_id: str) -> Any:
        return await self._post(f"/session/{session_id}/abort", {})

    # ------------------------------------------------------------------ #
    #  Messages                                                            #
    # ------------------------------------------------------------------ #

    async def get_messages(self, session_id: str) -> list[dict]:
        return await self._get(f"/session/{session_id}/message")

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
        """Fire-and-forget prompt — returns immediately, response comes via SSE."""
        body: dict = {"parts": [{"type": "text", "text": text}]}
        if provider_id and model_id:
            body["model"] = {"providerID": provider_id, "modelID": model_id}
        return await self._post(f"/session/{session_id}/prompt_async", body)

    # ------------------------------------------------------------------ #
    #  Models                                                              #
    # ------------------------------------------------------------------ #

    async def list_models(self) -> list[dict]:
        """Returns the list of available models from the server."""
        return await self._get("/api/model")

    # ------------------------------------------------------------------ #
    #  Projects                                                            #
    # ------------------------------------------------------------------ #

    async def list_projects(self) -> list[dict]:
        return await self._get("/project")

    async def open_project(self, path: str) -> dict:
        return await self._post("/project", {"path": path})

    async def close_project(self, project_id: str) -> Any:
        return await self._delete(f"/project/{project_id}")

    # ------------------------------------------------------------------ #
    #  SSE event stream                                                    #
    # ------------------------------------------------------------------ #

    async def event_stream(self) -> AsyncIterator[dict]:
        """
        Async generator that yields every SSE event as a dict.
        Reconnects automatically on connection errors.
        Each yielded dict has at minimum: {"type": "...", "properties": {...}}
        """
        retry_delay = 2
        while True:
            try:
                async with aiohttp.ClientSession(headers=self.headers) as session:
                    async with session.get(f"{self.base_url}/event") as resp:
                        resp.raise_for_status()
                        retry_delay = 2  # reset on success
                        async for line in resp.content:
                            line = line.decode("utf-8").strip()
                            if not line.startswith("data:"):
                                continue
                            raw = line[len("data:"):].strip()
                            if not raw:
                                continue
                            try:
                                event = json.loads(raw)
                                yield event
                            except json.JSONDecodeError:
                                logger.warning("SSE: invalid JSON: %s", raw)
            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.warning("SSE disconnected (%s), retrying in %ds", exc, retry_delay)
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 30)
