"""
OpenCode HTTP + SSE client.
Uses the `directory` query param to scope all operations to a specific project.
SSE uses /global/event for all events across all projects.
"""

import json
import asyncio
import aiohttp
import logging
import sqlite3
from pathlib import Path
from typing import AsyncIterator, Any

logger = logging.getLogger(__name__)

HTTP_TIMEOUT = aiohttp.ClientTimeout(total=30, connect=10)
SSE_TIMEOUT  = aiohttp.ClientTimeout(total=None, connect=5, sock_read=30)

MAX_CONNECTIONS          = 10
MAX_CONNECTIONS_PER_HOST = 5


class OpenCodeClient:
    def __init__(self, host: str, port: int, password: str | None = None):
        self.base_url  = f"http://{host}:{port}"
        self._headers  = {"Content-Type": "application/json"}
        if password:
            self._headers["Authorization"] = f"Bearer {password}"
        self._session: aiohttp.ClientSession | None = None

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    def _get_session(self) -> aiohttp.ClientSession:
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
    #  Low-level helpers                                                   #
    # ------------------------------------------------------------------ #

    def _url(self, path: str, directory: str | None = None) -> str:
        url = f"{self.base_url}{path}"
        if directory:
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}directory={directory}"
        return url

    async def _get(self, path: str, directory: str | None = None) -> Any:
        url = self._url(path, directory)
        logger.debug(f"GET {url}")
        async with self._get_session().get(url) as resp:
            if resp.status != 200:
                text = await resp.text()
                logger.error(f"GET {url} → {resp.status}: {text[:200]}")
            resp.raise_for_status()
            return await resp.json()

    async def _post(self, path: str, body: dict | None = None, directory: str | None = None) -> Any:
        url = self._url(path, directory)
        async with self._get_session().post(url, json=body or {}) as resp:
            resp.raise_for_status()
            if resp.status == 204 or resp.content_length == 0:
                return {}
            if "json" not in (resp.content_type or ""):
                return {}
            return await resp.json()

    async def _delete(self, path: str, directory: str | None = None) -> Any:
        url = self._url(path, directory)
        async with self._get_session().delete(url) as resp:
            resp.raise_for_status()
            try:
                return await resp.json()
            except Exception:
                return {}

    async def _patch(self, path: str, body: dict, directory: str | None = None) -> Any:
        url = self._url(path, directory)
        async with self._get_session().patch(url, json=body) as resp:
            resp.raise_for_status()
            return await resp.json()

    # ------------------------------------------------------------------ #
    #  Health                                                              #
    # ------------------------------------------------------------------ #

    async def ping(self) -> bool:
        try:
            await self._get("/global/health")
            return True
        except Exception:
            return False

    # ------------------------------------------------------------------ #
    #  Projects  (native OpenCode concept)                                 #
    # ------------------------------------------------------------------ #

    async def list_projects(self) -> list[dict]:
        """List all projects OpenCode knows about."""
        return await self._get("/project")

    async def get_project(self, directory: str) -> dict | None:
        """Find a project by its worktree directory."""
        projects = await self.list_projects()
        for p in projects:
            if p.get("worktree") == directory:
                return p
        return None

    # ------------------------------------------------------------------ #
    #  Sessions                                                            #
    # ------------------------------------------------------------------ #

    def _get_session_directories_from_db(self) -> list[str]:
        """
        Read unique session directories directly from OpenCode's SQLite database.
        This is needed because the API only matches exact directories, but sessions
        can be created in subdirectories of a project worktree (e.g. valletpv/django).
        """
        db_path = Path.home() / ".local" / "share" / "opencode" / "opencode.db"
        try:
            conn = sqlite3.connect(str(db_path))
            rows = conn.execute(
                "SELECT DISTINCT directory FROM session WHERE time_archived IS NULL ORDER BY time_updated DESC"
            ).fetchall()
            conn.close()
            return [r[0] for r in rows if r[0]]
        except Exception as exc:
            logger.warning(f"Could not read OpenCode DB for directories: {exc}")
            return []

    async def list_sessions(self, directory: str | None = None, roots: bool = False) -> list[dict]:
        """
        List sessions. If directory is given, scoped to that project.
        If roots=True, only returns top-level sessions (no children/forks).
        If not, reads all unique session directories from OpenCode's SQLite DB
        and queries the API for each one — needed because sessions can be in
        subdirectories of a project worktree and the API matches exact directory.
        """
        if directory:
            path = "/session?roots=true" if roots else "/session"
            return await self._get(path, directory=directory)

        # Get all unique directories that have sessions from the DB
        dirs = await asyncio.get_event_loop().run_in_executor(
            None, self._get_session_directories_from_db
        )

        all_sessions: list[dict] = []
        seen_ids: set[str] = set()
        sess_path = "/session?roots=true" if roots else "/session"

        results = await asyncio.gather(
            *[self._get(sess_path, directory=d) for d in dirs],
            return_exceptions=True,
        )
        for d, result in zip(dirs, results):
            if isinstance(result, Exception):
                continue
            for s in result:
                sid = s.get("id", "")
                if sid and sid not in seen_ids:
                    seen_ids.add(sid)
                    effective_dir = s.get("directory") or d
                    s = {**s, "_worktree": effective_dir}
                    all_sessions.append(s)

        # Fallback: bare call in case DB is unavailable
        if not all_sessions:
            try:
                bare = await self._get(sess_path)
                for s in bare:
                    sid = s.get("id", "")
                    if sid and sid not in seen_ids:
                        seen_ids.add(sid)
                        all_sessions.append(s)
            except Exception:
                pass

        return all_sessions

    async def get_session_children(self, session_id: str, directory: str | None = None) -> list[dict]:
        """Return child sessions of a given session."""
        try:
            return await self._get(f"/session/{session_id}/children", directory=directory)
        except Exception:
            return []

    async def get_session(self, session_id: str, directory: str | None = None) -> dict:
        return await self._get(f"/session/{session_id}", directory=directory)

    async def create_session(
        self,
        directory: str,
        provider_id: str | None = None,
        model_id: str | None = None,
        title: str | None = None,
    ) -> dict:
        body: dict = {}
        if title:
            body["title"] = title
        if provider_id and model_id:
            body["model"] = {"providerID": provider_id, "id": model_id}
        return await self._post("/session", body, directory=directory)

    async def delete_session(self, session_id: str, directory: str | None = None) -> Any:
        return await self._delete(f"/session/{session_id}", directory=directory)

    async def update_session(self, session_id: str, directory: str | None = None, **kwargs) -> dict:
        return await self._patch(f"/session/{session_id}", kwargs, directory=directory)

    async def abort_session(self, session_id: str, directory: str | None = None) -> Any:
        return await self._post(f"/session/{session_id}/abort", {}, directory=directory)

    async def respond_permission(
        self,
        session_id: str,
        permission_id: str,
        response: str,
        remember: bool = False,
        directory: str | None = None,
    ) -> Any:
        body = {"response": response, "remember": remember}
        return await self._post(f"/session/{session_id}/permissions/{permission_id}", body, directory=directory)

    # ------------------------------------------------------------------ #
    #  Question tool                                                       #
    # ------------------------------------------------------------------ #

    async def list_questions(self, directory: str | None = None) -> list[dict]:
        """List all pending question requests."""
        return await self._get("/question", directory=directory)

    async def reply_question(self, request_id: str, answers: list[list[str]], directory: str | None = None) -> Any:
        """Reply to a question request. answers is a list per question, each a list of selected labels."""
        return await self._post(f"/question/{request_id}/reply", {"answers": answers}, directory=directory)

    async def reject_question(self, request_id: str, directory: str | None = None) -> Any:
        """Reject a question request (dismiss)."""
        return await self._post(f"/question/{request_id}/reject", {}, directory=directory)

    # ------------------------------------------------------------------ #
    #  Messages                                                            #
    # ------------------------------------------------------------------ #

    async def get_messages(self, session_id: str, directory: str | None = None) -> list[dict]:
        return await self._get(f"/session/{session_id}/message", directory=directory)

    async def send_message_async(
        self,
        session_id: str,
        text: str,
        directory: str | None = None,
        provider_id: str | None = None,
        model_id: str | None = None,
    ) -> Any:
        """Fire-and-forget prompt. Response comes via SSE."""
        body: dict = {"parts": [{"type": "text", "text": text}]}
        if provider_id and model_id:
            body["model"] = {"providerID": provider_id, "modelID": model_id}
        return await self._post(f"/session/{session_id}/prompt_async", body, directory=directory)

    # ------------------------------------------------------------------ #
    #  Models                                                              #
    # ------------------------------------------------------------------ #

    async def list_models(self) -> list[dict]:
        """Return flat list of models from connected providers only."""
        data = await self._get("/provider")
        connected = set(data.get("connected", []))
        models = []
        for provider in data.get("all", []):
            pid = provider.get("id", "")
            if connected and pid not in connected:
                continue
            for mid, model in provider.get("models", {}).items():
                models.append({**model, "providerID": pid, "id": mid})
        return models

    async def get_model_context_limit(self, provider_id: str, model_id: str) -> int | None:
        """Return context window size (tokens) for a given model, or None if unknown."""
        try:
            data = await self._get("/provider")
            for provider in data.get("all", []):
                if provider.get("id") == provider_id:
                    models = provider.get("models", {})
                    model = models.get(model_id, {})
                    return model.get("limit", {}).get("context")
        except Exception:
            pass
        return None

    # ------------------------------------------------------------------ #
    #  SSE event stream                                                    #
    # ------------------------------------------------------------------ #

    async def event_stream(self) -> AsyncIterator[dict]:
        """
        Global SSE stream — receives events for ALL projects/sessions.
        Each event contains `properties.sessionID` and the session's `directory`
        so the bot can route events to the right project.
        Reconnects automatically with exponential backoff.
        """
        retry_delay = 2
        max_buffer  = 1024 * 1024  # 1 MB

        while True:
            try:
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

                            if len(buffer) > max_buffer:
                                logger.error("SSE buffer overflow, resetting")
                                buffer = b""
                                continue

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
