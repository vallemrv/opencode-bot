"""Group uploaded files after a quiet period, keeping their original destination."""

import asyncio
import json
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from collections.abc import Callable, Coroutine, Hashable
from typing import Any

UPLOAD_DELAY = 5.0


def upload_path(root: Path, filename: str) -> Path:
    """Reserve a private directory so repeated names never overwrite files."""
    root.mkdir(parents=True, exist_ok=True)
    name = Path(filename).name
    if name in ("", ".", ".."):
        name = "archivo"
    return Path(tempfile.mkdtemp(dir=root)) / name


def upload_prompt(files: list[dict]) -> str:
    return (
        "He subido los siguientes archivos por Telegram. Están guardados en este "
        "servidor; puedes abrirlos usando su ruta local. Revisa los archivos y "
        "atiende los comentarios adjuntos a cada uno. Si no hay una petición "
        "concreta, describe brevemente lo recibido y pregunta qué necesito.\n\n"
        + json.dumps(files, ensure_ascii=False, indent=2)
    )


@dataclass
class UploadBatch:
    deliver: Callable[[str], Coroutine[Any, Any, None]]
    files: list[dict] = field(default_factory=list)
    pending: int = 0
    timer: asyncio.TimerHandle | None = None


class UploadBatcher:
    def __init__(self, create_task, delay: float = UPLOAD_DELAY):
        self.create_task = create_task
        self.delay = delay
        self.batches: dict[Hashable, UploadBatch] = {}

    def begin(self, key: Hashable, deliver) -> UploadBatch:
        batch = self.batches.setdefault(key, UploadBatch(deliver))
        if batch.timer:
            batch.timer.cancel()
            batch.timer = None
        batch.pending += 1
        return batch

    def finish(self, key: Hashable, batch: UploadBatch, path: Path | None,
               caption: str = "") -> None:
        if path is not None:
            batch.files.append({"ruta": str(path), "url": path.as_uri(),
                                "comentario": caption})
        batch.pending -= 1
        if batch.pending:
            return
        if not batch.files:
            self.batches.pop(key, None)
            return
        batch.timer = asyncio.get_running_loop().call_later(
            self.delay, self._flush, key, batch)

    def _flush(self, key: Hashable, batch: UploadBatch) -> None:
        self.batches.pop(key, None)
        # Only the waiting timer is cancelled by a new upload, never delivery.
        self.create_task(batch.deliver(upload_prompt(batch.files)))
