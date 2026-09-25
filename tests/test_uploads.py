import asyncio
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
os.environ["TELEGRAM_BOT_TOKEN"] = "123:test"
os.environ["TELEGRAM_ADMIN_ID"] = "1"

from upload_batch import UploadBatcher, upload_path, upload_prompt
import telegram_bot as bot

IS_OPENCODE = hasattr(bot, "handle_file_upload")


class BatchTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tasks = []
        self.deliver = AsyncMock()
        self.batcher = UploadBatcher(self.create_task, delay=0.06)

    def create_task(self, coro):
        task = asyncio.create_task(coro)
        self.tasks.append(task)
        return task

    async def asyncTearDown(self):
        for batch in self.batcher.batches.values():
            if batch.timer:
                batch.timer.cancel()
        await asyncio.gather(*self.tasks)

    def add(self, key="a", path="/tmp/photo.jpg", caption="", deliver=None):
        batch = self.batcher.begin(key, deliver or self.deliver)
        self.batcher.finish(key, batch, Path(path), caption)

    async def test_timer_restarts_and_preserves_every_caption(self):
        self.add(caption="Primera *foto*\nCon detalle")
        await asyncio.sleep(0.04)
        self.add(path="/tmp/informe.pdf", caption="Compara con la foto")
        await asyncio.sleep(0.04)
        self.deliver.assert_not_awaited()
        await asyncio.sleep(0.04)
        self.deliver.assert_awaited_once()
        files = json.loads(self.deliver.call_args.args[0].split("\n\n", 1)[1])
        self.assertEqual(len(files), 2)
        self.assertEqual(files[0]["comentario"], "Primera *foto*\nCon detalle")
        self.assertEqual(files[1]["url"], "file:///tmp/informe.pdf")

    async def test_download_in_progress_holds_batch(self):
        self.add()
        pending = self.batcher.begin("a", self.deliver)
        await asyncio.sleep(0.09)
        self.deliver.assert_not_awaited()
        self.batcher.finish("a", pending, Path("/tmp/slow.pdf"))
        await asyncio.sleep(0.09)
        self.deliver.assert_awaited_once()

    async def test_failed_download_does_not_lose_previous_files(self):
        self.add()
        pending = self.batcher.begin("a", self.deliver)
        self.batcher.finish("a", pending, None)
        await asyncio.sleep(0.09)
        self.deliver.assert_awaited_once()
        self.assertNotIn("null", self.deliver.call_args.args[0])

    async def test_only_failed_downloads_do_not_notify(self):
        batch = self.batcher.begin("a", self.deliver)
        self.batcher.finish("a", batch, None)
        await asyncio.sleep(0.09)
        self.deliver.assert_not_awaited()
        self.assertFalse(self.batcher.batches)

    async def test_destinations_are_independent(self):
        other = AsyncMock()
        self.add()
        self.add(key="b", path="/tmp/other.pdf", deliver=other)
        await asyncio.sleep(0.09)
        self.deliver.assert_awaited_once()
        other.assert_awaited_once()
        self.assertNotIn("other.pdf", self.deliver.call_args.args[0])

    async def test_new_upload_does_not_cancel_delivery_in_progress(self):
        started, release = asyncio.Event(), asyncio.Event()

        async def slow_deliver(text):
            started.set()
            await release.wait()

        self.add(deliver=slow_deliver)
        await asyncio.wait_for(started.wait(), 1)
        self.add(path="/tmp/second.jpg")
        await asyncio.sleep(0.09)
        self.assertFalse(self.tasks[0].cancelled())
        self.deliver.assert_awaited_once()
        release.set()

    def test_names_are_safe_and_unique_and_urls_are_encoded(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = [upload_path(Path(tmp), "../../foto #1.jpg") for _ in range(3)]
            self.assertEqual(len(set(paths)), 3)
            for path in paths:
                self.assertTrue(path.is_relative_to(tmp))
                self.assertEqual(path.name, "foto #1.jpg")
            prompt = upload_prompt([{"url": paths[0].as_uri()}])
            self.assertIn("foto%20%231.jpg", prompt)


class HandlerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.active = {"directory": "/project/a", "session_id": "session-a",
                       "claude_session_id": "session-a", "model": "test-model",
                       "effort": "high"}
        get_active = AsyncMock if IS_OPENCODE else Mock
        self.get_active = get_active(side_effect=lambda: dict(self.active) if self.active else None)
        self.actual_dispatch = bot._dispatch_prompt if IS_OPENCODE else bot._dispatch
        self.dispatch = AsyncMock()
        self.tasks = []
        self.batcher = UploadBatcher(self.create_task, delay=0.04)
        self.download = AsyncMock(side_effect=lambda path: path.write_bytes(b"test file"))
        self.ctx = SimpleNamespace(
            bot_data={"upload_batcher": self.batcher},
            bot=SimpleNamespace(get_file=AsyncMock(return_value=SimpleNamespace(
                download_to_drive=self.download))),
            application=SimpleNamespace(create_task=self.create_task),
        )
        self.ctx.application.bot_data = self.ctx.bot_data
        for target, replacement in [
            ("TMP_DIR", Path(self.tmp.name)),
            ("_dispatch_prompt" if IS_OPENCODE else "_dispatch", self.dispatch),
            ("_track_msg", Mock()),
        ]:
            p = patch.object(bot, target, replacement)
            p.start()
            self.addCleanup(p.stop)
        p = patch.object(bot.db, "get_active", self.get_active)
        p.start()
        self.addCleanup(p.stop)

    def create_task(self, coro):
        task = asyncio.create_task(coro)
        self.tasks.append(task)
        return task

    async def asyncTearDown(self):
        for batch in self.batcher.batches.values():
            if batch.timer:
                batch.timer.cancel()
        await asyncio.gather(*self.tasks)

    async def upload(self, kind="photo", caption=None, user=1):
        media = SimpleNamespace(file_id="file", file_name="same_[name].pdf")
        msg = SimpleNamespace(
            chat_id=1, document=media if kind == "document" else None,
            photo=[media] if kind == "photo" else [],
            video=media if kind == "video" else None, caption=caption,
            reply_text=AsyncMock(return_value=SimpleNamespace(message_id=123)))
        update = SimpleNamespace(message=msg, effective_user=SimpleNamespace(id=user))
        handler = bot.handle_file_upload if IS_OPENCODE else bot.handle_file
        await handler(update, self.ctx)
        return msg

    async def test_mixed_album_sends_once_to_captured_session(self):
        await self.upload(caption="Mira esta imagen")
        await self.upload("document", "Revisa *esto* y _esto_")
        await self.upload("video")
        self.active.update(session_id="session-b", claude_session_id="session-b",
                           directory="/project/b")
        await asyncio.sleep(0.07)
        self.dispatch.assert_awaited_once()
        args = self.dispatch.call_args.args
        self.assertIn("session-a", args)
        self.assertIn("/project/a", args)
        prompt = args[-1] if IS_OPENCODE else args[3]
        files = json.loads(prompt.split("\n\n", 1)[1])
        self.assertEqual(len(files), 3)
        self.assertEqual(files[1]["comentario"], "Revisa *esto* y _esto_")
        self.assertEqual(files[2]["comentario"], "")
        for item in files:
            self.assertEqual(Path(item["ruta"]).read_bytes(), b"test file")

    async def test_busy_session_queues_the_whole_batch(self):
        if IS_OPENCODE:
            self.ctx.bot_data["statuses"] = {"session-a": {"state": "busy"}}
            p = patch.object(bot, "_session_actually_idle", AsyncMock(return_value=False))
            p.start()
            self.addCleanup(p.stop)
        else:
            for p in [patch.dict(bot.STATUSES, {"session-a": {"state": "busy"}}, clear=True),
                      patch.dict(bot.QUEUES, {}, clear=True),
                      patch.object(bot, "_safe_send", AsyncMock(return_value=None)),
                      patch.object(bot, "_queue_msg_text", Mock(return_value="queued")),
                      patch.object(bot, "_key", Mock(return_value=1))]:
                p.start()
                self.addCleanup(p.stop)
        self.dispatch.side_effect = self.actual_dispatch
        await self.upload(caption="Revisa las dos")
        await self.upload("document")
        await asyncio.sleep(0.07)
        queues = self.ctx.bot_data["queues"] if IS_OPENCODE else bot.QUEUES
        self.assertEqual(len(queues["session-a"]), 1)
        prompt = queues["session-a"][0]["text"]
        self.assertEqual(len(json.loads(prompt.split("\n\n", 1)[1])), 2)

    async def test_repeated_documents_are_not_overwritten(self):
        for _ in range(3):
            await self.upload("document")
        paths = [call.args[0] for call in self.download.call_args_list]
        self.assertEqual(len(set(paths)), 3)

    async def test_no_active_session_does_not_download_or_send(self):
        self.active = None
        msg = await self.upload()
        self.ctx.bot.get_file.assert_not_awaited()
        self.assertIn("/open", msg.reply_text.call_args.args[0])
        self.assertFalse(self.batcher.batches)

    async def test_unauthorized_upload_is_ignored(self):
        await self.upload(user=999)
        self.ctx.bot.get_file.assert_not_awaited()
        self.assertFalse(self.batcher.batches)

    async def test_download_error_is_not_reported_as_saved(self):
        self.download.side_effect = RuntimeError("download failed")
        msg = await self.upload()
        self.assertIn("Error al guardar", msg.reply_text.call_args.args[0])
        self.assertFalse(self.batcher.batches)
        self.assertEqual(list(Path(self.tmp.name).iterdir()), [])


if __name__ == "__main__":
    unittest.main()
