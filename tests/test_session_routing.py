import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from session_routing import interaction_directory


class RoutingTest(unittest.IsolatedAsyncioTestCase):
    async def test_no_usa_la_sesion_activa_para_preguntas_ajenas(self):
        app = SimpleNamespace(bot_data={})
        db = SimpleNamespace(get_active=AsyncMock(return_value={
            "session_id": "ses_telegram", "directory": "/telegram",
        }))
        oc = SimpleNamespace(get_session=AsyncMock(return_value={}))
        self.assertIsNone(await interaction_directory(app, "ses_valle", db, oc))
        self.assertEqual(await interaction_directory(app, "ses_telegram", db, oc), "/telegram")

    async def test_subsesion_hereda_solo_del_padre_reconocido(self):
        app = SimpleNamespace(bot_data={"statuses": {"ses_parent": {"directory": "/telegram"}}})
        db = SimpleNamespace(get_active=AsyncMock(return_value=None))
        oc = SimpleNamespace(get_session=AsyncMock(return_value={"parentID": "ses_parent"}))
        self.assertEqual(await interaction_directory(app, "ses_child", db, oc), "/telegram")
        app.bot_data.clear()
        self.assertIsNone(await interaction_directory(app, "ses_child", db, oc))

    async def test_valle_tiene_prioridad_aunque_telegram_tenga_la_sesion_abierta(self):
        with tempfile.TemporaryDirectory() as folder, patch.dict(os.environ, {"XDG_DATA_HOME": folder}):
            path = Path(folder) / "valle-bots/opencode/routes/ses_valle.json"
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps({"owner": "valle", "expires": time.time() + 60}))
            app = SimpleNamespace(bot_data={"statuses": {"ses_valle": {"directory": "/same"}}})
            db = SimpleNamespace(get_active=AsyncMock(return_value=None))
            oc = SimpleNamespace(get_session=AsyncMock(return_value={}))
            self.assertIsNone(await interaction_directory(app, "ses_valle", db, oc))
            self.assertFalse(oc.get_session.called)
            path.unlink()
            self.assertEqual(await interaction_directory(app, "ses_valle", db, oc), "/same")
