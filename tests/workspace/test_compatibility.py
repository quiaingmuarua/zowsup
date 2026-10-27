"""Explicit offline checks for the workspace fork; no pytest account discovery.

Run from the isolated smoke container with:
    python -B tests/workspace/test_compatibility.py
Only synthetic objects/data and temporary files are used. No account is opened.
"""

import asyncio
import contextlib
import io
import logging
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))


def reject_network(event, args):
    if event in {"socket.connect", "socket.getaddrinfo"}:
        raise AssertionError("Offline workspace test attempted network access")


sys.addaudithook(reject_network)

from app.layer.pairing import PairingManager
from app.layer.zowbot_layer import ZowBotLayer
from app.zowbot import ZowBot
from app.zowbot_values import ZowBotType
from common.utils import Utils
from conf.constants import SysVar
from conf.logging_config import setup_logging
from core.layers.protocol_iq.protocolentities.wmex.iq_wmex_query import WmexQueryIqProtocolEntity
from core.layers.protocol_iq.protocolentities.wmex.iq_wmex_result import WmexResultIqProtocolEntity
from core.structs import ProtocolTreeNode
from core.layers.network.dispatcher.dispatcher_asyncio import AsyncioConnectionDispatcher
from zargo.argo_message_decoder import ArgoMessageDecoder


class ResourceLocationTests(unittest.TestCase):
    def test_protocol_resources_load_outside_the_checkout(self):
        with tempfile.TemporaryDirectory(prefix="zowsup-resources-") as directory:
            with contextlib.chdir(directory):
                self.assertEqual(Utils.getMobileCC("12025550123"), "1")
                with mock.patch.object(WmexQueryIqProtocolEntity, "_queryIdMap", None):
                    WmexQueryIqProtocolEntity.loadDict()
                    self.assertTrue(WmexQueryIqProtocolEntity._queryIdMap)

                node = ProtocolTreeNode(
                    "iq", {"id": "offline-schema", "type": "result"},
                    children=[ProtocolTreeNode("result", {"format": "argo"}, data=b"synthetic")],
                )

                def decode_synthetic(query_name, data):
                    self.assertEqual(query_name, "OfflineQuery")
                    self.assertEqual(data, b"synthetic")
                    expected = ROOT / "data" / "whatsapp-android-mex_argo_wire_types.argo"
                    self.assertEqual(Path(ArgoMessageDecoder._schemaFile), expected)
                    ArgoMessageDecoder.loadSchemaFile()
                    self.assertTrue(ArgoMessageDecoder._schemaStore)
                    return {"offline": True}

                with (
                    mock.patch.object(WmexResultIqProtocolEntity, "idNameMap", {"offline-schema": "OfflineQuery"}),
                    mock.patch.object(ArgoMessageDecoder, "_schemaFile", None),
                    mock.patch.object(ArgoMessageDecoder, "_schemaStore", None),
                    mock.patch.object(ArgoMessageDecoder, "decodeMessage", side_effect=decode_synthetic),
                ):
                    entity = WmexResultIqProtocolEntity.fromProtocolTreeNode(node)
                    self.assertEqual(entity.result_obj, {"offline": True})
                self.assertEqual(list(Path(directory).iterdir()), [])


class LoggingLocationTests(unittest.TestCase):
    def test_file_logging_uses_configured_directory_only_when_enabled(self):
        root_logger = logging.getLogger()
        original_handlers = root_logger.handlers[:]
        original_level = root_logger.level
        root_logger.handlers.clear()
        try:
            with tempfile.TemporaryDirectory(prefix="zowsup-logging-") as directory:
                root = Path(directory)
                log_path = root / "runtime" / "logs"
                with (
                    contextlib.chdir(root),
                    contextlib.redirect_stdout(io.StringIO()),
                    mock.patch.object(SysVar, "LOG_PATH", str(log_path), create=True),
                ):
                    setup_logging(enable_file=False)
                    self.assertFalse(log_path.exists())
                    setup_logging(enable_file=True, log_file_name="offline.log")
                    root_logger.info("synthetic log entry")
                    for handler in root_logger.handlers:
                        handler.flush()
                    self.assertIn("synthetic log entry", (log_path / "offline.log").read_text(encoding="utf-8"))
                    self.assertFalse((root / "logs").exists())
                    # Release the file before TemporaryDirectory cleanup on Windows.
                    setup_logging(enable_file=False)
        finally:
            for handler in root_logger.handlers[:]:
                root_logger.removeHandler(handler)
                handler.close()
            root_logger.handlers[:] = original_handlers
            root_logger.setLevel(original_level)


class ConfigEncodingTests(unittest.TestCase):
    def test_bom_config_loads_with_only_temporary_runtime_directories(self):
        with tempfile.TemporaryDirectory(prefix="zowsup-config-") as directory:
            root = Path(directory)
            values = {"PLATFORM": "windows" if sys.platform == "win32" else "linux",
                      "PYTHON": sys.executable, "DEFAULT_ENV": "android"}
            for key, name in (
                ("ACCOUNT_PATH", "account"), ("TMP_ACCOUNT_PATH", "tmp-account"),
                ("DOWNLOAD_PATH", "download"), ("UPLOAD_PATH", "upload"),
                ("LOG_PATH", "logs"), ("TASK_PATH", "task"),
            ):
                values[key] = (root / "runtime" / name).as_posix() + "/"
            config = root / "with-bom.conf"
            config.write_text(
                "[SysVar]\n" + "".join(f"{key}={value}\n" for key, value in values.items()),
                encoding="utf-8-sig",
            )
            self.assertTrue(config.read_bytes().startswith(b"\xef\xbb\xbf"))
            self.assertFalse((root / "runtime").exists())
            restored_names = {*values, "BASE_URL", "HTTP_URL", "CMD_WAIT"}
            with mock.patch.multiple(SysVar, create=True, **{name: None for name in restored_names}):
                self.assertTrue(SysVar.loadConfig(str(config)))
                for key, value in values.items():
                    self.assertEqual(getattr(SysVar, key), value)
                self.assertTrue((root / "runtime" / "account" / "device").is_dir())
                self.assertTrue((root / "runtime" / "logs").is_dir())


class PairCodeHookTests(unittest.IsolatedAsyncioTestCase):
    def manager(self, code=None, bot_type=ZowBotType.TYPE_RUN_SINGLETON):
        layer = SimpleNamespace(
            bot=SimpleNamespace(bot_type=bot_type, workspace_pair_code=code),
            getStack=lambda: None,
            executeCommand=mock.AsyncMock(),
        )
        return PairingManager(layer), layer

    async def test_default_does_not_submit_a_code(self):
        manager, layer = self.manager()
        await manager._handle_linkcode_companion_hello(object())
        self.assertEqual(manager.status, "WAIT_PAIRINGCODE")
        layer.executeCommand.assert_not_awaited()

    async def test_code_runs_in_existing_loop_and_duplicates_are_ignored(self):
        manager, layer = self.manager("TESTCODE")
        started, release = asyncio.Event(), asyncio.Event()
        current_loop = asyncio.get_running_loop()

        async def execute(command, params):
            self.assertIs(asyncio.get_running_loop(), current_loop)
            started.set()
            await release.wait()

        layer.executeCommand.side_effect = execute
        first_entity = object()
        await manager._handle_linkcode_companion_hello(first_entity)
        task = manager._workspace_pair_code_task
        try:
            await asyncio.wait_for(started.wait(), timeout=1)
            self.assertFalse(task.done())
            await asyncio.wait_for(manager._handle_linkcode_companion_hello(object()), timeout=1)
            self.assertIs(manager.companion_hello_entity, first_entity)
        finally:
            release.set()
            await asyncio.wait_for(task, timeout=1)
        layer.executeCommand.assert_awaited_once_with("md.inputcode", ["TESTCODE"])

    async def test_failed_attempt_is_not_retried_by_another_notification(self):
        manager, layer = self.manager("TESTCODE")
        layer.executeCommand.side_effect = RuntimeError("synthetic TESTCODE failure")
        with self.assertLogs("app.layer.pairing", level="ERROR") as captured:
            await manager._handle_linkcode_companion_hello(object())
            await manager._workspace_pair_code_task
        self.assertNotIn("TESTCODE", "\n".join(captured.output))
        layer.bot.workspace_pair_code = "NEXTCODE"
        await manager._handle_linkcode_companion_hello(object())
        layer.executeCommand.assert_awaited_once()

    async def test_sequential_reader_can_receive_the_iq_reply(self):
        """The real dispatcher cannot read frame 2 until frame 1's handler returns."""
        manager, layer = self.manager("TESTCODE")
        command_started = asyncio.Event()
        iq_reply = asyncio.get_running_loop().create_future()
        received = []

        async def execute(command, params):
            command_started.set()
            await iq_reply

        async def receive(data):
            received.append(data)
            if data == b"companion_hello":
                await manager._handle_linkcode_companion_hello(object())
            elif data == b"iq_result":
                iq_reply.set_result({"status": "ok"})

        class Reader:
            calls = 0

            async def read(self, size):
                self.calls += 1
                if self.calls == 1:
                    return b"companion_hello"
                if self.calls == 2:
                    await command_started.wait()
                    return b"iq_result"
                return b""

        layer.executeCommand.side_effect = execute
        callbacks = SimpleNamespace(
            getStack=lambda: SimpleNamespace(getProp=lambda key: SimpleNamespace(networkEnv=None)),
            onRecvData=receive,
            onDisconnected=mock.AsyncMock(),
        )
        dispatcher = AsyncioConnectionDispatcher(callbacks)
        dispatcher._reader = Reader()
        try:
            await asyncio.wait_for(dispatcher._read_loop(), timeout=1)
            self.assertEqual(received, [b"companion_hello", b"iq_result"])
            await asyncio.wait_for(manager._workspace_pair_code_task, timeout=1)
            layer.executeCommand.assert_awaited_once_with("md.inputcode", ["TESTCODE"])
        finally:
            await manager.stop_workspace_pairing()

    async def test_bot_shutdown_cancels_and_joins_pending_pairing(self):
        manager, layer = self.manager("TESTCODE")
        started, cancelled = asyncio.Event(), asyncio.Event()

        async def execute(command, params):
            started.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                cancelled.set()
                raise

        async def idle_commands():
            await asyncio.Future()

        layer.executeCommand.side_effect = execute
        await manager._handle_linkcode_companion_hello(object())
        task = manager._workspace_pair_code_task
        await asyncio.wait_for(started.wait(), timeout=1)
        bot = SimpleNamespace(
            botId=None,
            _command_queue=None,
            _command_runner_loop=idle_commands,
            _cmd_args_for_exec=None,
            _exit_code=None,
            _stack=SimpleNamespace(broadcastEvent=mock.AsyncMock(), loop=mock.AsyncMock()),
            botLayer=SimpleNamespace(pairing=manager, getProp=lambda key: key == "QUITTED"),
        )
        await asyncio.wait_for(ZowBot._async_run(bot), timeout=1)
        self.assertTrue(cancelled.is_set())
        self.assertTrue(task.cancelled())
        self.assertIsNone(manager._workspace_pair_code_task)
        await manager._handle_linkcode_companion_hello(object())
        self.assertIsNone(manager._workspace_pair_code_task)
        layer.executeCommand.assert_awaited_once()

    async def test_invalid_explicit_value_fails_without_running_a_command(self):
        for code in ("", "   ", 123):
            manager, layer = self.manager(code)
            with self.assertRaisesRegex(ValueError, "non-empty string"):
                await manager._handle_linkcode_companion_hello(object())
            layer.executeCommand.assert_not_awaited()

    async def test_temporary_mode_keeps_its_upstream_behavior(self):
        manager, layer = self.manager("TESTCODE", ZowBotType.TYPE_RUN_TEMP)
        await manager._handle_linkcode_companion_hello(object())
        await manager._handle_linkcode_companion_hello(object())
        self.assertEqual(layer.executeCommand.await_args_list, [
            mock.call("md.inputcode", ["AAAAAA"]),
            mock.call("md.inputcode", ["AAAAAA"]),
        ])


class PairCodeLoggingTests(unittest.IsolatedAsyncioTestCase):
    async def test_success_logs_omit_pairing_parameters_and_results(self):
        logger = logging.getLogger("workspace-test-pair-code")
        execute = mock.AsyncMock(return_value={"echo": "TESTCODE"})
        layer = SimpleNamespace(bot=SimpleNamespace(cmdList={"md.inputcode": execute}), logger=logger)
        with self.assertLogs(logger, level="DEBUG") as captured:
            result = await ZowBotLayer.executeCommand(layer, "md.inputcode", ["TESTCODE"])
        self.assertEqual(result, {"echo": "TESTCODE"})
        self.assertNotIn("TESTCODE", "\n".join(captured.output))

    async def test_failure_logs_omit_exception_payload(self):
        logger = logging.getLogger("workspace-test-pair-code")
        execute = mock.AsyncMock(side_effect=RuntimeError("synthetic TESTCODE detail"))
        layer = SimpleNamespace(bot=SimpleNamespace(cmdList={"md.inputcode": execute}), logger=logger)
        with self.assertLogs(logger, level="DEBUG") as captured:
            with self.assertRaises(RuntimeError):
                await ZowBotLayer.executeCommand(layer, "md.inputcode", ["TESTCODE"])
        self.assertNotIn("TESTCODE", "\n".join(captured.output))
        self.assertIn("RuntimeError", "\n".join(captured.output))


if __name__ == "__main__":
    unittest.main(verbosity=2)
