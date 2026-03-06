import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
SERVER_DIR = ROOT / "server"
if str(SERVER_DIR) not in sys.path:
    sys.path.insert(0, str(SERVER_DIR))

import app as app_module  # noqa: E402
import db  # noqa: E402


class _FakeUpstreamResponse:
    def __init__(self, *, status_code: int, lines: list[str] | None = None, body: str = ""):
        self.status_code = status_code
        self._lines = list(lines or [])
        self._body = body

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def aiter_lines(self):
        for line in self._lines:
            yield line

    async def aread(self) -> bytes:
        return self._body.encode("utf-8")


class _FakeAsyncClient:
    response_factory = None
    get_factory = None
    post_factory = None
    captured: dict = {}

    def __init__(self, *args, **kwargs):
        _FakeAsyncClient.captured = {"timeout": kwargs.get("timeout")}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def stream(self, method: str, url: str, json: dict | None = None, headers: dict | None = None):
        _FakeAsyncClient.captured.update(
            {
                "method": method,
                "url": url,
                "json": json or {},
                "headers": headers or {},
            }
        )
        factory = _FakeAsyncClient.response_factory
        if not callable(factory):
            return _FakeUpstreamResponse(status_code=200, lines=['data: {"choices":[{"delta":{"content":"ok"},"finish_reason":"stop"}]}'])
        return factory(method=method, url=url, json=json, headers=headers)

    async def get(self, url: str, headers: dict | None = None, follow_redirects: bool = False):
        _FakeAsyncClient.captured.update(
            {
                "get_url": url,
                "get_headers": headers or {},
                "follow_redirects": bool(follow_redirects),
            }
        )
        factory = _FakeAsyncClient.get_factory
        if callable(factory):
            return factory(url=url, headers=headers, follow_redirects=follow_redirects)
        return _FakeHttpResponse(status_code=200, payload={"data": [{"id": app_module.SUPPORT_LLM_MODEL}]})

    async def post(self, url: str, json: dict | None = None, headers: dict | None = None):
        _FakeAsyncClient.captured.update(
            {
                "post_url": url,
                "post_json": json or {},
                "post_headers": headers or {},
            }
        )
        factory = _FakeAsyncClient.post_factory
        if callable(factory):
            return factory(url=url, json=json, headers=headers)
        return _FakeHttpResponse(
            status_code=200,
            payload={"choices": [{"message": {"content": "fallback text"}, "finish_reason": "stop"}]},
        )


class _FakeHttpResponse:
    def __init__(self, *, status_code: int = 200, payload: dict | None = None, text: str = ""):
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self):
        if self._payload is None:
            raise ValueError("invalid json")
        return self._payload


def _parse_sse(text: str) -> list[tuple[str, dict]]:
    events: list[tuple[str, dict]] = []
    blocks = str(text or "").replace("\r\n", "\n").split("\n\n")
    for block in blocks:
        rows = [line for line in block.split("\n") if line.strip()]
        if not rows:
            continue
        event = "message"
        data_lines: list[str] = []
        for row in rows:
            if row.startswith("event:"):
                event = row[6:].strip() or "message"
            elif row.startswith("data:"):
                data_lines.append(row[5:].strip())
        if not data_lines:
            continue
        payload = json.loads("\n".join(data_lines))
        events.append((event, payload))
    return events


class SupportChatStreamApiTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self._old_db_path = db.DB_PATH
        db.DB_PATH = Path(self._tmp.name) / "support-chat-test.db"
        db.init_db()
        _FakeAsyncClient.response_factory = None
        _FakeAsyncClient.get_factory = None
        _FakeAsyncClient.post_factory = None
        _FakeAsyncClient.captured = {}
        self.client = TestClient(app_module.app)
        setup = self.client.post("/auth/setup-admin", json={"password": "admin123"})
        self.assertEqual(setup.status_code, 200)
        self.headers = {"Authorization": f"Bearer {setup.json()['token']}"}

    def tearDown(self):
        self.client.close()
        db.DB_PATH = self._old_db_path
        self._tmp.cleanup()

    def test_support_chat_stream_success(self):
        def _factory(**_kwargs):
            return _FakeUpstreamResponse(
                status_code=200,
                lines=[
                    'data: {"choices":[{"delta":{"content":"你好"},"finish_reason":null}]}',
                    'data: {"choices":[{"delta":{"content":"，这里是客服。"},"finish_reason":null}]}',
                    'data: {"choices":[{"delta":{},"finish_reason":"stop"}]}',
                    "data: [DONE]",
                ],
            )

        _FakeAsyncClient.response_factory = _factory
        with mock.patch.object(app_module.httpx, "AsyncClient", _FakeAsyncClient):
            response = self.client.post(
                "/support/chat/stream",
                json={"messages": [{"role": "user", "content": "你好"}]},
                headers=self.headers,
            )

        self.assertEqual(response.status_code, 200)
        self.assertNotIn("httpx.Timeout must", response.text)
        events = _parse_sse(response.text)
        self.assertTrue(events)
        self.assertEqual(events[0][0], "ready")
        delta_text = "".join(payload.get("content", "") for evt, payload in events if evt == "delta")
        self.assertEqual(delta_text, "你好，这里是客服。")
        self.assertEqual(events[-1][0], "finish")

        timeout = _FakeAsyncClient.captured["timeout"]
        self.assertEqual(timeout.connect, app_module.SUPPORT_LLM_CONNECT_TIMEOUT_SECS)
        self.assertEqual(timeout.write, app_module.SUPPORT_LLM_WRITE_TIMEOUT_SECS)
        self.assertEqual(timeout.pool, app_module.SUPPORT_LLM_POOL_TIMEOUT_SECS)
        self.assertIsNone(timeout.read)
        self.assertEqual(_FakeAsyncClient.captured["method"], "POST")
        self.assertTrue(_FakeAsyncClient.captured["url"].endswith("/chat/completions"))
        payload = _FakeAsyncClient.captured["json"]
        self.assertEqual(payload["model"], app_module.SUPPORT_LLM_MODEL)
        self.assertEqual(payload["temperature"], 0.7)
        self.assertEqual(payload["top_p"], 0.8)
        self.assertEqual(payload["top_k"], 20)
        self.assertEqual(payload["min_p"], 0.0)
        self.assertEqual(payload["presence_penalty"], 1.5)
        self.assertEqual(payload["repetition_penalty"], 1.0)
        self.assertEqual(payload["max_tokens"], 1024)

    def test_support_system_prompt_includes_latest_platform_features(self):
        prompt = app_module.SUPPORT_SYSTEM_PROMPT

        self.assertIn("需求反馈清单", prompt)
        self.assertIn("AI自动排期中", prompt)
        self.assertIn("Patchset", prompt)
        self.assertIn("管理员账号不提供需求提交入口", prompt)
        self.assertIn("模型自检", prompt)

    def test_support_chat_stream_surfaces_upstream_error(self):
        def _factory(**_kwargs):
            return _FakeUpstreamResponse(status_code=503, body='{"error":"gateway down"}')

        _FakeAsyncClient.response_factory = _factory
        with mock.patch.object(app_module.httpx, "AsyncClient", _FakeAsyncClient):
            response = self.client.post(
                "/support/chat/stream",
                json={"messages": [{"role": "user", "content": "排查下"}]},
                headers=self.headers,
            )

        self.assertEqual(response.status_code, 200)
        events = _parse_sse(response.text)
        self.assertTrue(events)
        self.assertEqual(events[0][0], "error")
        self.assertIn("客服模型请求失败", events[0][1].get("message", ""))

    def test_support_chat_stream_supports_content_array_and_reasoning(self):
        def _factory(**_kwargs):
            return _FakeUpstreamResponse(
                status_code=200,
                lines=[
                    'data: {"choices":[{"delta":{"content":[{"type":"text","text":"你好"}]},"finish_reason":null}]}',
                    'data: {"choices":[{"delta":{"reasoning_content":"，补充说明"},"finish_reason":null}]}',
                    'data: {"choices":[{"delta":{},"finish_reason":"stop"}]}',
                ],
            )

        _FakeAsyncClient.response_factory = _factory
        with mock.patch.object(app_module.httpx, "AsyncClient", _FakeAsyncClient):
            response = self.client.post(
                "/support/chat/stream",
                json={"messages": [{"role": "user", "content": "你好"}]},
                headers=self.headers,
            )

        self.assertEqual(response.status_code, 200)
        events = _parse_sse(response.text)
        self.assertTrue(events)
        delta_text = "".join(payload.get("content", "") for evt, payload in events if evt == "delta")
        reasoning_text = "".join(payload.get("content", "") for evt, payload in events if evt == "reasoning")
        self.assertEqual(delta_text, "你好")
        self.assertEqual(reasoning_text, "，补充说明")

    def test_support_chat_stream_supports_reasoning_field(self):
        def _factory(**_kwargs):
            return _FakeUpstreamResponse(
                status_code=200,
                lines=[
                    'data: {"choices":[{"delta":{"reasoning":"先分析，"},"finish_reason":null}]}',
                    'data: {"choices":[{"delta":{"reasoning":"再回答。"},"finish_reason":"stop"}]}',
                ],
            )

        _FakeAsyncClient.response_factory = _factory
        with mock.patch.object(app_module.httpx, "AsyncClient", _FakeAsyncClient):
            response = self.client.post(
                "/support/chat/stream",
                json={"messages": [{"role": "user", "content": "你好"}]},
                headers=self.headers,
            )

        self.assertEqual(response.status_code, 200)
        events = _parse_sse(response.text)
        self.assertTrue(events)
        delta_text = "".join(payload.get("content", "") for evt, payload in events if evt == "delta")
        reasoning_text = "".join(payload.get("content", "") for evt, payload in events if evt == "reasoning")
        self.assertEqual(delta_text, "fallback text")
        self.assertEqual(reasoning_text, "先分析，再回答。")
        self.assertTrue(_FakeAsyncClient.captured.get("post_url", "").endswith("/chat/completions"))
        post_json = _FakeAsyncClient.captured.get("post_json", {})
        self.assertIs(post_json.get("stream"), False)
        self.assertEqual(post_json.get("chat_template_kwargs", {}).get("enable_thinking"), False)

    def test_support_chat_stream_supports_non_sse_json_line(self):
        def _factory(**_kwargs):
            return _FakeUpstreamResponse(
                status_code=200,
                lines=[
                    '{"id":"abc","choices":[{"message":{"content":"这是一次非SSE返回"},"finish_reason":"stop"}]}',
                ],
            )

        _FakeAsyncClient.response_factory = _factory
        with mock.patch.object(app_module.httpx, "AsyncClient", _FakeAsyncClient):
            response = self.client.post(
                "/support/chat/stream",
                json={"messages": [{"role": "user", "content": "test"}]},
                headers=self.headers,
            )

        self.assertEqual(response.status_code, 200)
        events = _parse_sse(response.text)
        self.assertTrue(events)
        delta_text = "".join(payload.get("content", "") for evt, payload in events if evt == "delta")
        self.assertIn("非SSE返回", delta_text)

    def test_support_chat_stream_supports_multiline_json_body(self):
        def _factory(**_kwargs):
            return _FakeUpstreamResponse(
                status_code=200,
                lines=[
                    "{",
                    '  "id": "abc",',
                    '  "choices": [{"message": {"content": "这是多行JSON返回"}, "finish_reason": "stop"}]',
                    "}",
                ],
            )

        _FakeAsyncClient.response_factory = _factory
        with mock.patch.object(app_module.httpx, "AsyncClient", _FakeAsyncClient):
            response = self.client.post(
                "/support/chat/stream",
                json={"messages": [{"role": "user", "content": "test"}]},
                headers=self.headers,
            )

        self.assertEqual(response.status_code, 200)
        events = _parse_sse(response.text)
        self.assertTrue(events)
        delta_text = "".join(payload.get("content", "") for evt, payload in events if evt == "delta")
        self.assertIn("多行JSON返回", delta_text)

    def test_support_chat_stream_falls_back_to_non_stream_when_stream_empty(self):
        def _factory(**_kwargs):
            return _FakeUpstreamResponse(
                status_code=200,
                lines=[
                    'data: {"choices":[{"delta":{},"finish_reason":"stop"}]}',
                    "data: [DONE]",
                ],
            )

        def _post_factory(**_kwargs):
            return _FakeHttpResponse(
                status_code=200,
                payload={"choices": [{"message": {"content": "补偿返回内容"}, "finish_reason": "stop"}]},
            )

        _FakeAsyncClient.response_factory = _factory
        _FakeAsyncClient.post_factory = _post_factory
        with mock.patch.object(app_module.httpx, "AsyncClient", _FakeAsyncClient):
            response = self.client.post(
                "/support/chat/stream",
                json={"messages": [{"role": "user", "content": "test"}]},
                headers=self.headers,
            )

        self.assertEqual(response.status_code, 200)
        events = _parse_sse(response.text)
        self.assertTrue(events)
        delta_text = "".join(payload.get("content", "") for evt, payload in events if evt == "delta")
        self.assertIn("补偿返回内容", delta_text)

    def test_support_chat_stream_rejects_empty_messages(self):
        response = self.client.post(
            "/support/chat/stream",
            json={"messages": []},
            headers=self.headers,
        )
        self.assertEqual(response.status_code, 422)

    def test_support_chat_health_ok(self):
        def _get_factory(**_kwargs):
            return _FakeHttpResponse(
                status_code=200,
                payload={"data": [{"id": app_module.SUPPORT_LLM_MODEL}, {"id": "other-model"}]},
            )

        _FakeAsyncClient.get_factory = _get_factory
        with mock.patch.object(app_module.httpx, "AsyncClient", _FakeAsyncClient):
            response = self.client.get("/runtime/support-chat/health", headers=self.headers)

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload.get("ok"))
        self.assertTrue(payload.get("gateway_ok"))
        self.assertTrue(payload.get("model_available"))
        self.assertIn("已发现目标模型", payload.get("detail", ""))

    def test_support_chat_health_gateway_ok_but_model_missing(self):
        def _get_factory(**_kwargs):
            return _FakeHttpResponse(status_code=200, payload={"data": [{"id": "foo-1"}, {"id": "foo-2"}]})

        _FakeAsyncClient.get_factory = _get_factory
        with mock.patch.object(app_module.httpx, "AsyncClient", _FakeAsyncClient):
            response = self.client.get("/runtime/support-chat/health", headers=self.headers)

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertFalse(payload.get("ok"))
        self.assertTrue(payload.get("gateway_ok"))
        self.assertFalse(payload.get("model_available"))
        self.assertIn("未发现目标模型", payload.get("detail", ""))

    def test_support_chat_health_gateway_unreachable(self):
        async def _raise_get(*_args, **_kwargs):
            raise RuntimeError("network down")

        class _FailingClient(_FakeAsyncClient):
            async def get(self, url: str, headers: dict | None = None, follow_redirects: bool = False):
                return await _raise_get(url, headers, follow_redirects)

        with mock.patch.object(app_module.httpx, "AsyncClient", _FailingClient):
            response = self.client.get("/runtime/support-chat/health", headers=self.headers)

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertFalse(payload.get("ok"))
        self.assertFalse(payload.get("gateway_ok"))
        self.assertIn("模型网关不可达", payload.get("detail", ""))


if __name__ == "__main__":
    unittest.main()
