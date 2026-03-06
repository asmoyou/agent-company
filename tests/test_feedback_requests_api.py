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


class _FakeHttpResponse:
    def __init__(self, *, status_code: int = 200, payload: dict | None = None, text: str = ""):
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self):
        if self._payload is None:
            raise ValueError("invalid json")
        return self._payload


class _FakeAsyncClient:
    post_factory = None
    captured: dict = {}

    def __init__(self, *args, **kwargs):
        _FakeAsyncClient.captured = {"timeout": kwargs.get("timeout")}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

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
            payload={
                "choices": [
                    {
                        "message": {
                            "content": json_module.dumps(
                                {
                                    "decision": "approve",
                                    "reason": "需求明确，可进入后续排期。",
                                    "normalized_title": "默认标题",
                                    "normalized_description": "默认描述",
                                },
                                ensure_ascii=False,
                            )
                        },
                        "finish_reason": "stop",
                    }
                ]
            },
        )


json_module = json


class FeedbackRequestsApiTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self._old_db_path = db.DB_PATH
        db.DB_PATH = Path(self._tmp.name) / "feedback-requests-test.db"
        db.init_db()
        _FakeAsyncClient.post_factory = None
        _FakeAsyncClient.captured = {}
        self.client = TestClient(app_module.app)

        setup = self.client.post("/auth/setup-admin", json={"password": "admin123"})
        self.assertEqual(setup.status_code, 200)
        self.admin_headers = {"Authorization": f"Bearer {setup.json()['token']}"}

        created = self.client.post(
            "/users",
            json={"username": "demo-user", "password": "demo-pass"},
            headers=self.admin_headers,
        )
        self.assertEqual(created.status_code, 201)
        self.user = created.json()
        login = self.client.post(
            "/auth/login",
            json={"username": "demo-user", "password": "demo-pass"},
        )
        self.assertEqual(login.status_code, 200)
        self.user_headers = {"Authorization": f"Bearer {login.json()['token']}"}

    def tearDown(self):
        self.client.close()
        db.DB_PATH = self._old_db_path
        self._tmp.cleanup()

    def _mock_review(self, *, decision: str, reason: str, title: str, description: str):
        def _factory(**_kwargs):
            return _FakeHttpResponse(
                status_code=200,
                payload={
                    "choices": [
                        {
                            "message": {
                                "content": json.dumps(
                                    {
                                        "decision": decision,
                                        "reason": reason,
                                        "normalized_title": title,
                                        "normalized_description": description,
                                    },
                                    ensure_ascii=False,
                                )
                            },
                            "finish_reason": "stop",
                        }
                    ]
                },
            )

        _FakeAsyncClient.post_factory = _factory

    def test_feedback_request_approved_enters_queue_and_admin_can_update(self):
        self._mock_review(
            decision="approve",
            reason="需求明确，已进入后续排期。",
            title="新增需求反馈入口",
            description="在页面中新增需求反馈清单入口，并展示历史记录。",
        )

        with mock.patch.object(app_module.httpx, "AsyncClient", _FakeAsyncClient):
            created = self.client.post(
                "/feedback-requests",
                json={
                    "title": "需求反馈入口",
                    "description": "请在客服入口上方增加需求反馈清单，并展示历史提交记录。",
                },
                headers=self.user_headers,
            )

        self.assertEqual(created.status_code, 201)
        payload = created.json()
        self.assertEqual(payload["status"], "todo")
        self.assertEqual(payload["ai_decision"], "approve")
        self.assertIsNone(payload["project_id"])
        self.assertTrue(payload["queue_visible_to_customer"])
        self.assertIn("全 AI 自动排期队列", payload["status_detail"])
        self.assertTrue(_FakeAsyncClient.captured["post_url"].endswith("/chat/completions"))

        mine = self.client.get("/feedback-requests", headers=self.user_headers)
        self.assertEqual(mine.status_code, 200)
        self.assertEqual(len(mine.json()), 1)

        queue = self.client.get("/feedback-requests?status=todo", headers=self.admin_headers)
        self.assertEqual(queue.status_code, 200)
        self.assertEqual(len(queue.json()), 1)
        self.assertEqual(queue.json()[0]["id"], payload["id"])

        forbidden = self.client.patch(
            f"/feedback-requests/{payload['id']}",
            json={"status": "in_progress"},
            headers=self.user_headers,
        )
        self.assertEqual(forbidden.status_code, 403)

        updated = self.client.patch(
            f"/feedback-requests/{payload['id']}",
            json={"status": "in_progress", "admin_feedback": "已进入第一轮开发。"},
            headers=self.admin_headers,
        )
        self.assertEqual(updated.status_code, 200)
        updated_payload = updated.json()
        self.assertEqual(updated_payload["status"], "in_progress")
        self.assertEqual(updated_payload["admin_feedback"], "已进入第一轮开发。")
        self.assertEqual(updated_payload["updated_by_user_id"], db.get_user_by_username("admin")["id"])

        mine_after = self.client.get("/feedback-requests", headers=self.user_headers)
        self.assertEqual(mine_after.status_code, 200)
        self.assertEqual(mine_after.json()[0]["admin_feedback"], "已进入第一轮开发。")

    def test_feedback_request_rejected_returns_reason_and_not_in_todo_queue(self):
        self._mock_review(
            decision="reject",
            reason="当前描述偏空泛，缺少明确的软件改动目标。",
            title="请优化一下",
            description="请明确要优化的页面、流程或能力后再提交。",
        )

        with mock.patch.object(app_module.httpx, "AsyncClient", _FakeAsyncClient):
            created = self.client.post(
                "/feedback-requests",
                json={
                    "title": "请优化一下",
                    "description": "感觉不太好用，请全部优化。",
                },
                headers=self.user_headers,
            )

        self.assertEqual(created.status_code, 201)
        payload = created.json()
        self.assertEqual(payload["status"], "rejected")
        self.assertEqual(payload["ai_decision"], "reject")
        self.assertFalse(payload["queue_visible_to_customer"])
        self.assertIn("缺少明确的软件改动目标", payload["status_detail"])

        queue = self.client.get("/feedback-requests?status=todo", headers=self.admin_headers)
        self.assertEqual(queue.status_code, 200)
        self.assertEqual(queue.json(), [])

    def test_feedback_request_forbids_admin_submission_and_surfaces_ai_failure(self):
        forbidden = self.client.post(
            "/feedback-requests",
            json={
                "title": "管理员提交",
                "description": "管理员端不应该提供这个入口。",
            },
            headers=self.admin_headers,
        )
        self.assertEqual(forbidden.status_code, 403)

        def _error_factory(**_kwargs):
            return _FakeHttpResponse(status_code=503, text="gateway down")

        _FakeAsyncClient.post_factory = _error_factory
        with mock.patch.object(app_module.httpx, "AsyncClient", _FakeAsyncClient):
            failed = self.client.post(
                "/feedback-requests",
                json={
                    "title": "需求反馈入口",
                    "description": "请在客服入口上方增加需求反馈清单。",
                },
                headers=self.user_headers,
            )

        self.assertEqual(failed.status_code, 503)
        self.assertIn("小白客审核暂时不可用", failed.text)


if __name__ == "__main__":
    unittest.main()
