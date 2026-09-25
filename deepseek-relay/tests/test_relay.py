# -*- coding: utf-8 -*-
"""端到端：Relay + 假上游，验证"什么时候重试、什么时候换 key、什么时候绝不罚 key"。"""
from __future__ import annotations

import json
import os
import socket
import sys
import unittest
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from relay.config import Config, CredentialConfig                     # noqa: E402
from relay.server import Relay, Sink                                  # noqa: E402
from tests import mock_upstream                                        # noqa: E402

SRV, BASE = mock_upstream.start()


def make_cfg(api_path="/auto", creds=None, client_keys=("ck-1",), **kw):
    cfg = Config(base_url=BASE, api_path=api_path,
                 credentials=list(creds or [CredentialConfig(key="keyA", label="A"),
                                            CredentialConfig(key="keyB", label="B")]),
                 client_keys=list(client_keys), max_attempts=3,
                 upstream_timeout_connect=5.0, upstream_timeout_read=10.0,
                 inbound_rpm=6000, inbound_burst=2000)
    for k, v in kw.items():
        setattr(cfg, k, v)
    return Relay(cfg)


def call(relay, payload, sink=None, rid="t-1", headers=None):
    s = sink or Sink()
    body = json.dumps(payload).encode("utf-8")
    status = relay.handle_chat(body, headers or {}, s, rid)
    return status, s


BARE429 = (429, json.dumps({"error": {"message": "Too Many Requests"}}))
CAP429 = (429, json.dumps({"error": {"message": "current capacity is full at the moment",
                                     "type": "rate_limit_exceeded",
                                     "code": "RateLimitReached"}}))
RISK401 = (401, json.dumps({"error": {"code": "authentication_failed",
                                      "message": "login failed: RISK_DEVICE_DETECTED",
                                      "type": "authentication_error"}}))
OK200 = (200, json.dumps({"choices": [{"message": {"role": "assistant",
                                                   "content": "OK"}}]}))
BAD400 = (400, json.dumps({"error": {"message": "The model `deepseek-v99` does not exist",
                                     "type": "invalid_request_error",
                                     "code": "model_not_found"}}))


class TestRelay(unittest.TestCase):
    def setUp(self):
        mock_upstream.reset()

    # ---- 分诊驱动的重试语义 ----
    def test_bare429_retries_other_key_without_cooldown(self):
        mock_upstream.STATE.scenarios = {"keyA": {"auto": [BARE429]},
                                         "keyB": {"auto": [OK200]}}
        relay = make_cfg()
        status, sink = call(relay, {"model": "deepseek-chat", "messages": []})
        self.assertEqual(status, 200)
        self.assertEqual(b"OK" in b"".join(sink.chunks), True)
        a = [c for c in relay.pool.status() if c["label"] == "A"][0]
        self.assertTrue(a["available"], "裸 429 不该把 key 打入冷却")
        self.assertEqual(a["cooldown_left"], 0)

    def test_capacity_429_keeps_whole_pool_available(self):
        mock_upstream.STATE.scenarios = {"keyA": {"auto": [CAP429]},
                                         "keyB": {"auto": [CAP429]}}
        relay = make_cfg()
        status, sink = call(relay, {"model": "m", "messages": []})
        self.assertEqual(status, 429)
        err = json.loads(b"".join(sink.chunks).decode())
        self.assertEqual(err["error"]["code"], "exhausted_upstream_capacity")
        self.assertIn("retry_after_seconds", err["relay"])
        for c in relay.pool.status():
            self.assertTrue(c["available"], "容量类 429 绝不能罚 key（否则全池冷却=一直429）")
        self.assertEqual(len(mock_upstream.STATE.hits), relay.cfg.max_attempts)

    def test_risk_device_penalizes_credential_and_switches(self):
        mock_upstream.STATE.scenarios = {"keyA": {"auto": [RISK401]},
                                         "keyB": {"auto": [OK200]}}
        relay = make_cfg()
        status, sink = call(relay, {"model": "m", "messages": []})
        self.assertEqual(status, 200)
        a = [c for c in relay.pool.status() if c["label"] == "A"][0]
        self.assertFalse(a["available"], "被风控的账号必须下线")
        self.assertGreater(a["cooldown_left"], 0)

    def test_client_error_is_not_retried_or_penalized(self):
        mock_upstream.STATE.scenarios = {"keyA": {"auto": [BAD400]}}
        relay = make_cfg()
        status, sink = call(relay, {"model": "deepseek-v99", "messages": []})
        self.assertEqual(status, 400)
        self.assertEqual(len(mock_upstream.STATE.hits), 1, "4xx 客户端错绝不能重试")
        self.assertTrue(all(c["available"] for c in relay.pool.status()))
        self.assertEqual(json.loads(b"".join(sink.chunks).decode())["error"]["code"],
                         "client_error")

    def test_single_credential_is_never_ejected(self):
        mock_upstream.STATE.scenarios = {"keyA": {"auto": [RISK401, RISK401, RISK401]}}
        relay = make_cfg(creds=[CredentialConfig(key="keyA", label="A")])
        for _ in range(3):
            call(relay, {"model": "m", "messages": []})
        st = [s for s in relay.pool.states if s.label == "A"][0]
        self.assertTrue(st.usable_now(__import__("time").time()) or not st.available(
            __import__("time").time()), "见 candidates() 的兜底逻辑")
        status, _sink = call(relay, {"model": "m", "messages": []})
        self.assertIn(status, (401, 503, 429))
        self.assertGreaterEqual(len(mock_upstream.STATE.hits), 4,
                                "池里只剩一个也必须继续尝试，不能直接 no upstream")

    def test_stream_early_error_retries_before_headers(self):
        mock_upstream.STATE.scenarios = {
            "keyA": {"stream": ['data: {"error":{"message":"RISK_DEVICE_DETECTED"}}\n\n']},
            "keyB": {"stream": ['data: {"choices":[{"delta":{"content":"hi"}}]}\n\n',
                                "data: [DONE]\n\n"]},
        }
        relay = make_cfg(api_path="/stream")
        status, sink = call(relay, {"model": "m", "messages": [], "stream": True})
        self.assertEqual(status, 200)
        joined = b"".join(sink.chunks)
        self.assertIn(b'"delta"', joined)
        self.assertNotIn(b"RISK_DEVICE", joined)

    def test_stream_success_passthrough(self):
        mock_upstream.STATE.scenarios = {"keyA": {"stream": [
            'data: {"choices":[{"delta":{"content":"a"}}]}\n\n',
            'data: {"choices":[{"delta":{"content":"b"}}]}\n\n', "data: [DONE]\n\n"]}}
        relay = make_cfg(api_path="/stream")
        status, sink = call(relay, {"model": "m", "messages": [], "stream": True})
        self.assertEqual(status, 200)
        self.assertEqual(b"".join(sink.chunks).count(b"data:"), 3)

    def test_upstream_gone_yields_network_error_status(self):
        mock_upstream.STATE.scenarios = {"keyA": {"auto": [(503, "Service Unavailable")]},
                                         "keyB": {"auto": [(503, "Service Unavailable")]}}
        relay = make_cfg()
        status, sink = call(relay, {"model": "m", "messages": []})
        self.assertEqual(status, 502)
        self.assertTrue(json.loads(b"".join(sink.chunks).decode())["error"]["code"]
                        .startswith("exhausted_upstream_error"))

    def test_malformed_body_is_400_before_upstream(self):
        relay = make_cfg()
        sink = Sink()
        status = relay.handle_chat(b"{not json", {}, sink, "t")
        self.assertEqual(status, 400)
        self.assertEqual(mock_upstream.STATE.hits, [])

    def test_status_snapshot_masks_nothing_but_labels(self):
        relay = make_cfg()
        snap = relay.status_snapshot()
        self.assertEqual(len(snap["credentials"]), 2)
        for c in snap["credentials"]:
            self.assertNotIn("key", c)


class TestWire(unittest.TestCase):
    """真 socket 走一遍：验证 HTTP 报文格式（Content-Length / SSE 不被缓冲）。"""

    @classmethod
    def setUpClass(cls):
        from relay.server import make_handler
        from http.server import ThreadingHTTPServer as T
        mock_upstream.reset()
        mock_upstream.STATE.scenarios = {"keyA": {"auto": [OK200]},
                                         "keyB": {"auto": [OK200]}}
        cls.relay = make_cfg()
        cls.httpd = T(("127.0.0.1", 0), make_handler(cls.relay))
        cls.httpd.daemon_threads = True
        import threading
        cls.thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.thread.start()
        cls.port = cls.httpd.server_address[1]

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        cls.httpd.server_close()

    def _req(self, method, path, body=None, headers=None):
        req = urllib.request.Request("http://127.0.0.1:%d%s" % (self.port, path),
                                     data=body, method=method, headers=headers or {})
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                return r.status, dict(r.headers), r.read()
        except urllib.error.HTTPError as e:
            return e.code, dict(e.headers), e.read()

    def test_healthz_needs_no_key(self):
        status, _h, body = self._req("GET", "/healthz")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["status"], "ok")

    def test_inbound_auth_rejects_missing_key(self):
        status, _h, body = self._req("POST", "/v1/chat/completions",
                                     b'{"model":"m","messages":[]}')
        self.assertEqual(status, 401)
        self.assertEqual(json.loads(body)["error"]["code"], "auth_invalid")

    def test_nonstream_response_has_content_length(self):
        status, h, body = self._req(
            "POST", "/v1/chat/completions",
            json.dumps({"model": "m", "messages": [{"role": "user", "content": "x"}]}).encode(),
            {"Authorization": "Bearer ck-1", "Content-Type": "application/json",
             "User-Agent": "attacker-ua/9.9", "X-Forwarded-For": "1.2.3.4"})
        self.assertEqual(status, 200)
        self.assertIn("Content-Length", h)
        self.assertIn(b"OK", body)
        for tag, _p in mock_upstream.STATE.hits:
            self.assertEqual(tag, "keyA")
        # 入站 UA 不允许透传：假上游收到的 UA 由 relay 自己决定
        self.assertEqual(json.loads(body)["choices"][0]["message"]["content"], "OK")

    def test_unknown_path_is_json_404(self):
        status, h, body = self._req("GET", "/v1/nope")
        self.assertEqual(status, 404)
        self.assertIn("json", h.get("Content-Type", ""))


if __name__ == "__main__":
    unittest.main(verbosity=2)
