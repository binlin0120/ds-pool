# -*- coding: utf-8 -*-
"""golden 回放：每种 429/4xx 都必须落到确定的 Verdict。"""
import io
import json
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from relay.triage import safe_error, triage, triage_sse_frame      # noqa: E402

GOLDEN = os.path.join(ROOT, "tests", "golden")

EXPECTED = {
    "nginx_limit_req_429": "unknown_burst",
    "official_auth_governor_401": "auth_invalid",
    "web_login_risk_device_401": "credential_risk",
    "web_waf_429_block": "waf_block",
    "official_capacity_429": "upstream_capacity",
    "official_quota_402": "quota",
    "official_rpm_429": "rpm",
    "official_tpm_429": "tpm",
    "bare_429_no_code": "unknown_burst",
    "upstream_503": "upstream_error",
    "client_error_400": "client_error",
}

# 硬规则断言：penalize_credential 决定"会不会整池进冷却 = 一直 429"
PENALIZE = {
    "nginx_limit_req_429": False, "official_capacity_429": False,
    "web_waf_429_block": False, "official_rpm_429": False,
    "official_tpm_429": False, "upstream_503": False,
    "client_error_400": False, "official_auth_governor_401": True,
    "web_login_risk_device_401": True, "official_quota_402": True,
    "bare_429_no_code": False,
}
REBUILD_EGRESS = {
    "web_waf_429_block": True, "web_login_risk_device_401": True,
    "official_capacity_429": False, "nginx_limit_req_429": False,
}


def load(name):
    with io.open(os.path.join(GOLDEN, name + ".json"), encoding="utf-8") as f:
        return json.load(f)


class TestGoldenTriage(unittest.TestCase):
    def test_every_golden_classifies(self):
        files = sorted(x[:-5] for x in os.listdir(GOLDEN) if x.endswith(".json"))
        self.assertEqual(files, sorted(EXPECTED.keys()), "golden 目录与用例表不同步")
        for name in files:
            with self.subTest(golden=name):
                g = load(name)
                v = triage(g["status"], g.get("headers"), g.get("body", ""))
                self.assertEqual(v.kind, EXPECTED[name])
                self.assertEqual(v.penalize_credential, PENALIZE[name],
                                 "%s 的罚分标记错了，会导致误判全池冷却" % name)
                if name in REBUILD_EGRESS:
                    self.assertEqual(v.rebuild_egress, REBUILD_EGRESS[name])

    def test_retry_after_is_clamped(self):
        v = triage(429, {"retry-after": "3600"}, '{"error":{"message":"x"}}')
        self.assertLessEqual(v.backoff_seconds, 30.0)
        self.assertGreaterEqual(v.backoff_seconds, 5.0)
        v2 = triage(429, {}, '{"error":{"code":"RateLimitReached","type":"requests",'
                             '"message":"rate limit"}}')
        self.assertEqual(v2.kind, "rpm")
        self.assertLessEqual(v2.backoff_seconds or 0, 30.0)

    def test_capacity_never_penalizes_key(self):
        v = triage(429, {}, json.dumps({"error": {
            "message": "The model is at full capacity, current capacity is full",
            "type": "server_error", "code": "engine_overloaded"}}))
        self.assertIn(v.kind, ("upstream_capacity",))
        self.assertFalse(v.penalize_credential)
        self.assertFalse(v.switch_credential)

    def test_sse_frame_error_inside_200(self):
        v = triage_sse_frame('data: {"error":{"message":"RISK_DEVICE_DETECTED"}}')
        self.assertEqual(v.kind, "credential_risk")
        v2 = triage_sse_frame("data: [DONE]")
        self.assertIsNone(v2)
        v3 = triage_sse_frame('data: {"choices":[{"delta":{"content":"hi"}}]}')
        self.assertIsNone(v3)

    def test_safe_error_handles_non_json(self):
        self.assertEqual(safe_error("<html>boom</html>"), {})
        self.assertEqual(safe_error(b'{"error":{"message":"m"}}')["message"], "m")
        self.assertEqual(safe_error('{"msg":"m2"}')["message"], "m2")
        self.assertEqual(safe_error('{"error":"plain"}')["message"], "plain")

    def test_ok_on_200(self):
        v = triage(200, {"content-type": "application/json"},
                   '{"choices":[{"message":{"content":"OK"}}]}')
        self.assertEqual(v.kind, "ok")
        self.assertFalse(v.is_error)


if __name__ == "__main__":
    unittest.main(verbosity=2)
