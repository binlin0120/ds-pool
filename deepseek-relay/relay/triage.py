# -*- coding: utf-8 -*-
"""429/4xx 分诊器：把"看起来都是 429"的上游响应拆成互斥的处理决策。

四条硬规则（来自 docs/仓库调研与方案设计.md 3.3）：
1. penalize_credential=False 的情形绝不扣 key 健康分（否则整池集体进冷却 = "一直 429"）。
2. waf_block 不重试，立刻换出口（重试同出口只会加深风控标记）。
3. Retry-After 必须 clamp。
4. SSE 流中途的错误也要分诊：HTTP 200 + 流内 error 帧同样要判。
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional

KINDS = (
    "ok", "waf_block", "auth_invalid", "credential_risk", "quota",
    "rpm", "tpm", "upstream_capacity", "upstream_error", "client_error",
    "unknown_burst", "network_error", "upstream_empty_output",
)

_WAF_BODY = re.compile(r"(request blocked|block-event-id|clou-201|just a moment|"
                       r"attention required|cf-mitigated)", re.I)
_GOVERNOR = re.compile(r"governor", re.I)
_CAPACITY = re.compile(r"(is full at the moment|current capacity|max capacity|"
                       r"server is busy|too many concurrent|overload)", re.I)
_RISK = re.compile(r"(risk_device_detected|risk device|device.{0,12}risk|"
                   r"login failed|risk_control|riskverify)", re.I)
_QUOTA = re.compile(r"(insufficient_quota|insufficient balance|exceeded your current|"
                    r"quota|billing|arrears|no balance)", re.I)


@dataclass(frozen=True)
class Verdict:
    kind: str
    retryable: bool
    switch_credential: bool
    rebuild_egress: bool
    backoff_seconds: Optional[float]
    penalize_credential: bool
    status_code: int = 0
    reason: str = ""
    detail: str = ""

    @property
    def is_error(self) -> bool:
        return self.kind != "ok"


def clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _lower_headers(headers: Mapping[str, str]) -> Dict[str, str]:
    return {str(k).lower(): str(v) for k, v in (headers or {}).items()}


def parse_retry_after(headers: Mapping[str, str], lo: float = 1.0,
                      hi: float = 60.0) -> Optional[float]:
    h = _lower_headers(headers)
    raw = h.get("retry-after") or h.get("x-ratelimit-reset-requests") or \
        h.get("retry-after-interval")
    if not raw:
        return None
    m = re.search(r"(\d+(?:\.\d+)?)", str(raw))
    if not m:
        return None
    return clamp(float(m.group(1)), lo, hi)


def safe_error(resp_body: str | bytes) -> Dict[str, Any]:
    """尽力解出 OpenAI/DeepSeek 风格 error 对象；解不出返回 {}。"""
    if isinstance(resp_body, bytes):
        resp_body = resp_body.decode("utf-8", "replace")
    text = (resp_body or "").strip()
    if not text:
        return {}
    try:
        obj = json.loads(text)
    except Exception:
        m = re.search(r"\{.*\}", text, re.S)
        if not m:
            return {}
        try:
            obj = json.loads(m.group(0))
        except Exception:
            return {}
    if not isinstance(obj, dict):
        return {}
    err = obj.get("error")
    if isinstance(err, dict):
        out = {"code": err.get("code"), "type": err.get("type"),
               "message": err.get("message") or ""}
        if err.get("biz_msg"):
            out["message"] = str(out["message"]) + " " + str(err["biz_msg"])
        return out
    if isinstance(err, str):
        return {"code": None, "type": None, "message": err}
    for k in ("message", "msg", "biz_msg", "detail", "error_msg"):
        if obj.get(k):
            return {"code": obj.get("code"), "type": obj.get("type"),
                    "message": str(obj[k])}
    return {}


def triage(status_code: int, headers: Optional[Mapping[str, str]] = None,
           body: str | bytes = "", exc: Optional[BaseException] = None) -> Verdict:
    """status_code=0 + exc 表示连接层失败（超时/DNS/TLS）。"""
    h = _lower_headers(headers or {})
    if isinstance(body, bytes):
        text = body.decode("utf-8", "replace")
    else:
        text = body or ""
    err = safe_error(text)
    msg = " ".join(str(x) for x in (err.get("message"), err.get("code"),
                                    err.get("type"), text[:400]) if x)

    if exc is not None or status_code == 0:
        return Verdict("network_error", True, True, True, 2.0, False,
                       0, "connection/timeout: %s" % type(exc).__name__ if exc
                       else "connection/timeout")

    # (1) 边缘 WAF：换 key 无效、重试只会加重 -> 换出口，不罚 key
    if "block-event-id" in h or (status_code in (403, 429, 503) and _WAF_BODY.search(text)):
        return Verdict("waf_block", False, False, True, None, False, status_code,
                       "edge WAF block (Block-Event-Id / clou-201 style)")

    # (2) 网页版风控/登录失效：换账号 + 换出口，罚该账号
    if _RISK.search(msg) or _RISK.search(text):
        return Verdict("credential_risk", False, True, True, None, True, status_code,
                       "upstream risk control rejected this credential/device")

    # (3) 官方 governor：认证失败，重试无意义
    if status_code in (401, 403) and _GOVERNOR.search(text):
        return Verdict("auth_invalid", False, True, False, None, True, status_code,
                       "authentication fails (governor)")

    code = str(err.get("code") or "").lower()
    etype = str(err.get("type") or "").lower()

    if status_code in (401, 403) and ("invalid" in etype or "auth" in etype or
                                       code in ("", "invalid_api_key",
                                                "authentication_error")):
        return Verdict("auth_invalid", False, True, False, None, True, status_code,
                       etype or code or "auth error")
    if status_code == 402 or code == "insufficient_quota" or \
            (status_code in (401, 403, 429) and _QUOTA.search(msg) and
             not _CAPACITY.search(msg)):
        return Verdict("quota", False, True, False, None, True, status_code,
                       code or etype or "quota/balance exhausted")

    if status_code == 429:
        if _CAPACITY.search(msg):
            # 上游全局容量：与 key / IP 无关 -> 退避重试，但不罚 key
            return Verdict("upstream_capacity", True, False, False,
                           parse_retry_after(h, 2, 60) or 3.0, False, status_code,
                           "upstream at capacity")
        if code == "ratelimitreached" or etype in ("requests", "tokens"):
            if etype == "tokens" or "token" in str(err.get("message", "")).lower():
                return Verdict("tpm", True, False, False,
                               parse_retry_after(h, 1, 30) or 5.0, False,
                               status_code, "token rate limit")
            return Verdict("rpm", True, False, False,
                           parse_retry_after(h, 1, 30) or 5.0, False,
                           status_code, "request rate limit")
        if etype == "insufficient_system_quota" or code == "engine_overloaded":
            return Verdict("upstream_capacity", True, False, False,
                           parse_retry_after(h, 2, 60) or 3.0, False, status_code,
                           code or etype)
        # 裸 429（无 code）：短时突发饱和，几秒就好，抄 codex-lb
        return Verdict("unknown_burst", True, True, False,
                       clamp(parse_retry_after(h, 5, 30) or 5.0, 5, 30), False,
                       status_code, "bare 429 without error code")

    if status_code in (500, 502, 503, 504, 524, 599):
        return Verdict("upstream_error", True, True, False,
                       parse_retry_after(h, 2, 30) or 2.0, False, status_code,
                       "upstream %d" % status_code)

    if 400 <= status_code < 500:
        # 客户端请求本身有问题：原样回传，绝不重试、绝不罚 key
        return Verdict("client_error", False, False, False, None, False, status_code,
                       code or etype or "bad request")

    if 200 <= status_code < 300:
        return Verdict("ok", False, False, False, None, False, status_code, "")

    return Verdict("unknown_burst", True, True, False, 5.0, False, status_code,
                   "unclassified status")


def triage_sse_frame(frame: str) -> Optional[Verdict]:
    """流内 error 帧（HTTP 已经是 200 的情况）——反代最容易漏的路径。"""
    payload = frame.strip()
    if payload.startswith("data:"):
        payload = payload[5:].strip()
    if not payload or payload == "[DONE]":
        return None
    err = safe_error(payload)
    if not err and "error" not in payload.lower():
        return None
    msg = str(err.get("message") or "") + " " + str(err.get("code") or "")
    if not msg.strip():
        return None
    if _RISK.search(msg):
        return Verdict("credential_risk", False, True, True, None, True, 200,
                       "risk control inside SSE frame")
    if _CAPACITY.search(msg):
        return Verdict("upstream_capacity", True, False, False, 3.0, False, 200,
                       "capacity inside SSE frame")
    if _QUOTA.search(msg):
        return Verdict("quota", False, True, False, None, True, 200,
                       "quota inside SSE frame")
    return Verdict("upstream_error", True, True, False, 2.0, False, 200,
                   "sse error frame")
