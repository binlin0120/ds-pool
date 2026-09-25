# -*- coding: utf-8 -*-
"""L1 网关 + L2 治理：OpenAI 兼容入口 -> key 池 -> 上游，带 429 分诊。

只做三件事：
1. 入站：鉴权 + 宽松 IP 令牌桶 + 并发闸门，错误永远是 JSON（不是 nginx 的 HTML 429）。
2. 出站：不透传入站指纹，按池选择凭证，每种失败按 Verdict 决定重试/换 key/停。
3. 观测：每请求一行结构化日志（rid/kind/status/label/耗时），/relay/status 看池子状态。
"""
from __future__ import annotations

import http.client
import json
import logging
import secrets
import socket
import threading
import time
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, List, Optional, Tuple

from .config import Config
from .egress import (UpstreamClient, UpstreamError, build_upstream_headers,
                     set_sock_timeout)
from .limiter import Gate, InboundLimiter
from .pool import KeyPool
from .triage import Verdict, clamp, triage, triage_sse_frame

CLIENT_ERROR_STATUS = {"client_error": 400, "auth_invalid": 401, "quota": 402,
                       "credential_risk": 503, "waf_block": 503,
                       "upstream_capacity": 429, "rpm": 429, "tpm": 429,
                       "unknown_burst": 429, "upstream_error": 502,
                       "network_error": 502}
RETRYABLE = {"upstream_capacity", "rpm", "tpm", "unknown_burst",
             "upstream_error", "network_error", "auth_invalid", "quota",
             "credential_risk"}


def send_err(sink: "Sink", err: Tuple[int, Dict[str, str], bytes]) -> int:
    """_err 返回三元组，这里统一写入 sink。"""
    status, headers, data = err
    sink.start(status, headers)
    sink.write(data)
    return status


def _err(status: int, kind: str, message: str, rid: str,
         retry_after: Optional[float] = None) -> Tuple[int, Dict[str, str], bytes]:
    body = {"error": {"message": message, "type": "relay_error", "code": kind,
                      "param": None},
            "relay": {"request_id": rid, "verdict": kind,
                      "retry_after_seconds": retry_after}}
    data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    headers = {"Content-Type": "application/json; charset=utf-8"}
    if retry_after is not None:
        headers["Retry-After"] = str(int(clamp(retry_after, 1, 120)))
    headers["X-Request-Id"] = rid
    return status, headers, data


@dataclass
class Sink:
    """把响应写回客户端的抽象；测试里换成内存实现。"""
    status: Optional[int] = None
    headers: Optional[Dict[str, str]] = None
    started: bool = False
    chunks: List[bytes] = field(default_factory=list)

    def start(self, status: int, headers: Dict[str, str]) -> None:
        self.status, self.headers, self.started = status, dict(headers), True

    def write(self, data: bytes) -> None:
        self.chunks.append(data)

    def is_started(self) -> bool:
        return self.started


class Relay:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.pool = KeyPool(cfg)
        self.limiter = InboundLimiter(cfg.inbound_rpm, cfg.inbound_burst)
        self.gate = Gate(cfg.max_inflight)
        self.client = UpstreamClient(cfg.upstream_timeout_connect,
                                     cfg.upstream_timeout_read, cfg.proxy_url)
        self.log = logging.getLogger("relay")
        self.counters = {"requests": 0, "ok": 0, "rejected_gate": 0,
                         "rejected_limit": 0, "attempts": 0,
                         "kinds": {}}
        self._lock = threading.Lock()

    # ---------- 入站鉴权 ----------
    def check_client_key(self, authorization: str) -> bool:
        keys = [k for k in self.cfg.client_keys if k]
        if not keys:
            return True                     # 未配置 = 不鉴权（仅限内网监听）
        token = (authorization or "").strip()
        if token.lower().startswith("bearer "):
            token = token[7:].strip()
        return any(secrets.compare_digest(token, k) for k in keys)

    def _note(self, kind: str) -> None:
        with self._lock:
            self.counters["attempts"] += 1
            self.counters["kinds"][kind] = self.counters.get("kinds", {}).get(kind, 0) + 1

    def upstream_url(self, cred_base: str) -> str:
        base = (cred_base or self.cfg.base_url).rstrip("/")
        return base + self.cfg.api_path

    # ---------- 主流程 ----------
    def handle_chat(self, raw_body: bytes, inbound: Dict[str, str],
                    sink: Sink, rid: str) -> int:
        try:
            payload = json.loads(raw_body.decode("utf-8") or "{}")
            if not isinstance(payload, dict):
                raise ValueError("body is not an object")
        except Exception as exc:
            status, headers, data = _err(400, "client_error",
                                         "invalid JSON body: %s" % exc, rid)
            sink.start(status, headers)
            sink.write(data)
            return status

        stream = bool(payload.get("stream"))
        tried: List[str] = []
        last: Optional[Verdict] = None
        deadline = time.monotonic() + max(30.0, self.cfg.upstream_timeout_read * 0.8)
        attempts = 0

        while attempts < self.cfg.max_attempts:
            st = self.pool.pick(exclude=tried)
            if st is None:
                break
            attempts += 1
            tried.append(st.label)
            url = self.upstream_url(st.cfg.base_url)
            body = dict(payload)
            headers = build_upstream_headers(inbound, self.cfg.user_agent,
                                             {"Authorization": "Bearer " + st.cfg.key}
                                             if st.cfg.key else None)
            t0 = time.monotonic()
            try:
                resp, conn, resp_headers, _elapsed = self.client.post(
                    url, json.dumps(body, ensure_ascii=False).encode("utf-8"), headers)
            except UpstreamError as exc:
                v = triage(0, {}, b"", exc=exc)
                last = v
                self._note(v.kind)
                self.pool.note_result(st, v)
                self.log.warning("rid=%s cred=%s attempt=%d %s (%.2fs) -> %s",
                                 rid, st.label, attempts, v.kind,
                                 time.monotonic() - t0, v.reason)
                if not self._wait(v, deadline, attempts):
                    break
                continue

            status = resp.status
            if status >= 400 or not stream:
                data = self.client.read_all(resp, conn)
                v = triage(status, resp_headers, data)
                last = v
                self._note(v.kind)
                self.pool.note_result(st, v)
                self.log.info("rid=%s cred=%s attempt=%d http=%d kind=%s t=%.2fs %s",
                              rid, st.label, attempts, status, v.kind,
                              time.monotonic() - t0, v.reason)
                if v.kind == "ok":
                    with self._lock:
                        self.counters["ok"] += 1
                    h = {"Content-Type": resp_headers.get("content-type",
                                                          "application/json")}
                    if resp_headers.get("x-request-id"):
                        h["X-Upstream-Request-Id"] = resp_headers["x-request-id"]
                    sink.start(200, h)
                    sink.write(data)
                    return 200
                if self._should_retry(v, attempts) and self._wait(v, deadline, attempts):
                    continue
                if attempts >= self.cfg.max_attempts:
                    break      # 重试预算用尽，统一走 exhausted_* 出口
                send_err(sink, _err(CLIENT_ERROR_STATUS.get(v.kind, status), v.kind,
                                 "upstream %d: %s" % (status,
                                                      (data[:300].decode("utf-8", "replace")
                                                       if data else v.reason)),
                                 rid, v.backoff_seconds))
                sink.write(b"")
                return CLIENT_ERROR_STATUS.get(v.kind, status)

            # --- 流式：先探首包，错误要在发出 200 头之前判定 ---
            v = self._stream(st, resp, conn, payload, sink, rid, attempts, t0)
            last = v
            self._note(v.kind)
            self.pool.note_result(st, v)
            if v.kind == "ok":
                with self._lock:
                    self.counters["ok"] += 1
                return 200
            if sink.started:
                return 200          # 头已发出，只能就地断流，由客户端重试
            if self._should_retry(v, attempts) and self._wait(v, deadline, attempts):
                continue
            if attempts >= self.cfg.max_attempts:
                break      # 重试预算用尽，统一走 exhausted_* 出口
            send_err(sink, _err(CLIENT_ERROR_STATUS.get(v.kind, 502), v.kind,
                             "upstream stream error: %s" % v.reason, rid,
                             v.backoff_seconds))
            sink.write(b"")
            return CLIENT_ERROR_STATUS.get(v.kind, 502)

        kind = (last.kind if last else "no_credential")
        status = CLIENT_ERROR_STATUS.get(kind, 503)
        if kind == "unknown_burst" or status == 200:
            status = 429
        send_err(sink, _err(status, "exhausted_" + kind,
                         "no upstream succeeded after %d attempt(s): %s"
                         % (attempts, (last.reason if last else "pool empty")),
                         rid, (last.backoff_seconds if last else 2.0)))
        sink.write(b"")
        return status

    def _should_retry(self, v: Verdict, attempts: int) -> bool:
        """retryable = 同凭证退避后重试；switch_credential = 换下一个凭证再试。

        只看 retryable 会漏掉 credential_risk / quota / governor 这三类
        "必须换 key、但不需要退避" 的情况 —— 而这正是"一个账号被风控 =
        整个网关一直 429/503"的成因。"""
        if attempts >= self.cfg.max_attempts:
            return False
        return bool(v.retryable or v.switch_credential)

    def _wait(self, v: Verdict, deadline: float, attempts: int) -> bool:
        if attempts >= self.cfg.max_attempts:
            return False
        if time.monotonic() >= deadline:
            return False
        wait = v.backoff_seconds
        if wait is None:
            # 换 key 不需要等（下一个凭证是现成的），只有退避类错误才 sleep
            wait = 0.05 if (v.switch_credential and not v.retryable) else 1.0
        remaining = max(0.0, deadline - time.monotonic())
        time.sleep(min(max(0.2, float(wait)), remaining, 30.0))
        return True

    # ---------- 流式转发 ----------
    def _stream(self, st, resp, conn, payload: dict, sink: Sink, rid: str,
                attempts: int, t0: float) -> Verdict:
        ctype = (resp.getheader("Content-Type") or "text/event-stream").lower()
        first = b""
        probe = b""
        try:
            while True:
                chunk = resp.read1(65536) if hasattr(resp, "read1") else resp.read(1)
                if not chunk:
                    break
                first = chunk
                probe = (probe + chunk)[:4096]
                bad = _early_error(probe, ctype)
                if bad is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass
                    self.log.info("rid=%s cred=%s attempt=%d stream-early-error kind=%s",
                                  rid, st.label, attempts, bad.kind)
                    return bad
                if len(probe) >= 16 or b"data:" in probe:
                    break
            if not first:
                try:
                    conn.close()
                except Exception:
                    pass
                return Verdict("upstream_empty_output", True, True, False, 2.0,
                               False, resp.status, "stream closed before first byte")
            sink.start(200, {"Content-Type": ctype, "Cache-Control": "no-cache",
                             "X-Accel-Buffering": "no", "X-Request-Id": rid})
            sink.write(first)
            while True:
                try:
                    set_sock_timeout(conn, self.cfg.upstream_timeout_read)
                    chunk = resp.read1(65536) if hasattr(resp, "read1") else resp.read(4096)
                except (socket.timeout, TimeoutError, ConnectionError, OSError) as exc:
                    self.log.warning("rid=%s stream read interrupted: %s", rid, exc)
                    return Verdict("ok", False, False, False, None, False, 200,
                                   "stream ended by read timeout")
                if not chunk:
                    break
                sink.write(chunk)
            return Verdict("ok", False, False, False, None, False, resp.status,
                           "streamed %.2fs" % (time.monotonic() - t0))
        except (socket.timeout, TimeoutError, ConnectionError, OSError,
                http.client.HTTPException) as exc:
            try:
                conn.close()
            except Exception:
                pass
            return triage(0, {}, b"", exc=exc)
        finally:
            try:
                conn.close()
            except Exception:
                pass

    # ---------- 其他端点 ----------
    def handle_models(self, inbound: Dict[str, str], sink: Sink, rid: str) -> int:
        st = self.pool.pick()
        base = ((st.cfg.base_url if st else "") or self.cfg.base_url).rstrip("/")
        url = base + "/v1/models" if not base.endswith("/v1") else base + "/models"
        headers = build_upstream_headers(
            inbound, self.cfg.user_agent,
            {"Authorization": "Bearer " + st.cfg.key} if st and st.cfg.key else None)
        try:
            status, rh, data, _t = self.client.json_request(url, {}, headers)
        except UpstreamError as exc:
            v = triage(0, {}, b"", exc=exc)
            if st:
                self.pool.note_result(st, v)
            self._note(v.kind)
            send_err(sink, _err(502, v.kind, str(exc), rid, 2.0))
            return 502
        if st:
            v = triage(status, rh, data)
            self._note(v.kind)
            self.pool.note_result(st, v)
        sink.start(status, {"Content-Type": rh.get("content-type", "application/json"),
                            "X-Request-Id": rid})
        sink.write(data)
        return status

    def status_snapshot(self) -> Dict[str, object]:
        with self._lock:
            counters = dict(self.counters)
            counters["requests"] = self.counters["attempts"]
        return {"ok": True, "uptime_role": "relay", "credentials": self.pool.status(),
                "inbound_limit": self.limiter.stats(), "gate": self.gate.stats(),
                "counters": counters,
                "config": {"base_url": self.cfg.base_url, "api_path": self.cfg.api_path,
                           "max_attempts": self.cfg.max_attempts,
                           "proxy": bool(self.cfg.proxy_url)}}


def _early_error(probe: bytes, ctype: str) -> Optional[Verdict]:
    """首包探伤：HTTP 200 但内容其实是错误（ds2api/中转站常见）。"""
    text = probe.decode("utf-8", "replace").lstrip()
    if not text:
        return None
    if text.startswith("{") and '"error"' in text:
        v = triage(200, {"content-type": ctype}, probe)
        return None if v.kind == "ok" else v
    for line in text.splitlines():
        if line.strip().startswith("data:"):
            v = triage_sse_frame(line)
            if v is not None:
                return v
    return None

# ---------- HTTP 表层 ----------
class HttpSink(Sink):
    def __init__(self, handler: BaseHTTPRequestHandler):
        super().__init__()
        self.h = handler
        self.committed = False

    def _is_stream(self) -> bool:
        return "event-stream" in ((self.headers or {}).get("Content-Type", "") or "").lower()

    def write(self, data: bytes) -> None:
        if not data:
            return
        if self._is_stream():
            self._commit()
            self.chunks.append(data)
            try:
                self.h.wfile.write(data)
                self.h.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                raise
        else:
            self.chunks.append(data)

    def _commit(self) -> None:
        if self.committed:
            return
        self.committed = True
        self.h.send_response(self.status or 500)
        for k, v in (self.headers or {}).items():
            self.h.send_header(k, v)
        self.h.send_header("X-Relay", "deepseek-relay/1.0")
        self.h.close_connection = True          # 流式不声明 Content-Length，用关连接兜底
        self.h.end_headers()

    def finish(self) -> None:
        if self._is_stream():
            if not self.committed:
                self._commit()
            return
        body = b"".join(self.chunks)
        self.h.send_response(self.status or 500)
        sent = set()
        for k, v in (self.headers or {}).items():
            self.h.send_header(k, v)
            sent.add(k.lower())
        if "content-length" not in sent:
            self.h.send_header("Content-Length", str(len(body)))
        self.h.send_header("X-Relay", "deepseek-relay/1.0")
        self.h.end_headers()
        if body:
            self.h.wfile.write(body)


def make_handler(relay: Relay):
    cfg = relay.cfg

    class Handler(BaseHTTPRequestHandler):
        server_version = "deepseek-relay"
        sys_version = ""
        protocol_version = "HTTP/1.1"

        # ---- helpers ----
        def real_ip(self) -> str:
            peer = (self.client_address[0] if self.client_address else "?")
            fwd = self.headers.get("X-Forwarded-For", "")
            if fwd and peer in ("127.0.0.1", "::1"):
                return fwd.split(",")[0].strip()
            return peer

        def inbound_headers(self) -> Dict[str, str]:
            return {k: v for k, v in self.headers.items()}

        def send_json(self, status: int, obj: dict) -> int:
            data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return status

        def drain_body(self, cap: int = 1 << 20) -> None:
            """有 body 却直接回 4xx 就关连接，客户端拿到的是连接重置
            （WinError 10053 / ECONNRESET）而不是我们的 JSON 错误。
            所有"未读 body 就拒绝"的分支，先把 body 读空。"""
            try:
                remaining = min(max(int(self.headers.get("Content-Length") or 0), 0), cap)
            except ValueError:
                remaining = 0
            while remaining > 0:
                chunk = self.rfile.read(min(65536, remaining))
                if not chunk:
                    break
                remaining -= len(chunk)

        def read_body(self) -> bytes:
            length = self.headers.get("Content-Length")
            if length is None:
                if (self.headers.get("Transfer-Encoding", "") or "").lower() == "chunked":
                    raise ValueError("chunked request body not supported")
                return b""
            n = int(length)
            if n > 20 * 1024 * 1024:
                raise ValueError("body too large")
            return self.rfile.read(n)

        def authorize(self) -> bool:
            return relay.check_client_key(self.headers.get("Authorization", ""))

        def is_admin(self) -> bool:
            token = cfg.admin_token
            if not token:
                return self.real_ip() in ("127.0.0.1", "::1")
            got = (self.headers.get("Authorization", "") or "").strip()
            if got.lower().startswith("bearer "):
                got = got[7:].strip()
            return secrets.compare_digest(got, token)

        # ---- routes ----
        def do_GET(self):  # noqa: N802
            path = self.path.split("?")[0]
            if path in ("/healthz", "/health", "/"):
                self.send_json(200, {"status": "ok", "ts": int(time.time())})
                return
            if path in ("/v1/models", "/models"):
                if not self.authorize():
                    self.send_json(401, {"error": {"message": "invalid api key",
                                                  "type": "relay_error",
                                                  "code": "auth_invalid"}})
                    return
                sink = HttpSink(self)
                try:
                    relay.handle_models(self.inbound_headers(), sink, self.rid())
                except Exception as exc:            # 兜底 500，绝不裸堆栈给客户端
                    relay.log.exception("models failed")
                    sink.status, sink.headers = 500, {"Content-Type": "application/json"}
                    sink.write(json.dumps({"error": {"message": str(exc)[:200],
                                                     "type": "relay_error",
                                                     "code": "internal"}}).encode())
                sink.finish()
                return
            if path in ("/relay/status", "/relay/metrics"):
                if not self.is_admin():
                    self.send_json(403, {"error": "admin token required"})
                    return
                snap = relay.status_snapshot()
                if path.endswith("metrics"):
                    lines = ["relay_up 1"]
                    for c in snap["credentials"]:
                        lines.append('relay_credential_available{label="%s"} %d'
                                     % (c["label"], 1 if c["available"] else 0))
                        lines.append('relay_credential_failed_total{label="%s"} %d'
                                     % (c["label"], c["failed"]))
                    self.send_response(200)
                    self.send_header("Content-Type", "text/plain; version=0.0.4")
                    body = ("\n".join(lines) + "\n").encode()
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                self.send_json(200, snap)
                return
            self.send_json(404, {"error": {"message": "unknown path " + path,
                                          "type": "relay_error", "code": "not_found"}})

        def do_POST(self):  # noqa: N802
            path = self.path.split("?")[0]
            if path not in ("/v1/chat/completions", "/chat/completions"):
                self.drain_body()
                self.send_json(404, {"error": {"message": "unknown path " + path,
                                               "type": "relay_error",
                                               "code": "not_found"}})
                return
            if not self.authorize():
                self.drain_body()
                self.send_json(401, {"error": {"message": "invalid api key",
                                               "type": "relay_error",
                                               "code": "auth_invalid"}})
                return
            ok, wait = relay.limiter.allow(self.real_ip())
            if not ok:
                self.drain_body()
                with relay._lock:
                    relay.counters["rejected_limit"] += 1
                self.send_json(429, {"error": {
                    "message": "inbound rate limit (relay), retry in %.0fs" % wait,
                    "type": "relay_error", "code": "rpm"},
                    "relay": {"hint": "这是网关自己的入站保护，不是上游 429"}})
                return
            try:
                body = self.read_body()
            except Exception as exc:
                self.send_json(400, {"error": {"message": str(exc), "type": "relay_error",
                                              "code": "client_error"}})
                return
            with relay._lock:
                relay.counters["requests"] += 1
            sink = HttpSink(self)
            try:
                with relay.gate:
                    relay.handle_chat(body, self.inbound_headers(), sink, self.rid())
            except MemoryError:
                self.send_json(503, {"error": {"message": "relay busy, retry later",
                                               "type": "relay_error",
                                               "code": "upstream_capacity"},
                                     "relay": {"retry_after_seconds": 2}})
                return
            except Exception as exc:
                relay.log.exception("request failed")
                if not sink.committed:
                    self.send_json(500, {"error": {"message": str(exc)[:200],
                                                   "type": "relay_error",
                                                   "code": "internal"}})
                    return
            finally:
                try:
                    sink.finish()
                except Exception:
                    pass

        def rid(self) -> str:
            return self.headers.get("X-Request-Id") or \
                secrets.token_hex(6)

        def log_message(self, fmt, *args):   # 静音默认 stderr 访问日志
            relay.log.debug("http %s", fmt % args)

    return Handler


def serve(cfg: Config) -> None:
    logging.basicConfig(level=getattr(logging, cfg.log_level.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    relay = Relay(cfg)
    httpd = ThreadingHTTPServer((cfg.host, cfg.port), make_handler(relay))
    httpd.daemon_threads = True
    relay.log.info("deepseek-relay listening on http://%s:%d -> %s (creds=%d)",
                   cfg.host, cfg.port, cfg.base_url, len(relay.pool))
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
