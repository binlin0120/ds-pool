# -*- coding: utf-8 -*-
"""出口层：只用标准库实现"连接超时/读超时分离 + 流式响应 + HTTP CONNECT 代理 +
入站头不透传"。

要点（对应这次踩过的坑）：
- 入站头只白名单透传，**绝不透传 User-Agent、X-Forwarded-For、X-Real-IP、CF-* 等**；
  把客户端指纹原样带到上游，等于把"机房 IP + 随机客户端"钉成一个稳定的风险特征。
- 连接超时短（默认 10s），读超时长（默认 300s 覆盖长 SSE），每次读之前重置 socket
  超时，避免长流被误杀。
- proxy_url 只支持 http://host:port（CONNECT 隧道，TLS 仍在本地校验）。需要
  socks5/住宅出口时用 privoxy 或 WARP 前置一层。
"""
from __future__ import annotations

import http.client
import json
import socket
import ssl
import time
from typing import Dict, Optional, Tuple
from urllib.parse import urlparse

KEEP_INBOUND = {"authorization", "content-type", "accept"}
DROP_PREFIXES = ("x-forwarded-", "x-real-", "x-envoy-", "cf-", "sec-",
                 "postman-", "x-dashscope", "x-stainless")


def build_upstream_headers(inbound: Optional[Dict[str, str]] = None,
                           user_agent: str = "deepseek-relay/1.0",
                           extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    out: Dict[str, str] = {
        "User-Agent": user_agent or "deepseek-relay/1.0",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Connection": "close",
    }
    for k, v in (inbound or {}).items():
        lk = str(k).lower()
        if lk in KEEP_INBOUND and not lk.startswith(DROP_PREFIXES):
            out[lk] = str(v)
    out.update(extra or {})
    # HTTP 头名大小写等价，但 dict 不等价：入站的 authorization 会和 extra 里
    # 新加的 Authorization 共存，上游只读到第一个（= 客户端的 key）。必须归一化。
    merged = {}
    for k, v in out.items():
        merged[str(k).lower()] = str(v)
    return merged


def set_sock_timeout(conn, timeout):
    """http.client 读完 body 后会把 sock 置 None，直接 conn.sock.settimeout 会
    AttributeError。所有重置读超时的地方都走这个容错版本。"""
    sock = getattr(conn, "sock", None)
    if sock is None:
        return False
    try:
        sock.settimeout(timeout)
        return True
    except Exception:
        return False


class UpstreamError(Exception):
    """连接层失败（DNS/TLS/超时/对端重置），交给 triage 判 network_error。"""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


class UpstreamClient:
    """一次请求一条连接（Connection: close）：失败就换 key 重来，语义最简单。"""

    def __init__(self, connect_timeout: float = 10.0, read_timeout: float = 300.0,
                 proxy_url: str = ""):
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.proxy_url = (proxy_url or "").strip()
        self.ssl_ctx = ssl.create_default_context()

    def _open(self, url: str) -> http.client.HTTPConnection:
        u = urlparse(url)
        host = u.hostname
        if not host:
            raise UpstreamError("bad upstream url")
        port = u.port or (443 if u.scheme == "https" else 80)
        if self.proxy_url:
            p = urlparse(self.proxy_url if "//" in self.proxy_url
                         else "http://" + self.proxy_url)
            phost = p.hostname
            pport = p.port or 80
            cls = (http.client.HTTPSConnection if u.scheme == "https"
                   else http.client.HTTPConnection)
            if u.scheme == "https":
                conn = cls(phost, pport, timeout=self.connect_timeout,
                           context=self.ssl_ctx)
            else:
                conn = cls(phost, pport, timeout=self.connect_timeout)
            conn.set_tunnel(host, port)
            return conn
        if u.scheme == "https":
            return http.client.HTTPSConnection(host, port,
                                               timeout=self.connect_timeout,
                                               context=self.ssl_ctx)
        return http.client.HTTPConnection(host, port, timeout=self.connect_timeout)

    @staticmethod
    def _path(url: str) -> str:
        u = urlparse(url)
        return (u.path or "/") + (("?" + u.query) if u.query else "")

    def post(self, url: str, body: bytes,
             headers: Dict[str, str]) -> Tuple[object, http.client.HTTPConnection,
                                               Dict[str, str], float]:
        """返回 (resp, conn, headers, connect_seconds)。调用方负责 conn.close()。"""
        t0 = time.monotonic()
        conn = self._open(url)
        try:
            conn.putrequest("POST", self._path(url), skip_accept_encoding=True)
            for k, v in headers.items():
                if k.lower() == "content-length":
                    continue
                conn.putheader(k, v)
            conn.putheader("Content-Length", str(len(body)))
            conn.endheaders(body)
            set_sock_timeout(conn, self.read_timeout)
            resp = conn.getresponse()
        except (socket.timeout, TimeoutError, ConnectionError, OSError,
                http.client.HTTPException) as exc:
            try:
                conn.close()
            except Exception:
                pass
            kind = "timeout" if isinstance(exc, (socket.timeout, TimeoutError)) else str(exc)
            raise UpstreamError("%s: %s" % (type(exc).__name__, kind)) from exc
        elapsed = time.monotonic() - t0
        hdrs = {str(k).lower(): str(v) for k, v in resp.getheaders()}
        return resp, conn, hdrs, elapsed

    def read_all(self, resp, conn, limit: int = 4 * 1024 * 1024) -> bytes:
        try:
            set_sock_timeout(conn, self.read_timeout)
            return resp.read(limit) or b""
        except Exception:
            return b""
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def json_request(self, url: str, payload: dict, headers: Dict[str, str]):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        resp, conn, hdrs, elapsed = self.post(url, body, headers)
        data = self.read_all(resp, conn)
        return resp.status, hdrs, data, elapsed
