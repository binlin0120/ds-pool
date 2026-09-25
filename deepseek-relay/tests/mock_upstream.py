# -*- coding: utf-8 -*-
"""可编程假上游：按 Authorization 里的 tag 走脚本，用于回放每种 429。"""
from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class State:
    def __init__(self):
        self.scenarios = {}      # tag -> {"auto": [(status, body)], "stream": [frames]}
        self.attempts = {}       # tag -> 次数
        self.hits = []           # (tag, path)


STATE = State()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _tag(self) -> str:
        auth = self.headers.get("Authorization", "")
        return auth.replace("Bearer ", "").strip() or "anon"

    def do_POST(self):  # noqa: N802
        n = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(n) if n else b""
        try:
            payload = json.loads(raw.decode("utf-8") or "{}")
        except Exception:
            payload = {}
        tag = self._tag()
        STATE.attempts[tag] = STATE.attempts.get(tag, 0) + 1
        idx = STATE.attempts[tag] - 1
        STATE.hits.append((tag, self.path))
        sc = STATE.scenarios.get(tag, {})
        if self.path.startswith("/stream"):
            frames = sc.get("stream") or ['data: {"error":{"message":"no scenario"}}\n\n']
            self.send_response(sc.get("status", 200))
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Connection", "close")
            self.end_headers()
            for fr in frames:
                if isinstance(fr, str) and fr.startswith("delay:"):
                    time.sleep(float(fr.split(":")[1]))
                    continue
                self.wfile.write(fr.encode("utf-8") if isinstance(fr, str) else fr)
                self.wfile.flush()
            self.close_connection = True
            return
        seq = sc.get("auto") or [(500, json.dumps({"error": {"message": "no scenario"}}))]
        status, body = seq[idx] if idx < len(seq) else seq[-1]
        data = body.encode("utf-8") if isinstance(body, str) else body
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):  # noqa: N802
        if self.path.startswith("/v1/models"):
            data = json.dumps({"object": "list",
                               "data": [{"id": "deepseek-chat"}]}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return
        self.send_response(404)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, fmt, *args):
        pass


def start():
    srv = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    srv.daemon_threads = True
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    return srv, "http://127.0.0.1:%d" % srv.server_address[1]


def reset():
    STATE.scenarios = {}
    STATE.attempts = {}
    STATE.hits = []
