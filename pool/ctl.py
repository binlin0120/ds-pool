#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ds-pool-ctl -- root 侧最小控制助手。

池网关 ds-pool 跑在 User=dspool + NoNewPrivileges=yes，自己无法执行 systemctl。
需要特权的运维动作（重启浏览器 / 重启 UWA 桥接）通过本助手转发：
  * unix socket（/run/ds-pool-ctl/ctl.sock, 0660 root:dspool）+ TCP 127.0.0.1 兜底双通道
    （部分机器上 AliYunDun 等安全 agent 会拖慢 AF_UNIX 写回，pool.py 探测失败后自动切 TCP）
  * 鉴权：复用 /opt/ds-pool/pool.env 里的 POOL_TOKENS；TCP 通道额外校验 DS_POOL_CTL_TOKEN
  * 白名单：服务名 + 动作都硬编码，其余一律拒绝
  * 每个服务之间最少 3 秒一次，防止误刷重启风暴
"""

import grp
import json
import os
import re
import socket
import socketserver
import subprocess
import threading
import time

SOCK = os.environ.get("POOL_CTL_SOCKET", "/run/ds-pool-ctl/ctl.sock")
TCP_PORT = int(os.environ.get("POOL_CTL_TCP_PORT", "8399"))
SECRET = os.environ.get("DS_POOL_CTL_TOKEN", "")
TOKENS = {t.strip() for t in (os.environ.get("POOL_TOKENS", "") or "").split(",") if t.strip()}
SERVICES = {s.strip() for s in (os.environ.get("CTL_SERVICES") or
                                "chrome-webapi,chrome-webapi2,uwa-webapi,uwa-webapi2,ds-pool").split(",") if s.strip()}
ACTIONS = {"restart", "start", "stop", "is-active", "status", "switch"}
MIN_INTERVAL = float(os.environ.get("CTL_MIN_INTERVAL", "3"))
SWITCH_SCRIPT = "/opt/ds-pool/switch_account.sh"
PROFILES_FILE = "/opt/ds-pool/profiles.conf"

_last = {}
_lock = threading.Lock()


def log(msg):
    print(time.strftime("%Y-%m-%dT%H:%M:%S") + " [ctl] " + msg, flush=True)


def _run(name, action):
    now = time.time()
    err = _rate_limit(name, action, now)
    if err:
        return 429, err, ""
    try:
        p = subprocess.run(["systemctl", action, name + ".service"],
                           capture_output=True, text=True, timeout=30)
        return p.returncode, (p.stdout or "").strip()[-400:], (p.stderr or "").strip()[-400:]
    except subprocess.TimeoutExpired:
        return 124, "", "systemctl %s 超时" % action
    except Exception as exc:
        return 1, "", repr(exc)


def _rate_limit(name, action, now):
    with _lock:
        prev = _last.get(name, 0.0)
        if action in ("restart", "start", "stop", "switch") and now - prev < MIN_INTERVAL:
            return "操作太频繁（最小间隔 %ds），请稍后再试" % int(MIN_INTERVAL)
        if action in ("restart", "start", "stop", "switch"):
            _last[name] = now
    return None


def _known_accounts():
    """读账号注册表，切号白名单，防止任意参数打到 systemctl/脚本。"""
    out = set()
    try:
        with open(PROFILES_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "|" not in line:
                    continue
                out.add(line.split("|", 1)[0].strip())
    except OSError:
        pass
    return out


def _run_switch(name, acct):
    """切号动作：switch_account.sh <dsN> <account>，最长时间放宽到 240s。"""
    if not acct or not re.fullmatch(r"[A-Za-z0-9._\-]{1,32}", acct):
        return 400, "", "bad account value"
    if acct not in _known_accounts():
        return 403, "", "account not in whitelist"
    err = _rate_limit(name, "switch", time.time())
    if err:
        return 429, "", err
    n = "".join(ch for ch in name if ch.isdigit()) or "1"
    try:
        p = subprocess.run(["bash", SWITCH_SCRIPT, "ds" + n, acct],
                           capture_output=True, text=True, timeout=240)
        return p.returncode, (p.stdout or "").strip()[-1600:], (p.stderr or "").strip()[-400:]
    except subprocess.TimeoutExpired:
        return 124, "", "switch 超时"
    except Exception as exc:
        return 1, "", repr(exc)


class Handler(socketserver.BaseRequestHandler):
    """裸 socket 处理，不用 makefile/StreamRequestHandler。

    实测（本机 Windows + Linux 服务器）StreamRequestHandler 的 rfile/wfile
    在这两台机器上都会出现「读/写回被拖到超时」的诡异现象，而裸 socket
    环回 RTT 是 0.3ms。所以这里全部走 self.request.recv/sendall。
    """
    def handle(self):
        try:
            self._serve()
        except Exception as exc:
            log("handler error: %r" % exc)
            self._reply(500, {"ok": False, "error": repr(exc)})

    def _serve(self):
        head = b""
        while b"\r\n\r\n" not in head:
            chunk = self.request.recv(4096)
            if not chunk:
                return
            head += chunk
            if len(head) > 65536:
                return self._reply(400, {"ok": False, "error": "header too big"})
        raw_head, rest = head.split(b"\r\n\r\n", 1)
        lines = raw_head.decode("utf-8", "replace").split("\r\n")
        try:
            method, path, _ver = lines[0].split(" ", 2)
        except ValueError:
            return self._reply(400, {"ok": False, "error": "bad request line"})
        headers = {}
        for ln in lines[1:]:
            if ":" in ln:
                k, v = ln.split(":", 1)
                headers[k.strip().lower()] = v.strip()
        try:
            clen = int(headers.get("content-length") or 0)
        except ValueError:
            clen = 0
        body = rest
        while len(body) < clen:
            more = self.request.recv(clen - len(body))
            if not more:
                break
            body += more

        if self.server.address_family == socket.AF_INET:
            if not SECRET or headers.get("x-ctl-secret") != SECRET:
                log("tcp auth_fail from %s (bad/absent ctl secret)" %
                    (self.client_address,))
                return self._reply(403, {"ok": False, "error": "tcp channel forbidden"})
        if method == "GET" and path.rstrip("/") == "/health":
            return self._reply(200, {"ok": True, "service": "ds-pool-ctl",
                                     "services": sorted(SERVICES), "actions": sorted(ACTIONS)})
        auth = headers.get("authorization") or ""
        key = auth[7:].strip() if auth.lower().startswith("bearer ") else ""
        if not any(_eq(key, t) for t in TOKENS):
            log("auth_fail from %s" % (self.client_address,))
            return self._reply(401, {"ok": False, "error": "invalid api key"})
        m = re.match(r"^/service/([\w.\-]+)/(restart|start|stop|is-active|status|switch)$", path)
        if not m:
            return self._reply(404, {"ok": False, "error": "not found: %s" % path})
        name, action = m.group(1), m.group(2)
        if name not in SERVICES:
            return self._reply(403, {"ok": False, "error": "forbidden service: %s" % name})
        if action == "switch":
            if not name.startswith("chrome-"):
                return self._reply(403, {"ok": False, "error": "switch only allowed on chrome services"})
            acct = headers.get("x-switch-account") or ""
            log("switch %s -> %s" % (name, acct))
            code, out, err = _run_switch(name, acct)
            self._reply(200 if code == 0 else 503,
                        {"ok": code == 0, "service": name, "action": "switch",
                         "result": out.strip()[-800:], "stdout": out, "stderr": err})
            return
        log("%s %s" % (action, name))
        code, out, err = _run(name, action)
        result = out.strip() if (action in ("is-active", "status") and code == 0) else code
        self._reply(200 if code == 0 else 503,
                    {"ok": code == 0, "service": name, "action": action,
                     "result": result, "stdout": out, "stderr": err})

    def _reply(self, code, obj):
        raw = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        msg = ("HTTP/1.1 %d %s\r\nContent-Type: application/json; charset=utf-8\r\n"
               "Content-Length: %d\r\nConnection: close\r\n\r\n" %
               (code, "OK" if code == 200 else "ERR", len(raw)))
        self.request.sendall(msg.encode("utf-8") + raw)


def _eq(a, b):
    if len(a) != len(b):
        return False
    r = 0
    for x, y in zip(a, b):
        r |= ord(x) ^ ord(y)
    return r == 0


class Server(socketserver.ThreadingUnixStreamServer):
    daemon_threads = True
    allow_reuse_address = True


class TCPServer(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True


def main():
    if not TOKENS:
        log("WARN: POOL_TOKENS 为空 —— 任何本地进程都能控制服务")
    d = os.path.dirname(SOCK)
    if d:
        os.makedirs(d, exist_ok=True)
    for path in (SOCK,):
        if os.path.exists(path):
            os.unlink(path)
    srv = Server(SOCK, Handler)
    os.chmod(SOCK, 0o660)
    try:
        g = grp.getgrnam("dspool")
        os.chown(SOCK, os.getuid(), g.gr_gid)
    except KeyError:
        pass
    log("ctl listening on %s services=%s" % (SOCK, ",".join(sorted(SERVICES))))
    tcp = None
    if TCP_PORT:
        try:
            tcp = TCPServer(("127.0.0.1", TCP_PORT), Handler)
            log("ctl tcp fallback on 127.0.0.1:%d secret=%s" %
                (TCP_PORT, "set" if SECRET else "NOT SET"))
        except OSError as exc:
            log("tcp fallback 不可用: %s" % exc)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        srv.server_close()
        if tcp is not None:
            tcp.server_close()
        if os.path.exists(SOCK):
            os.unlink(SOCK)


if __name__ == "__main__":
    main()
