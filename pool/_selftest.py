# -*- coding: utf-8 -*-
"""pool.py 的离线自检：起两个假 UWA 上游 + 真 pool 进程，验证调度/重试/流式/鉴权。

跑法：  python pool/_selftest.py
不碰服务器、不占公网端口，只在 127.0.0.1 上开几个高位端口。
"""

import http.client
import json
import os
import subprocess
import sys
import socket
import socketserver
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
POOL = os.path.join(HERE, "pool.py")
UWA_TOKEN = "uwa-fake-token"
POOL_TOKEN = "sk-pool-test-9f3c"
PORT = {"ds1": 18301, "ds2": 18302, "pool": 18300}
PORT2 = {"pool2": 18303, "ctl_tcp": 18304}
ALLOWED = {"model", "messages", "stream"}

RESULTS = []
CALLS = {"ds1": 0, "ds2": 0}
CALLS_LOCK = threading.Lock()


def _note(name, msg):
    with CALLS_LOCK:
        CALLS[name] += 1
        n = CALLS[name]
    # ds2 第一次调用返回 429，用来验证「换一台重投」
    if name == "ds2" and n == 1:
        return 429
    return 200


class Fake(BaseHTTPRequestHandler):
    NAME = "?"
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        return

    def _json(self, code, obj):
        raw = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        try:
            self.wfile.write(raw)
        except Exception:
            pass

    def _sse(self, body, slow=False):
        self.close_connection = True
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Connection", "close")
        self.end_headers()
        pieces = ["你好", "，链路", "正常。"]
        gap = 4.2 if slow else 1.6
        try:
            for i, p in enumerate(pieces):
                chunk = {"id": "chatcmpl-fake", "object": "chat.completion.chunk",
                         "model": "chat.deepseek.com",
                         "choices": [{"index": 0, "delta": {"content": p},
                                       "finish_reason": None}]}
                self.wfile.write(("data: %s\n\n" % json.dumps(chunk, ensure_ascii=False)).encode("utf-8"))
                self.wfile.flush()
                if i == 0:
                    time.sleep(gap)  # 制造一个比 KEEPALIVE 更长的空隙，逼出心跳注释
            self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()
        except Exception:
            pass

    def do_POST(self):
        if self.path.rstrip("/") != "/v1/chat/completions":
            return self._json(404, {"error": "not found"})
        if (self.headers.get("Authorization") or "") != "Bearer " + UWA_TOKEN:
            return self._json(401, {"error": {"message": "unauthorized"}})
        n = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(n) if n else b"{}"
        try:
            body = json.loads(raw.decode("utf-8"))
        except Exception:
            return self._json(400, {"error": "bad json"})
        extra = [k for k in body if k not in ALLOWED]
        if extra:
            return self._json(422, {"detail": [{"loc": ["body", extra[0]],
                                                "msg": "extra inputs not allowed"}]})
        code = _note(self.NAME, None)
        if code != 200:
            return self._json(code, {"error": {"message": "rate limit reached"}})
        if body.get("stream"):
            return self._sse(body, "占住" in json.dumps(body, ensure_ascii=False))
        q = body["messages"][-1]["content"]
        return self._json(200, {
            "id": "chatcmpl-fake", "object": "chat.completion", "model": "chat.deepseek.com",
            "choices": [{"index": 0, "message": {"role": "assistant",
                                                 "content": "[%s] 收到：%s" % (self.NAME, q)},
                         "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}})


def make(name, port):
    cls = type(str("H_" + name), (Fake,), {"NAME": name})
    srv = ThreadingHTTPServer(("127.0.0.1", port), cls)
    srv.daemon_threads = True
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


class FakeCtl(socketserver.BaseRequestHandler):
    """模拟 root 侧 ds-pool-ctl 的 TCP 通道：校验 secret + bearer，回 is-active 结果。"""
    def handle(self):
        try:
            head = b""
            while b"\r\n\r\n" not in head:
                chunk = self.request.recv(4096)
                if not chunk:
                    return
                head += chunk
            raw_head = head.split(b"\r\n\r\n", 1)[0]
            lines = raw_head.decode("utf-8", "replace").split("\r\n")
            path = lines[0].split(" ")[1] if lines else ""
            headers = {}
            for ln in lines[1:]:
                if ":" in ln:
                    k, v = ln.split(":", 1)
                    headers[k.strip().lower()] = v.strip()
            if headers.get("x-ctl-secret") != "ctl-secret-abc":
                return self._reply(403, {"ok": False, "error": "tcp channel forbidden"})
            if (headers.get("authorization") or "") != "Bearer " + POOL_TOKEN:
                return self._reply(401, {"ok": False, "error": "invalid api key"})
            import re
            m = re.match(r"^/service/([\w.\-]+)/(is-active|status)$", path)
            if not m:
                return self._reply(404, {"ok": False, "error": "not found: %s" % path})
            return self._reply(200, {"ok": True, "service": m.group(1),
                                     "action": m.group(2), "result": "active",
                                     "stdout": "active", "stderr": ""})
        except Exception as exc:
            try:
                self._reply(500, {"ok": False, "error": repr(exc)})
            except Exception:
                pass

    def _reply(self, code, obj):
        raw = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        msg = ("HTTP/1.1 %d %s\r\nContent-Type: application/json; charset=utf-8\r\n"
               "Content-Length: %d\r\nConnection: close\r\n\r\n" %
               (code, "OK" if code == 200 else "ERR", len(raw)))
        self.request.sendall(msg.encode("utf-8") + raw)


def req(method, path, token=POOL_TOKEN, payload=None, stream=False, timeout=25):
    url = "http://127.0.0.1:%d%s" % (PORT["pool"], path)
    return _req_url(url, method, path, token, payload, stream, timeout)


def req2(method, path, token=POOL_TOKEN, payload=None, stream=False, timeout=25):
    url = "http://127.0.0.1:%d%s" % (PORT2["pool2"], path)
    return _req_url(url, method, path, token, payload, stream, timeout)


def _req_url(url, method, path, token, payload, stream, timeout):
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    r = urllib.request.Request(url, data=data, method=method)
    if token:
        r.add_header("Authorization", "Bearer " + token)
    if data:
        r.add_header("Content-Type", "application/json")
    try:
        resp = urllib.request.urlopen(r, timeout=timeout)
    except urllib.error.HTTPError as e:
        resp = e
    body = resp.read()
    if stream:
        return resp.status, dict(resp.getheaders()), body.decode("utf-8", "replace")
    try:
        return resp.status, dict(resp.getheaders()), json.loads(body.decode("utf-8"))
    except Exception:
        return resp.status, dict(resp.getheaders()), body.decode("utf-8", "replace")


def check(label, cond, detail=""):
    RESULTS.append((bool(cond), label, "" if cond else str(detail)[:300]))
    print(("  PASS  " if cond else "  FAIL  ") + label +
          ("" if cond else "   << " + str(detail)[:400]))


STATS = os.path.join(HERE, "_selftest.stats.json")


def start_pool(mode="w"):
    """拉起一个真实的 pool 子进程（独立端口、独立用量文件），返回 (proc, logfile)。"""
    env = dict(os.environ)
    env.update({
        "PYTHONIOENCODING": "utf-8",
        "POOL_HOST": "127.0.0.1", "POOL_PORT": str(PORT["pool"]),
        "POOL_TOKENS": POOL_TOKEN, "UPSTREAM_TOKEN": UWA_TOKEN,
        "UPSTREAM_1": "ds1|http://127.0.0.1:%d|chat.deepseek.com|acct1|" % PORT["ds1"],
        "UPSTREAM_2": "ds2|http://127.0.0.1:%d|chat.deepseek.com|acct2|" % PORT["ds2"],
        "UPSTREAM_SERVICES": "ds1=uwa-webapi,chrome-webapi,ds2=uwa-webapi2,chrome-webapi2",
        "UPSTREAM_MODELS": "chat.deepseek.com",
        "MODEL_ALIAS": "deepseek-chat=chat.deepseek.com,deepseek-reasoner=chat.deepseek.com",
        "DEFAULT_MODEL": "chat.deepseek.com",
        "PER_UPSTREAM_CONCURRENCY": "1", "COOLDOWN": "6",
        "QUEUE_TIMEOUT": "3", "FIRST_BYTE_TIMEOUT": "10", "KEEPALIVE": "1",
        "STATS_FILE": STATS, "STATS_FLUSH_SECS": "2", "USAGE_KEEP_DAYS": "5",
    })
    logf = open(os.path.join(HERE, "_selftest.pool.log"), mode, encoding="utf-8")
    p = subprocess.Popen([sys.executable, "-u", POOL], env=env, stdout=logf,
                         stderr=subprocess.STDOUT, cwd=HERE)
    return p, logf


def wait_up(trials=50):
    for _ in range(trials):
        try:
            if req("GET", "/health", token=None)[0] == 200:
                return True
        except Exception:
            time.sleep(0.2)
    return False


def stop_pool(p, logf):
    p.terminate()
    try:
        p.wait(timeout=8)
    except Exception:
        p.kill()
    try:
        logf.close()
    except Exception:
        pass


def raw_http(method, path, payload, timeout=20):
    """裸 socket 发一次请求，返回服务器写回的全部字节（用来看 SSE 分帧）。"""
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    head = ("%s %s HTTP/1.1\r\n"
            "Host: 127.0.0.1:%d\r\n"
            "Authorization: Bearer %s\r\n"
            "Content-Type: application/json\r\n"
            "Content-Length: %d\r\n"
            "Connection: close\r\n"
            "\r\n" % (method, path, PORT["pool"], POOL_TOKEN, len(body))).encode("ascii")
    s = socket.create_connection(("127.0.0.1", PORT["pool"]), timeout=timeout)
    s.sendall(head + body)
    buf = b""
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            s.settimeout(5)
            c = s.recv(4096)
        except (socket.timeout, OSError):
            break
        if not c:
            break
        buf += c
    s.close()
    return buf


def main():
    srvs = [make("ds1", PORT["ds1"]), make("ds2", PORT["ds2"])]
    for stale in (STATS, STATS + ".tmp" + str(os.getpid())):
        try:
            os.remove(stale)
        except OSError:
            pass
    p, logf = start_pool("w")
    try:
        if not wait_up():
            check("pool 起来了", False, "见 _selftest.pool.log")
            return 1
        time.sleep(0.3)

        print("[1] 健康检查 / 免鉴权")
        s, h, j = req("GET", "/health", token=None)
        check("/health 200 且两台 ready", s == 200 and sorted(j.get("upstreams_ready") or []) == ["ds1", "ds2"], j)

        print("[2] 鉴权")
        s, h, j = req("POST", "/v1/chat/completions", token=None,
                      payload={"model": "deepseek-chat", "messages": [{"role": "user", "content": "x"}]})
        check("无 token -> 401", s == 401, (s, j))
        s, h, j = req("POST", "/v1/chat/completions", token="sk-wrong",
                      payload={"model": "deepseek-chat", "messages": [{"role": "user", "content": "x"}]})
        check("错 token -> 401", s == 401, (s, j))
        s, h, j = req("GET", "/v1/models", token=None)
        check("?api_key= 不支持时 /v1/models 无 token -> 401", s == 401, (s, j))
        s, h, j = req("GET", "/v1/models?api_key=" + POOL_TOKEN, token=None)
        check("?api_key= 可用", s == 200, (s, j))

        print("[3] 非流式 + 上游嫌弃多余字段 -> 同实例退化成最小请求体")
        s, h, j = req("POST", "/v1/chat/completions",
                      payload={"model": "deepseek-chat", "temperature": 0.7, "user": "u1",
                               "stream_options": {"include_usage": True},
                               "messages": [{"role": "developer", "content": "你是助手"},
                                            {"role": "user", "content": "只回复四个字：链路正常"}]})
        check("HTTP 200", s == 200, (s, j))
        check("model 回显客户端请求的 deepseek-chat", isinstance(j, dict) and j.get("model") == "deepseek-chat", j)
        check("拿到正文", isinstance(j, dict) and "链路正常" in (j["choices"][0]["message"]["content"] or ""), j)
        u = (j or {}).get("usage") or {}
        check("usage 全 0 时按估算补齐", u.get("estimated") is True and u.get("total_tokens", 0) > 0, u)
        check("带上了 X-Pool-Upstream 头", "X-Pool-Upstream" in h or "x-pool-upstream" in h, h)

        print("[4] 流式 + 上游 429 -> 冷却并换一台")
        s, h, txt = req("POST", "/v1/chat/completions", stream=True,
                        payload={"model": "deepseek-reasoner", "stream": True,
                                 "messages": [{"role": "user", "content": "用150字介绍一下东北的冬天"}]})
        check("流式 200", s == 200, s)
        check("收到了 SSE data 帧", "data:" in txt, txt[:200])
        check("收到了 [DONE]", "[DONE]" in txt, txt[-200:])
        got = "".join(((json.loads(l[5:].strip()).get("choices") or [{}])[0]
                       .get("delta", {}) or {}).get("content", "")
                      for l in txt.splitlines()
                      if l.startswith("data:") and "[DONE]" not in l)
        check("正文能拼回来", got == "你好，链路正常。", got)
        check("流里出现 pool-keepalive 心跳", "pool-keepalive" in txt, txt)
        st = [json.loads(l[5:].strip()) for l in txt.splitlines()
              if l.startswith("data:") and "[DONE]" not in l]
        check("每个 chunk 的 model 被改写成请求方模型",
              all(c.get("model") == "deepseek-reasoner" for c in st), st[:2])

        print("[5] 池状态观测")
        s, h, j = req("GET", "/pool/status")
        ups = {u["id"]: u for u in (j or {}).get("upstreams", [])}
        check("/pool/status 200", s == 200 and len(ups) == 2, j)
        check("ds2 因 429 进入冷却", ups.get("ds2", {}).get("state") == "cooling", ups.get("ds2"))
        check("冷却剩余在合理区间", 0 <= ups.get("ds2", {}).get("cooldown_left", -1) <= 6, ups.get("ds2"))
        check("ds1 已成功服务过", ups.get("ds1", {}).get("ok", 0) >= 2, ups.get("ds1"))
        check("统计里有换实例重试", (j or {}).get("stats", {}).get("retry", 0) >= 1, (j or {}).get("stats"))

        print("[6] 手工解除冷却")
        s, h, j = req("POST", "/pool/upstream/ds2/reset")
        check("reset 返回 ok", s == 200 and j.get("ok") is True, (s, j))
        s, h, j = req("GET", "/pool/status")
        ups = {u["id"]: u for u in (j or {}).get("upstreams", [])}
        check("reset 后 ds2 变 ready", ups.get("ds2", {}).get("state") == "ready", ups.get("ds2"))
        s, h, j = req("POST", "/pool/upstream/nope/reset")
        check("不存在的实例 -> 404", s == 404, (s, j))

        print("[7] 单实例并发=1 时排队，超时给 429 pool_busy")
        holders = [{"status": None, "body": None} for _ in range(2)]

        def hold(cell):
            try:
                cell["status"], _, cell["body"] = req(
                    "POST", "/v1/chat/completions", stream=True, timeout=40,
                    payload={"model": "deepseek-chat", "stream": True,
                             "messages": [{"role": "user", "content": "占住一台"}]})
            except Exception as e:
                cell["status"] = "EXC %r" % e

        th = [threading.Thread(target=hold, args=(c,)) for c in holders]
        [t.start() for t in th]
        time.sleep(0.6)
        s, h, j = req("POST", "/v1/chat/completions",
                      payload={"model": "deepseek-chat", "messages": [{"role": "user", "content": "挤不进"}]},
                      timeout=20)
        check("满载排队后返回 429 pool_busy", s == 429 and "pool_busy" in json.dumps(j), (s, j))
        check("429 带 Retry-After 头", (h.get("Retry-After") or h.get("retry-after")) == "5", h)
        [t.join(timeout=45) for t in th]
        check("两路占位流式请求全部 200 收尾", all(c["status"] == 200 for c in holders),
              [c["status"] for c in holders])

        print("[8] 未知端点")
        s, h, j = req("POST", "/v1/responses", payload={"model": "deepseek-chat", "input": "x"})
        check("/v1/responses -> 501", s == 501, (s, j))
        s, h, j = req("GET", "/nope")
        check("未知路径 -> 404", s == 404, (s, j))

        print("[9] 按天/按账号用量 + 落盘")
        s, h, j = req("GET", "/pool/usage")
        check("/pool/usage 200", s == 200, (s, str(j)[:200]))
        u = (j or {}).get("usage") or {}
        today = u.get("today") or {}
        life = u.get("lifetime") or {}
        accts = u.get("accounts") or {}
        check("today.req 覆盖本次全部 chat 请求", today.get("req", 0) >= 5, today)
        check("today.ok / today.fail 分开计数", today.get("ok", 0) >= 3 and today.get("fail", 0) >= 1, today)
        check("今天累计到 token 数", today.get("prompt", 0) + today.get("compl", 0) > 0, today)
        check("lifetime.retry 记下过换实例重试", life.get("retry", 0) >= 1, life)
        check("按账号拆开统计", "ds1" in accts and "ds2" in accts, list(accts))
        check("ds1 今天成功数 >= 2", (accts.get("ds1") or {}).get("ok", 0) >= 2, accts.get("ds1"))
        check("账号有 last_used 时间戳", bool((accts.get("ds1") or {}).get("last_used")), accts.get("ds1"))
        s, h, j = req("GET", "/pool/status")
        check("/pool/status 里带 usage 块", bool(((j or {}).get("usage") or {}).get("today")), str(j)[:200])
        time.sleep(3.0)  # 等一次周期落盘（STATS_FLUSH_SECS=2）
        okfile = os.path.isfile(STATS)
        check("用量已落盘到文件", okfile, STATS)
        disk = {}
        if okfile:
            with open(STATS, encoding="utf-8") as f:
                disk = json.load(f)
        check("落盘内容结构完整", isinstance(disk.get("lifetime"), dict) and
              isinstance(disk.get("accounts"), dict) and "ds1" in (disk.get("accounts") or {}),
              list(disk.keys()) if disk else "空")
        prev_req = (disk.get("lifetime") or {}).get("req", 0)
        check("落盘的 req 与内存一致", prev_req >= today.get("req", -1), (prev_req, today))

        print("[10] 重启后用量不失忆")
        stop_pool(p, logf)
        p, logf = start_pool("a")
        check("重启后能再起来", wait_up(), "见 _selftest.pool.log")
        s, h, j = req("GET", "/pool/usage")
        u2 = (j or {}).get("usage") or {}
        check("lifetime.req 跨重启保留", (u2.get("lifetime") or {}).get("req", -1) >= prev_req,
              (prev_req, u2.get("lifetime")))
        check("按账号统计跨重启保留", "ds1" in (u2.get("accounts") or {}), list(u2.get("accounts") or {}))
        check("本次启动的即时计数从 0 开始",
              (j or {}).get("stats_this_boot", {}).get("req", -1) == 0, (j or {}).get("stats_this_boot"))
        s, h, j = req("POST", "/v1/chat/completions",
                      payload={"model": "deepseek-chat", "messages": [{"role": "user", "content": "重启后还能用"}]})
        check("重启后转发正常", s == 200 and bool(((j or {}).get("choices") or [{}])[0].get("message")), (s, str(j)[:200]))

        print("[11] 流式响应的字节级形状（回归：曾经把响应头发过两遍）")
        before = ((req("GET", "/pool/usage")[2] or {}).get("usage") or {}).get("today", {}).get("compl", 0)
        buf = raw_http("POST", "/v1/chat/completions",
                       {"model": "deepseek-chat", "stream": True,
                        "messages": [{"role": "user", "content": "再说一次"}]})
        check("只写了一个响应头块", buf.count(b"HTTP/1.1 ") == 1, buf.count(b"HTTP/1.1 "))
        check("状态行是 200", buf.startswith(b"HTTP/1.1 200"), buf[:40])
        check("响应头里有 event-stream", b"text/event-stream" in buf.split(b"\r\n\r\n")[0], buf[:200])
        check("data 帧数 >= 3", buf.count(b"data: ") >= 3, buf.count(b"data: "))
        check("以 [DONE] 收尾", b"data: [DONE]" in buf, buf[-120:])
        check("没有多出来的空事件（三连换行）", b"\n\n\n" not in buf, buf.count(b"\n\n\n"))
        check("正文帧都带上了请求方模型", buf.count(b'"model": "deepseek-chat"') >= 3, buf[:200])
        after = ((req("GET", "/pool/usage")[2] or {}).get("usage") or {}).get("today", {}).get("compl", 0)
        check("流式正文被计入 compl（没被二次交付清成 0）", after > before, (before, after))

        print("[12] 管理面板 UI / 服务控制路由")
        s, h, body = req("GET", "/", token=None)
        html = body if isinstance(body, str) else json.dumps(body, ensure_ascii=False)
        check("首页(免鉴权)返回 HTML 200", s == 200 and "text/html" in str(h) and "<title>" in html,
              (s, h.get("Content-Type") or h.get("content-type"), html[:80]))
        s, h, body = req("GET", "/ui.html", token=None)
        page = body if isinstance(body, str) else ""
        check("/ui.html 免鉴权返回 HTML", s == 200 and "<title>" in page, (s, page[:80]))
        s, h, j = req("GET", "/pool/status", token=None)
        check("数据端点无 token -> 401", s == 401, (s, j))
        s, h, j = req("POST", "/pool/service/evil/restart")
        check("白名单外的服务 -> 403", s == 403, (s, j))
        t0 = time.time()
        s, h, j = req("POST", "/pool/service/uwa-webapi/is-active")
        check("ctl 不在(本地无 unix socket) -> 503 且快速返回",
              s == 503 and time.time() - t0 < 8, (s, j))
        print("[12b] ctl TCP 兜底：unix 通道死了，自动切 127.0.0.1 TCP")
        ctl_srv = socketserver.ThreadingTCPServer(("127.0.0.1", PORT2["ctl_tcp"]), FakeCtl)
        ctl_srv.daemon_threads = True
        threading.Thread(target=ctl_srv.serve_forever, daemon=True).start()
        env2 = dict(os.environ)
        env2.update({
            "PYTHONIOENCODING": "utf-8",
            "POOL_HOST": "127.0.0.1", "POOL_PORT": str(PORT2["pool2"]),
            "POOL_TOKENS": POOL_TOKEN, "UPSTREAM_TOKEN": UWA_TOKEN,
            "UPSTREAM_1": "ds1|http://127.0.0.1:%d|chat.deepseek.com|acct1|" % PORT["ds1"],
            "UPSTREAM_SERVICES": "ds1=uwa-webapi,chrome-webapi",
            "UPSTREAM_MODELS": "chat.deepseek.com", "DEFAULT_MODEL": "chat.deepseek.com",
            "PER_UPSTREAM_CONCURRENCY": "1", "COOLDOWN": "6", "QUEUE_TIMEOUT": "3",
            "FIRST_BYTE_TIMEOUT": "10", "KEEPALIVE": "1",
            "STATS_FILE": os.path.join(HERE, "_selftest2.stats.json"),
            "STATS_FLUSH_SECS": "2", "USAGE_KEEP_DAYS": "5",
            "POOL_CTL_SOCKET": os.path.join(HERE, "_no-such-ctl.sock"),
            "POOL_CTL_TCP_PORT": str(PORT2["ctl_tcp"]),
            "DS_POOL_CTL_TOKEN": "ctl-secret-abc",
        })
        logf2 = open(os.path.join(HERE, "_selftest.pool2.log"), "w", encoding="utf-8")
        p2 = subprocess.Popen([sys.executable, "-u", POOL], env=env2, stdout=logf2,
                              stderr=subprocess.STDOUT, cwd=HERE)
        up2 = False
        for _ in range(50):
            try:
                r = urllib.request.urlopen(
                    "http://127.0.0.1:%d/health" % PORT2["pool2"], timeout=2)
                r.read()
                up2 = True
                break
            except Exception:
                time.sleep(0.2)
        check("pool2 起来（TCP 兜底测试实例）", up2, "未就绪")
        t0 = time.time()
        s, h, j = req2("POST", "/pool/service/uwa-webapi/is-active")
        check("unix 不存在时自动走 TCP 兜底 -> 200 且拿到 active",
              s == 200 and (j or {}).get("result") == "active" and time.time() - t0 < 8,
              (s, j))
        p2.terminate()
        try:
            p2.wait(timeout=8)
        except Exception:
            p2.kill()
        logf2.close()
        ctl_srv.shutdown()
        for stale2 in ("_selftest2.stats.json",):
            try:
                os.remove(os.path.join(HERE, stale2))
            except OSError:
                pass
        s, h, body = req("GET", "/", token=None)
        page = body if isinstance(body, str) else ""
        check("页面内容不包含网关令牌", POOL_TOKEN not in page, "泄漏!")
        s, h, j = req("GET", "/pool/status")
        ups = {u["id"]: u for u in (j or {}).get("upstreams", [])}
        check("snapshot 带 services 映射",
              ups.get("ds1", {}).get("services") == ["uwa-webapi", "chrome-webapi"] and
              ups.get("ds2", {}).get("services") == ["uwa-webapi2", "chrome-webapi2"],
              {k: v.get("services") for k, v in ups.items()})
        s, h, j = req("GET", "/pool/info")
        eps = " ".join((j or {}).get("endpoints") or [])
        check("/pool/info 列出服务控制端点", "/pool/service/" in eps and "/ui.html" in eps, eps)
    finally:
        stop_pool(p, logf)
        for s_ in srvs:
            s_.shutdown()

    bad = [r for r in RESULTS if not r[0]]
    print("\n==== 自检结果: %d 项，失败 %d 项 ====" % (len(RESULTS), len(bad)))
    for okk, label, detail in bad:
        print("  FAIL %s  << %s" % (label, detail))
    print("\n---- pool 侧日志 ----")
    print(open(os.path.join(HERE, "_selftest.pool.log"), encoding="utf-8",
               errors="replace").read().strip())
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
