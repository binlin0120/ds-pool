#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ds-pool -- 号池网关（纯 Python 标准库，零第三方依赖）

服务器位置: /opt/ds-pool/pool.py
配置文件  : /opt/ds-pool/pool.env  (systemd EnvironmentFile, 权限 600)
服务名    : ds-pool.service

定位：在多台 universal-web-api (UWA) 实例前面放一个统一的 OpenAI 兼容入口。
  * 轮询 + 最少在途优先调度
  * 单实例并发限制（默认 1，一个浏览器同时只跑一个会话）
  * 上游 429/5xx/连接错误/掉登录 -> 冷却该实例，并在响应首字节之前重投另一实例
  * SSE 流式透传 + 注释心跳，避免客户端或中间层超时断连
  * /pool/status 观测每台实例的在途、成功失败、冷却剩余、最近错误
  * /pool/usage 按天、按账号统计请求数与 token（落盘，重启不丢）

它不登录、不换号、不碰 Chrome，纯转发+调度；停掉服务就完全回到原状。
"""

import hmac
import http.client
import itertools
import json
import os
import re
import signal
import socket
import threading
import time
import tempfile
import urllib.error
import urllib.parse
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

START_TS = time.time()


def _env(name, default=None):
    v = os.environ.get(name)
    return v if v not in (None, "") else default


def _env_int(name, default):
    try:
        return int(str(_env(name, default)).strip())
    except (TypeError, ValueError):
        return default


POOL_HOST = _env("POOL_HOST", "0.0.0.0")
POOL_PORT = _env_int("POOL_PORT", 8288)
TOKENS = {t.strip() for t in (_env("POOL_TOKENS", "") or "").split(",") if t.strip()}
UPSTREAM_TOKEN = _env("UPSTREAM_TOKEN", "") or ""

COOLDOWN = _env_int("COOLDOWN", 300)            # 失败后冷却秒数
QUEUE_TIMEOUT = _env_int("QUEUE_TIMEOUT", 180)  # 全忙时排队等待秒数
FIRST_BYTE_TIMEOUT = _env_int("FIRST_BYTE_TIMEOUT", 90)
KEEPALIVE = _env_int("KEEPALIVE", 12)           # 流式心跳间隔，0 关闭
PER_CONC = _env_int("PER_UPSTREAM_CONCURRENCY", 1)
STATS_FLUSH_SECS = _env_int("STATS_FLUSH_SECS", 15)   # 用量落盘间隔秒，0 = 只在退出时写
USAGE_KEEP_DAYS = _env_int("USAGE_KEEP_DAYS", 8)      # 按天历史保留多少天
STATS_FILE = _env("STATS_FILE") or ""

DEFAULT_MODEL = _env("DEFAULT_MODEL", "chat.deepseek.com")
UPSTREAM_MODELS = [m.strip() for m in (_env("UPSTREAM_MODELS", "") or "").split(",") if m.strip()]
MODEL_ALIAS = {}
for _pair in (_env("MODEL_ALIAS", "") or "").split(","):
    if "=" in _pair:
        _k, _v = _pair.split("=", 1)
        if _k.strip():
            MODEL_ALIAS[_k.strip()] = _v.strip()

# UWA 驱动的是网页输入框，多余字段只会引发上游校验错误，一律剥掉
STRIP_KEYS = {k.strip() for k in (_env(
    "STRIP_KEYS",
    "user,web_search_options,reasoning_effort,verbosity,store,"
    "safety_identifier,prompt_cache_key,modalities,audio,prediction,service_tier,seed") or ""
).split(",") if k.strip()}

# 上游回包出现这些词，基本等于掉登录或被风控，要冷却并换一台重试
BAD_MARKERS = ("找不到输入框", "workflow_step_failed", "未登录", "风控", "触发",
               "rate limit", "too many requests", "login", "cookie")

# 反代层语言/风格指令（治「对话偶尔回英文」）：非空时作为第一条 system 消息
# 注入到每次请求。空字符串 = 不注入，完全保持原行为。
LANG_DIRECTIVE = (_env("POOL_LANG_DIRECTIVE", "") or "").strip()

# ---------------------------------------------------------------- 管理面板 UI
UI_FILE = _env("UI_FILE") or os.path.join(os.path.dirname(os.path.abspath(__file__)), "ui.html")
try:
    with open(UI_FILE, "rb") as _f:
        UI_HTML = _f.read()
    UI_LOADED = True
except OSError:
    UI_HTML = b""
    UI_LOADED = False


def log(msg):
    print(time.strftime("%Y-%m-%dT%H:%M:%S") + " " + msg, flush=True)


class Upstream(object):
    """一台 UWA 实例 = 一个浏览器 = 一个登录账号。"""

    def __init__(self, idx, uid, base, model, acct, token):
        self.idx = idx
        self.id = uid
        self.base = base.rstrip("/")
        self.model = model or DEFAULT_MODEL
        self.acct = acct or "?"
        self.token = token or UPSTREAM_TOKEN
        self.sem = threading.Semaphore(PER_CONC)
        self.lock = threading.Lock()
        self.inflight = 0
        self.cooldown_until = 0.0
        self.ok = 0
        self.fail = 0
        self.last_error = None
        self.last_ok_at = 0.0
        self.last_pick = 0.0
        self.lat_ema = 0.0
        u = urllib.parse.urlparse(self.base)
        self.host = u.hostname or "127.0.0.1"
        self.port = u.port or (443 if u.scheme == "https" else 80)

    def cooling(self, now=None):
        return self.cooldown_until > (now or time.time())

    def cooldown_left(self):
        return max(0, int(self.cooldown_until - time.time()))

    def mark_ok(self, secs):
        with self.lock:
            self.ok += 1
            self.last_ok_at = time.time()
            self.last_error = None
            self.lat_ema = secs if self.lat_ema <= 0 else (self.lat_ema * 0.7 + secs * 0.3)

    def mark_fail(self, msg, cool):
        with self.lock:
            self.fail += 1
            self.last_error = (msg or "")[:300]
            if cool:
                self.cooldown_until = time.time() + COOLDOWN
            else:
                self.cooldown_until = max(self.cooldown_until, time.time() + min(COOLDOWN, 20))

    def snapshot(self):
        now = time.time()
        return {
            "id": self.id, "account": self.acct, "base": self.base, "model": self.model,
            "inflight": self.inflight,
            "state": "cooling" if self.cooling(now) else ("busy" if self.inflight else "ready"),
            "cooldown_left": self.cooldown_left(),
            "ok": self.ok, "fail": self.fail, "lat_ema": round(self.lat_ema, 2),
            "last_ok_at": int(self.last_ok_at) or None, "last_error": self.last_error,
            "services": list(getattr(self, "services", ()) or ()),
        }


# 每台实例对应的系统服务（桥接 / 浏览器），供管理面板做「重启」按钮
UPSTREAM_SERVICES = {}
_cur_id = None
for _tok in (_env("UPSTREAM_SERVICES", "") or "").split(","):
    _tok = _tok.strip()
    if not _tok:
        continue
    if "=" in _tok:
        _kid, _rest = _tok.split("=", 1)
        _kid = _kid.strip()
        _rest = _rest.strip()
        _cur_id = _kid if _kid and _rest else None
        if _kid and _rest:
            UPSTREAM_SERVICES[_kid] = [_rest]
    elif _cur_id:
        UPSTREAM_SERVICES.setdefault(_cur_id, []).append(_tok)


def _default_services(uid):
    """env 没配 UPSTREAM_SERVICES 时按实例号推：ds1 -> uwa-webapi/chrome-webapi，ds2 -> uwa-webapi2/..."""
    n = "".join(ch for ch in uid if ch.isdigit()) or "1"
    suf = "" if n == "1" else n
    return ["uwa-webapi" + suf, "chrome-webapi" + suf]


UPSTREAMS = []
for _i in range(1, 17):
    _raw = _env("UPSTREAM_%d" % _i)
    if not _raw:
        continue
    _p = [x.strip() for x in _raw.split("|")]
    if len(_p) < 2 or not _p[1]:
        continue
    UPSTREAMS.append(Upstream(
        len(UPSTREAMS), _p[0], _p[1],
        _p[2] if len(_p) > 2 and _p[2] else DEFAULT_MODEL,
        _p[3] if len(_p) > 3 and _p[3] else "?",
        _p[4] if len(_p) > 4 and _p[4] else "",
    ))

for _u in UPSTREAMS:
    _u.services = UPSTREAM_SERVICES.get(_u.id) or _default_services(_u.id)
CTL_ALLOWED = sorted({s for vs in UPSTREAM_SERVICES.values() for s in vs} or
                     {s for u in UPSTREAMS for s in u.services})
CTL_SOCKET = _env("POOL_CTL_SOCKET", "/run/ds-pool-ctl/ctl.sock")
CTL_CONNECT_TIMEOUT = _env_int("POOL_CTL_CONNECT_TIMEOUT", 5)
CTL_READ_TIMEOUT = _env_int("POOL_CTL_READ_TIMEOUT", 45)
CTL_TCP_PORT = _env_int("POOL_CTL_TCP_PORT", 8399)
CTL_SECRET = _env("DS_POOL_CTL_TOKEN", "")

PROFILES_FILE = "/opt/ds-pool/profiles.conf"
LOGIN_STATE_FILE = _env("DS_POOL_LOGIN_STATE", "/var/lib/ds-pool/login_state.json")
LOGIN_STATE_CACHE_TTL = _env_int("LOGIN_STATE_CACHE_TTL", 10)
LOGIN_STATE_CACHE = {"ts": 0.0, "data": None}

_RR = itertools.count()
STATS = {"req": 0, "ok": 0, "fail": 0, "retry": 0, "auth_fail": 0}
STATS_LOCK = threading.Lock()


def _today():
    return time.strftime("%Y-%m-%d")


def _blank():
    return {"req": 0, "ok": 0, "fail": 0, "retry": 0, "prompt": 0, "compl": 0}


def _load_profiles():
    """读账号注册表 profiles.conf，返回 [{account, profile}]。读不到返回 []。"""
    out = []
    try:
        with open(PROFILES_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "|" not in line:
                    continue
                name, path = line.split("|", 1)
                name, path = name.strip(), path.strip()
                if name and path:
                    out.append({"account": name, "profile": path})
    except OSError:
        pass
    return out


def _load_login_state():
    """读健康探针写下的登录状态；读不到返回 None（面板显示「未知」即可）。"""
    try:
        with open(LOGIN_STATE_FILE, "r", encoding="utf-8") as f:
            j = json.load(f)
        if isinstance(j, dict) and isinstance(j.get("instances"), dict):
            return j
    except (OSError, ValueError):
        pass
    return None


def _login_state_cached():
    """带 TTL 缓存的登录状态：探针每 120s 才写一次，10s 缓存足够实时又免得每次请求读盘。"""
    now = time.time()
    if now - LOGIN_STATE_CACHE.get("ts", 0.0) < LOGIN_STATE_CACHE_TTL:
        return LOGIN_STATE_CACHE.get("data")
    data = _load_login_state()
    LOGIN_STATE_CACHE["ts"] = now
    LOGIN_STATE_CACHE["data"] = data
    return data


def _login_entry(login_state, uid):
    """返回该实例的探针条目（dict）或 None。"""
    if not isinstance(login_state, dict):
        return None
    inst = login_state.get("instances")
    if not isinstance(inst, dict):
        return None
    e = inst.get(uid)
    return e if isinstance(e, dict) else None


def _login_unhealthy(login_state, uid):
    """探针标记 unhealthy（封号停机/连续探活失败）→ 调度与健康统计都当不可用。"""
    e = _login_entry(login_state, uid)
    return bool(e and e.get("unhealthy"))


def _login_wake(login_state):
    """所有停机实例里最近的计划开机时间（epoch）；没有返回 None。"""
    nearest = None
    if not isinstance(login_state, dict):
        return None
    inst = login_state.get("instances")
    if not isinstance(inst, dict):
        return None
    now = time.time()
    for e in inst.values():
        if not (isinstance(e, dict) and e.get("unhealthy")):
            continue
        try:
            w = float(e.get("wake_at") or 0)
        except (TypeError, ValueError):
            w = 0.0
        if w > now and (nearest is None or w < nearest):
            nearest = w
    return nearest


def _fmt_ts(ts):
    try:
        return time.strftime("%m-%d %H:%M", time.localtime(float(ts)))
    except (TypeError, ValueError):
        return ""


def _nores():
    """交付中断/上游回了非 JSON 时使用的空结果，省掉到处写默认字典。"""
    return {"text": "", "prompt_tokens": 0, "completion_tokens": 0, "estimated": False}


def _num(src, k):
    try:
        return max(0, int(src.get(k) or 0))
    except (TypeError, ValueError):
        return 0


def _merge(dst, src):
    for k in ("req", "ok", "fail", "retry", "prompt", "compl"):
        dst[k] = _num(src, k)
    return dst


def resolve_stats_path():
    """挑一个真正能写的地方存用量：显式配置 > systemd StateDirectory > 代码同目录 > /tmp。

    服务以 User=dspool 跑，/opt/ds-pool 是 root 拥有的，直接写那里会失败，
    所以必须逐个试可写性，而不是猜路径。
    """
    cands = []
    if STATS_FILE:
        cands.append(STATS_FILE)
    cands.append("/var/lib/ds-pool/stats.json")
    cands.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "stats.json"))
    cands.append(os.path.join(tempfile.gettempdir(), "ds-pool.stats.json"))
    for p in cands:
        try:
            d = os.path.dirname(p) or "."
            if not os.path.isdir(d):
                os.makedirs(d)
            if os.path.isdir(d) and os.access(d, os.W_OK):
                return p
        except Exception:
            continue
    return cands[-1]


class Usage(object):
    """按天 + 按账号累计请求量与 token，周期性落盘。

    只存计数，不存正文、不存密钥；写盘走 tmp + os.replace，systemd 停机
    也不会留下半个 JSON。重启后历史还在，方便判断哪个账号今天跑得最多。
    """

    def __init__(self, path="", keep_days=USAGE_KEEP_DAYS, flush_secs=STATS_FLUSH_SECS):
        self.path = path
        self.keep_days = max(1, keep_days)
        self.flush_secs = max(0, flush_secs)
        self.lock = threading.Lock()
        self._dirty = False
        self._stop = threading.Event()
        self.load_errors = 0
        self.data = {"day": _today(), "lifetime": _blank(), "days": {}, "accounts": {}}
        self._load()

    # ------------------------------------------------------------ 内部
    def _acct(self, uid):
        a = self.data["accounts"].get(uid)
        if a is None:
            a = {"total": _blank(), "days": {}, "last_used": None}
            self.data["accounts"][uid] = a
        return a

    @staticmethod
    def _bucket(table, day):
        b = table.get(day)
        if b is None:
            b = _blank()
            table[day] = b
        return b

    def _trim(self):
        tables = [self.data["days"]]
        tables.extend(a["days"] for a in self.data["accounts"].values())
        for table in tables:
            keys = sorted(table)
            if len(keys) > self.keep_days:
                for k in keys[:len(keys) - self.keep_days]:
                    table.pop(k, None)

    def _load(self):
        if not self.path or not os.path.isfile(self.path):
            return
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                raw = json.load(f)
            if not isinstance(raw, dict):
                raise ValueError("顶层不是对象")
            if isinstance(raw.get("lifetime"), dict):
                _merge(self.data["lifetime"], raw["lifetime"])
            days = raw.get("days")
            if isinstance(days, dict):
                for k, v in days.items():
                    if isinstance(v, dict):
                        self.data["days"][str(k)] = _merge(_blank(), v)
            accts = raw.get("accounts")
            if isinstance(accts, dict):
                for uid, a in accts.items():
                    if not isinstance(a, dict):
                        continue
                    slot = self._acct(str(uid))
                    if isinstance(a.get("total"), dict):
                        _merge(slot["total"], a["total"])
                    if isinstance(a.get("days"), dict):
                        for k, v in a["days"].items():
                            if isinstance(v, dict):
                                slot["days"][str(k)] = _merge(_blank(), v)
                    slot["last_used"] = a.get("last_used")
            self._trim()
        except Exception as exc:
            self.load_errors += 1
            log("!! 用量文件读不动（%r），按全新计数继续；旧文件保留在 %s" % (exc, self.path))

    # ------------------------------------------------------------ 记录
    def note(self, uid=None, ok=False, prompt=0, compl=0):
        day = _today()
        with self.lock:
            self.data["day"] = day
            targets = [self.data["lifetime"], self._bucket(self.data["days"], day)]
            if uid:
                a = self._acct(uid)
                targets.append(a["total"])
                targets.append(self._bucket(a["days"], day))
                a["last_used"] = int(time.time())
            p, c = max(0, int(prompt or 0)), max(0, int(compl or 0))
            for b in targets:
                b["req"] += 1
                b["ok" if ok else "fail"] += 1
                if ok:
                    b["prompt"] += p
                    b["compl"] += c
            self._trim()
            self._dirty = True

    def note_retry(self):
        with self.lock:
            self.data["lifetime"]["retry"] += 1
            self._dirty = True

    # ------------------------------------------------------------ 输出
    def snapshot(self):
        day = _today()
        with self.lock:
            today = dict(self._bucket(self.data["days"], day))
            out = {"date": day, "today": today, "lifetime": dict(self.data["lifetime"]),
                   "accounts": {}, "stats_file": self.path, "history": {}}
            for uid, a in self.data["accounts"].items():
                cur = dict(a["days"].get(day) or _blank())
                cur["total"] = dict(a["total"])
                cur["last_used"] = a["last_used"]
                cur["active_days"] = len(a["days"])
                out["accounts"][uid] = cur
            for k in sorted(self.data["days"]):
                out["history"][k] = dict(self.data["days"][k])
        today["tokens"] = today.get("prompt", 0) + today.get("compl", 0)
        return out

    # ------------------------------------------------------------ 落盘
    def flush(self, force=False):
        with self.lock:
            if not self._dirty and not force:
                return False
            self._dirty = False
            blob = json.dumps(self.data, ensure_ascii=False, sort_keys=True)
        if not self.path:
            return False
        try:
            tmp = "%s.tmp%d" % (self.path, os.getpid())
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(blob)
            os.replace(tmp, self.path)
            return True
        except Exception as exc:
            with self.lock:
                self._dirty = True
            log("!! 用量落盘失败 %s: %r" % (self.path, exc))
            return False

    def run(self):
        while not self._stop.wait(self.flush_secs or 30):
            self.flush()

    def close(self):
        self._stop.set()
        self.flush()


USAGE = Usage(resolve_stats_path())


def map_model(m):
    if not m:
        return DEFAULT_MODEL
    m = MODEL_ALIAS.get(m, m)
    if UPSTREAM_MODELS and m not in UPSTREAM_MODELS:
        return DEFAULT_MODEL
    return m


def est_tokens(text):
    if not text:
        return 0
    cjk = sum(1 for ch in text if "\u4e00" <= ch <= "\u9fff")
    return max(1, int(cjk / 1.3 + (len(text) - cjk) / 4))


def flatten_content(c):
    """content 归一成纯文本：数组型 content、part 字典都吃掉。"""
    if isinstance(c, str):
        return c
    if c is None:
        return ""
    if isinstance(c, list):
        out = []
        for part in c:
            if isinstance(part, str):
                out.append(part)
            elif isinstance(part, dict):
                t = part.get("text") or part.get("content")
                if isinstance(t, str):
                    out.append(t)
        return "\n".join(x for x in out if x)
    return str(c)


def _resp_content_text(content):
    """Responses 的 content 字段转纯文本：字符串直接收；列表收 input_text/output_text/text/refusal。"""
    if isinstance(content, str):
        return content
    if content is None:
        return ""
    if isinstance(content, list):
        out = []
        for part in content:
            if isinstance(part, str):
                out.append(part)
            elif isinstance(part, dict):
                t = part.get("type")
                if not isinstance(t, str) and isinstance(part.get("text"), str):
                    t = "text"
                if t in ("input_text", "output_text", "text", "refusal"):
                    v = part.get("text") if t != "refusal" else part.get("refusal")
                    if isinstance(v, str):
                        out.append(v)
        return "\n".join(x for x in out if x)
    return str(content)


def _media_ref(part):
    """把 OpenAI / Responses / Anthropic 三种附件写法收敛成 (ref, detail, filename)。

    只认 http(s) URL 与 data URI；本地路径一律不认（UWA 也不收），宁可丢附件不能编造。
    """
    if not isinstance(part, dict):
        return None, None, None
    detail = part.get("detail")
    holders = []
    for key in ("image_url", "input_image", "file", "input_file", "file_data",
                "video_url", "audio_url"):
        v = part.get(key)
        if isinstance(v, dict):
            holders.append(v)
        elif isinstance(v, str) and v.strip():
            holders.append({"url": v})
    src = part.get("source")
    if isinstance(src, dict) and str(src.get("type") or "").lower() == "base64":
        data = src.get("data")
        if isinstance(data, str) and data.strip():
            media = str(src.get("media_type") or part.get("mime_type")
                        or "image/png").split(";", 1)[0].strip() or "image/png"
            holders.append({"url": "data:%s;base64,%s" % (media, data.strip())})
    filename = part.get("filename") or part.get("title")
    for h in holders:
        if not filename:
            filename = h.get("filename") or h.get("name")
        ref = h.get("url") or h.get("file_url") or h.get("file_data") or h.get("data")
        if isinstance(ref, str):
            ref = ref.strip()
            if ref.startswith(("http://", "https://", "data:")):
                # chat 风格把 detail 塞在 image_url 里，OpenAI 官方示例两种写法都有
                return ref, (detail or h.get("detail")), filename
    return None, None, filename


def split_content(c):
    """content -> (纯文本, 可转发的附件 part 列表)。

    文本沿用旧行为；图片/文件原样收敛成 UWA 认的 image_url / file part，
    不认识的类型（音频视频、file_id）静默丢弃，保持老逻辑只发文本。
    """
    if c is None:
        return "", []
    if isinstance(c, str):
        return c, []
    if not isinstance(c, list):
        return str(c), []
    texts, parts = [], []
    for p in c:
        if isinstance(p, str):
            if p:
                texts.append(p)
            continue
        if not isinstance(p, dict):
            continue
        t = str(p.get("type") or "").lower().strip()
        if not t and isinstance(p.get("text"), str):
            t = "text"
        if t in ("image_url", "input_image", "image"):
            ref, detail, _fn = _media_ref(p)
            if ref:
                iu = {"url": ref}
                if detail:
                    iu["detail"] = detail
                parts.append({"type": "image_url", "image_url": iu})
            continue
        if t in ("file", "input_file", "document"):
            ref, _d, filename = _media_ref(p)
            if ref:
                body = {"file_data": ref} if ref.startswith("data:") else {"file_url": ref}
                if filename:
                    body["filename"] = filename
                if not filename and ref.startswith("data:"):
                    body["filename"] = "attachment.bin"
                parts.append({"type": "file", "file": body})
            continue
        v = p.get("text") or p.get("content") or p.get("refusal")
        if isinstance(v, str) and v:
            texts.append(v)
    return "\n".join(x for x in texts if x), parts


def content_to_wire(text, parts):
    """没附件时保持字符串 content（完全兼容旧行为），有附件才升级成数组。"""
    if not parts:
        return text
    out = []
    if text:
        out.append({"type": "text", "text": text})
    out.extend(parts)
    return out


PART_TYPES = ("text", "input_text", "output_text", "refusal", "summary_text",
              "image_url", "input_image", "image", "file", "input_file",
              "document", "audio", "input_audio", "video_url", "audio_url")


def _looks_like_part(v):
    """判断列表元素是否像内容块（用来区分"附件数组"和"结构化 JSON 结果"）。"""
    if isinstance(v, str):
        return True
    if not isinstance(v, dict):
        return False
    if str(v.get("type") or "").strip().lower() in PART_TYPES:
        return True
    return isinstance(v.get("text"), str) and not v.get("type")


def _tool_output_content(out):
    """function_call_output.output -> (文本, 附件 parts)。

    只有整个 output 长得像内容块列表时才拆附件；其余维持旧的 json.dumps，
    避免把结构化结果里的 base64 字段当文本灌进网页。
    """
    if out is None:
        return "", []
    if isinstance(out, str):
        return out, []
    if isinstance(out, list) and out and all(_looks_like_part(x) for x in out):
        return split_content(out)
    return json.dumps(out, ensure_ascii=False), []


def _resp_item_id(prefix):
    return "%s_%s" % (prefix, uuid.uuid4().hex[:16])


def _chat_pick_output(j):
    """从 chat completions 非流式响应抽出纯文本与工具调用。"""
    content = ""
    fcalls = []
    for ch in (j.get("choices") or []):
        if not isinstance(ch, dict):
            continue
        msg = ch.get("message") or {}
        c = msg.get("content")
        if isinstance(c, str):
            content += c
        elif isinstance(c, list):
            content += flatten_content(c)
        for tc in (msg.get("tool_calls") or []):
            if not isinstance(tc, dict):
                continue
            fn = tc.get("function") or {}
            fcalls.append({"id": tc.get("id") or ("call_" + uuid.uuid4().hex[:12]),
                           "name": fn.get("name") or "",
                           "arguments": fn.get("arguments") or "{}"})
    return content, fcalls


def _build_response_obj(resp_id, model, content, fcalls, usage, prompt_chars,
                        status="completed", pt=None, ct=None):
    """组装 Responses 标准响应体；usage 优先用上游，没有则按正文估算。"""
    output = []
    text = content or ""
    if text:
        output.append({"id": _resp_item_id("msg"), "type": "message",
                       "status": "completed", "role": "assistant",
                       "content": [{"type": "output_text", "text": text}]})
    for fc in fcalls:
        output.append({"id": _resp_item_id("fc"), "type": "function_call",
                       "status": "completed",
                       "call_id": fc.get("id") or ("call_" + uuid.uuid4().hex[:12]),
                       "name": fc.get("name") or "",
                       "arguments": fc.get("arguments") or "{}"})
    if pt is None or ct is None:
        u = usage if isinstance(usage, dict) else {}
        pt = _num(u, "prompt_tokens")
        ct = _num(u, "completion_tokens")
    est = (pt <= 0 and ct <= 0)
    if est:
        pt = est_tokens("字" * prompt_chars)
        ct = est_tokens(text)
    usage_out = {"input_tokens": pt, "output_tokens": ct, "total_tokens": pt + ct,
                 "output_tokens_details": {"reasoning_tokens": 0}}
    if est:
        usage_out["estimated"] = True
    return {"id": resp_id, "object": "response", "created_at": int(time.time()),
            "status": status, "model": model, "output": output, "usage": usage_out}


def responses_to_chat(body):
    """把 /v1/responses 请求体转成 chat/completions 结构。
    返回 (chat_body, prompt_chars)；不支持的 input item 类型抛 ValueError。
    """
    msgs = []
    prompt_chars = 0
    instructions = body.get("instructions")
    if isinstance(instructions, str) and instructions.strip():
        msgs.append({"role": "system", "content": instructions.strip()})
        prompt_chars += len(instructions)
    inp = body.get("input")
    if isinstance(inp, str):
        if inp.strip():
            msgs.append({"role": "user", "content": inp})
            prompt_chars += len(inp)
    elif isinstance(inp, list):
        for it in inp:
            if not isinstance(it, dict):
                continue
            t = it.get("type")
            if isinstance(t, str):
                tt = t.strip().lower()
            else:
                tt = None
            if tt in ("reasoning", "computer_call", "computer_call_output",
                      "computer_call_preview", "web_search_call",
                      "web_search_tool_call", "custom_tool_call",
                      "custom_tool_call_output", "item_reference"):
                continue
            if tt == "function_call":
                msgs.append({"role": "assistant", "content": None, "tool_calls": [{
                    "id": it.get("call_id") or ("call_" + uuid.uuid4().hex[:12]),
                    "type": "function",
                    "function": {"name": it.get("name") or "",
                                 "arguments": it.get("arguments") or "{}"}}]})
                continue
            if tt == "function_call_output":
                t_txt, t_parts = _tool_output_content(it.get("output"))
                msgs.append({"role": "tool",
                             "content": content_to_wire(t_txt, t_parts),
                             "tool_call_id": it.get("call_id") or ""})
                continue
            # chat 风格兜底：message / 无 type / 未知 type / type 直接是角色名
            role = str(it.get("role") or "").strip().lower()
            if role == "tool" or (tt == "tool" and it.get("tool_call_id") is not None):
                t_txt, t_parts = _tool_output_content(it.get("content"))
                msgs.append({"role": "tool",
                             "content": content_to_wire(t_txt, t_parts),
                             "tool_call_id": it.get("tool_call_id") or it.get("call_id") or ""})
                continue
            tcalls = it.get("tool_calls")
            if isinstance(tcalls, list) and tcalls:
                fcs = []
                for tcx in tcalls:
                    if not isinstance(tcx, dict):
                        continue
                    fn = tcx.get("function") or {}
                    if not isinstance(fn, dict):
                        fn = {}
                    fcs.append({"id": tcx.get("id") or ("call_" + uuid.uuid4().hex[:12]),
                                "type": "function",
                                "function": {"name": fn.get("name") or "",
                                             "arguments": fn.get("arguments") or "{}"}})
                if fcs:
                    c_text, c_parts = split_content(it.get("content"))
                    content = content_to_wire(c_text, c_parts) or None
                    msgs.append({"role": "assistant", "content": content,
                                 "tool_calls": fcs})
                continue
            if role in ("system", "developer") or tt in ("system", "developer"):
                role = "system"
            elif role == "assistant" or tt == "assistant":
                role = "assistant"
            else:
                role = "user"
            c_text, c_parts = split_content(it.get("content"))
            wire = content_to_wire(c_text, c_parts)
            if wire:
                msgs.append({"role": role, "content": wire})
                prompt_chars += len(c_text)
    chat = {"model": body.get("model") or DEFAULT_MODEL,
            "messages": msgs, "stream": bool(body.get("stream"))}
    tools = []
    for tl in (body.get("tools") or []):
        if not isinstance(tl, dict) or tl.get("type") != "function":
            continue
        fn = {"name": tl.get("name") or "",
              "description": tl.get("description") or "",
              "parameters": tl.get("parameters")
              if isinstance(tl.get("parameters"), dict)
              else {"type": "object", "properties": {}}}
        if tl.get("strict") is not None:
            fn["strict"] = tl["strict"]
        tools.append({"type": "function", "function": fn})
    if tools:
        chat["tools"] = tools
    tc = body.get("tool_choice")
    if isinstance(tc, dict) and tc.get("type") == "function":
        chat["tool_choice"] = {"type": "function",
                               "function": {"name": tc.get("name") or ""}}
    elif tc in ("auto", "none", "required"):
        chat["tool_choice"] = tc
    if body.get("parallel_tool_calls") is not None:
        chat["parallel_tool_calls"] = bool(body["parallel_tool_calls"])
    if body.get("temperature") is not None:
        chat["temperature"] = body["temperature"]
    if body.get("top_p") is not None:
        chat["top_p"] = body["top_p"]
    if body.get("max_output_tokens") is not None:
        try:
            chat["max_tokens"] = int(body["max_output_tokens"])
        except (TypeError, ValueError):
            pass
    if body.get("stop") is not None:
        chat["stop"] = body["stop"]
    if body.get("stream_options") is not None:
        chat["stream_options"] = body["stream_options"]
    return chat, prompt_chars


def responses_payload(chat_body, requested_model):
    """同 normalize_payload，但保留 tool_calls / tool 消息（chat 路径用不到）。"""
    msgs = []
    prompt_chars = 0
    for m in (chat_body.get("messages") or []):
        if not isinstance(m, dict):
            continue
        role = (m.get("role") or "user").strip().lower()
        if role == "developer":
            role = "system"
        if role not in ("system", "user", "assistant", "tool"):
            role = "user"
        tcalls = m.get("tool_calls")
        text, parts = split_content(m.get("content"))
        wire = content_to_wire(text, parts)
        if not wire and not tcalls:
            continue
        prompt_chars += len(text)
        out = {"role": role}
        if tcalls:
            out["tool_calls"] = tcalls
        if wire:
            out["content"] = wire
        if role == "tool":
            out["tool_call_id"] = m.get("tool_call_id") or ""
        msgs.append(out)
    msgs, injected = maybe_inject_lang(msgs)
    if injected:
        prompt_chars += len(LANG_DIRECTIVE)
    stream = bool(chat_body.get("stream"))
    payload = {"model": map_model(requested_model), "messages": msgs, "stream": stream}
    for k, v in chat_body.items():
        if k in ("model", "messages", "stream") or k in STRIP_KEYS or v is None:
            continue
        payload[k] = v
    minimal = {"model": payload["model"], "messages": msgs, "stream": stream}
    return payload, minimal, prompt_chars


def normalize_payload(body, requested_model):
    msgs = []
    prompt_chars = 0
    for m in (body.get("messages") or []):
        if not isinstance(m, dict):
            continue
        role = (m.get("role") or "user").strip().lower()
        if role == "developer":
            role = "system"
        if role not in ("system", "user", "assistant"):
            role = "user"
        text, parts = split_content(m.get("content"))
        wire = content_to_wire(text, parts)
        if not wire:
            continue
        prompt_chars += len(text)
        msgs.append({"role": role, "content": wire})
    msgs, injected = maybe_inject_lang(msgs)
    if injected:
        prompt_chars += len(LANG_DIRECTIVE)
    stream = bool(body.get("stream"))
    payload = {"model": map_model(requested_model), "messages": msgs, "stream": stream}
    for k, v in body.items():
        if k in ("model", "messages", "stream") or k in STRIP_KEYS or v is None:
            continue
        payload[k] = v
    minimal = {"model": payload["model"], "messages": msgs, "stream": stream}
    return payload, minimal, prompt_chars


def maybe_inject_lang(msgs):
    """把 POOL_LANG_DIRECTIVE 前置为一条 system 消息；返回 (消息列表, 是否实际注入)。
    幂等：若客户端第一条本来就是同一条指令，则不再重复插入。
    注入发生在会话指纹之前（full_msgs = payload["messages"]），保证复用一致性；
    reuse 模式只发尾回合，首条指令已在首轮送过，不会重复。"""
    if not LANG_DIRECTIVE or not msgs:
        return msgs, False
    first = msgs[0]
    if isinstance(first, dict) and first.get("role") == "system" \
            and first.get("content") == LANG_DIRECTIVE:
        return msgs, False
    return [{"role": "system", "content": LANG_DIRECTIVE}] + list(msgs), True


try:
    import dsess
except Exception:                     # 会话模块出问题也不能拖垮号池
    dsess = None


def _sess_plan(full_msgs, payload):
    """为本次请求规划网页会话；拿不到计划就等于关闭增量。"""
    if dsess is None:
        return None
    try:
        return dsess.plan(full_msgs, payload.get("model"))
    except Exception as exc:
        log("dsess.plan 异常，本请求退回全量: %r" % (exc,))
        return None


def _sess_pick(h, tried, deadline):
    """有可复用会话就优先回那台实例；全新会话优先挑没人占的。"""
    unhealthy = {u.id for u in UPSTREAMS if _login_unhealthy(_login_state_cached(), u.id)}
    if h is None:
        return pick(tried, deadline, unhealthy=unhealthy)
    prefer = h.get("prefer") or None
    avoid = None
    if not h.get("k"):
        try:
            avoid = dsess.bound_ids()
        except Exception:
            avoid = None
    return pick(tried, deadline, prefer, avoid, unhealthy)


def _sess_frame(h, up, full_msgs, payload, minimal, prompt_chars):
    """返回 (请求体, 最小请求体, 实发字符数, 模式)。任何异常一律全量，宁可慢不能错。"""
    if h is None or up is None:
        return payload, minimal, prompt_chars, "off"
    try:
        msgs, preset, sent_chars, mode = dsess.frame(h, up.id, full_msgs)
    except Exception as exc:
        log("dsess.frame 异常，退回全量: %r" % (exc,))
        return payload, minimal, prompt_chars, "error"
    if mode != "reuse" or not preset:
        return payload, minimal, prompt_chars, mode
    body = dict(payload)
    body["messages"] = msgs
    body["preset_name"] = preset
    mini = {"model": body["model"], "messages": msgs, "stream": body.get("stream"),
            "preset_name": preset}
    return body, mini, sent_chars, mode


def _sess_commit(h, up, mode, full_msgs):
    if h is None or up is None:
        return
    try:
        dsess.commit(h, up.id, full_msgs)
        dsess.note_mode(mode)
    except Exception as exc:
        log("dsess.commit 异常: %r" % (exc,))


def _sess_fault(h, up):
    """这次没成，网页侧吃了多少已经说不准，作废指纹，下次全量重建。"""
    if h is None or up is None:
        return
    try:
        dsess.fault(h, up.id)
    except Exception as exc:
        log("dsess.fault 异常: %r" % (exc,))
    h["k"] = 0
    h["prefer"] = None


def pick(tried, deadline, prefer=None, avoid=None, unhealthy=None):
    """挑一台没试过、不在冷却、有空槽的实例；排队到 deadline 仍无则返回 None。

    prefer 是会话粘滞的目标实例：能立刻拿到就用它，拿不到不排队（改走全量重建）。
    avoid 是已被别的活会话占着的实例，全新会话尽量不往那儿投。
    unhealthy 是探针判定停机/封号的实例 id 集合：调度直接跳过，账号恢复后再投。
    """
    n = max(1, len(UPSTREAMS))
    cur = next(_RR)
    avoid = avoid or set()
    unhealthy = unhealthy or set()
    if prefer:
        pu = next((u for u in UPSTREAMS
                   if u.id == prefer and u.id not in tried and u.id not in unhealthy
                   and not u.cooling()), None)
        if pu is not None and pu.sem.acquire(blocking=False):
            with pu.lock:
                pu.inflight += 1
                pu.last_pick = time.time()
            return pu
    while True:
        now = time.time()
        cands = [u for u in UPSTREAMS
                 if u.id not in tried and u.id not in unhealthy and not u.cooling(now)]
        cands.sort(key=lambda u: (0 if u.id == prefer else 1,
                                  0 if u.id not in avoid else 1,
                                  u.inflight, (u.idx - cur) % n))
        for u in cands:
            if u.sem.acquire(blocking=False):
                with u.lock:
                    u.inflight += 1
                    u.last_pick = now
                return u
        # 健康池里已没有候选、又没有在途请求能释放槽位时，别干等到 deadline
        pool_alive = [u for u in UPSTREAMS if u.id not in unhealthy and not u.cooling(now)]
        if not any(u.inflight for u in pool_alive):
            return None
        if time.time() >= deadline:
            return None
        time.sleep(0.4)


def release(u):
    with u.lock:
        u.inflight = max(0, u.inflight - 1)
    try:
        u.sem.release()
    except ValueError:
        pass


def upstream_post(u, payload, stream):
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    hdrs = {"Content-Type": "application/json", "Content-Length": str(len(data)),
            "Accept": "text/event-stream" if stream else "application/json"}
    if u.token:
        hdrs["Authorization"] = "Bearer " + u.token
    conn = http.client.HTTPConnection(u.host, u.port, timeout=FIRST_BYTE_TIMEOUT)
    conn.request("POST", "/v1/chat/completions", body=data, headers=hdrs)
    return conn, conn.getresponse()


RETRYABLE_STATUS = {400, 401, 403, 408, 409, 425, 429, 500, 502, 503, 504}


class CtlUnavailable(Exception):
    """root 侧 ds-pool-ctl 连不上（socket 不存在 / 连接失败）。"""


_CTL_LOCK = threading.Lock()
_CTL_UNIX_OK = None  # None=未探明; True=unix 通道可用; False=已确认不可用（切 TCP）


def _ctl_ping_unix():
    """快速探活 unix 通道（本机 AliYunDun 等安全 agent 会拖慢 AF_UNIX 写回）。

    只读不回 / 回写被拖都会导致 health 拿不到响应，用 3 秒短超时判定，
    之后整条 unix 通道在本进程生命周期内直接弃用。
    """
    s = None
    try:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.settimeout(2)
        s.connect(CTL_SOCKET)
        s.sendall(b"GET /health HTTP/1.1\r\nHost: ds-pool\r\n"
                  b"Content-Length: 0\r\nConnection: close\r\n\r\n")
        s.settimeout(3)
        data = b""
        while True:
            chunk = s.recv(4096)
            if not chunk:
                break
            data += chunk
        status_line = data.split(b"\r\n", 1)[0]
        return b"200" in status_line and b"ds-pool-ctl" in data
    except OSError:
        return False
    finally:
        if s is not None:
            try:
                s.close()
            except Exception:
                pass


def _hdr_lines(extra):
    if not extra:
        return ""
    return "".join("%s: %s\r\n" % (k, v) for k, v in (extra or {}).items())


def _ctl_exchange(connector, extra_headers, name, action, timeout=CTL_READ_TIMEOUT):
    """通过已连上的 socket 发一条控制请求并解析响应。失败抛 CtlUnavailable。"""
    token = next(iter(TOKENS), "")
    head = ("POST /service/%s/%s HTTP/1.1\r\nHost: ds-pool\r\n"
            "Authorization: Bearer %s\r\n%s"
            "Content-Length: 0\r\nConnection: close\r\n\r\n" %
            (name, action, token, _hdr_lines(extra_headers))).encode("utf-8")
    raw = b""
    s = None
    try:
        s = connector()
        s.settimeout(timeout)
        s.sendall(head)
        while True:
            chunk = s.recv(65536)
            if not chunk:
                break
            raw += chunk
    except OSError as exc:
        raise CtlUnavailable("ctl 不可用: %s" % exc)
    finally:
        if s is not None:
            try:
                s.close()
            except Exception:
                pass
    if b"\r\n\r\n" not in raw:
        raise CtlUnavailable("ctl 响应异常（无响应头）")
    hdr, body = raw.split(b"\r\n\r\n", 1)
    try:
        status = int(hdr.split(b" ", 2)[1])
    except (IndexError, ValueError):
        status = 503
    try:
        obj = json.loads(body.decode("utf-8", "replace"))
    except Exception:
        obj = {"ok": False, "result": status, "stdout": "",
               "stderr": body.decode("utf-8", "replace")[:400]}
    return status, obj


def ctl_request(name, action, extra_headers=None, timeout=CTL_READ_TIMEOUT):
    """把服务控制请求转发给 root 侧的 ds-pool-ctl。返回 (http_status, obj)。

    unix socket 优先；本机 AF_UNIX 写回被安全 agent 拖慢时自动切 TCP 127.0.0.1 兜底。
    任一通道不可用 -> 抛 CtlUnavailable（上层回 503，绝不挂死请求）。
    """
    global _CTL_UNIX_OK
    last_err = None
    with _CTL_LOCK:
        if _CTL_UNIX_OK is None:
            _CTL_UNIX_OK = hasattr(socket, "AF_UNIX") and _ctl_ping_unix()
        if _CTL_UNIX_OK:
            try:
                return _ctl_exchange(
                    lambda: _connect_unix(), extra_headers, name, action, timeout=timeout)
            except CtlUnavailable as exc:
                last_err = exc
                _CTL_UNIX_OK = False

    def _connect_tcp():
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(CTL_CONNECT_TIMEOUT)
        try:
            s.connect(("127.0.0.1", CTL_TCP_PORT))
        except Exception:
            s.close()
            raise
        return s

    secret = ("X-Ctl-Secret: %s\r\n" % CTL_SECRET) if CTL_SECRET else ""
    try:
        merged = dict(extra_headers or {})
        if CTL_SECRET:
            merged["X-Ctl-Secret"] = CTL_SECRET
        return _ctl_exchange(_connect_tcp, merged, name, action, timeout=timeout)
    except CtlUnavailable as exc:
        msg = str(exc)
        if last_err is not None:
            msg = "%s; %s" % (last_err, msg)
        raise CtlUnavailable(msg)


def _connect_unix():
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(CTL_CONNECT_TIMEOUT)
    try:
        s.connect(CTL_SOCKET)
    except Exception:
        s.close()
        raise
    return s


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "ds-pool/1.0"

    # ---------------------------------------------------------------- helpers
    def _send(self, code, obj=None, raw=None, extra=None):
        if raw is None:
            raw = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        try:
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.send_header("Access-Control-Allow-Origin", "*")
            for k, v in (extra or {}).items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(raw)
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True

    def _err(self, code, msg, etype="ds_pool_error", ecode=None, extra=None):
        self._send(code, {"error": {"message": msg, "type": etype, "code": ecode or etype}}, extra=extra)

    def _send_html(self, raw):
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(raw)
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True

    def _api_key(self):
        h = self.headers.get("Authorization") or ""
        if h.lower().startswith("bearer "):
            return h[7:].strip()
        for name in ("x-api-key", "X-Api-Key", "X-API-Key", "api-key"):
            v = self.headers.get(name)
            if v:
                return v.strip()
        q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        for name in ("api_key", "token"):
            if q.get(name):
                return q[name][0].strip()
        return ""

    def _auth_ok(self):
        if not TOKENS:
            return True
        key = self._api_key()
        ok = any(hmac_equal(key, t) for t in TOKENS)
        if not ok:
            with STATS_LOCK:
                STATS["auth_fail"] += 1
        return ok

    def _path(self):
        return urllib.parse.urlparse(self.path).path.rstrip("/") or "/"

    # ---------------------------------------------------------------- routes
    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Api-Key")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Max-Age", "600")
        self.end_headers()

    def do_GET(self):
        p = self._path()
        if p in ("/health", "/healthz"):
            _ls = _login_state_cached()
            ready = [u.id for u in UPSTREAMS if not u.cooling()]
            healthy = [u.id for u in UPSTREAMS
                       if not u.cooling() and not _login_unhealthy(_ls, u.id)]
            ups = []
            for u in UPSTREAMS:
                e = _login_entry(_ls, u.id)
                ups.append({
                    "id": u.id,
                    "ready": not u.cooling(),
                    "healthy": not u.cooling() and not _login_unhealthy(_ls, u.id),
                    "state": u.snapshot()["state"],
                    "login_state": (e or {}).get("state"),
                    "login_unhealthy": bool(e and e.get("unhealthy")),
                    "wake_at": (e or {}).get("wake_at"),
                })
            return self._send(200, {"service": "ds-pool", "ok": bool(healthy),
                                    "upstreams_ready": ready,
                                    "upstreams_healthy": healthy,
                                    "upstreams": ups,
                                    "login_state_updated_at": (_ls or {}).get("updated_at"),
                                    "uptime": int(time.time() - START_TS)})
        if not self._auth_ok():
            return self._err(401, "invalid api key", "authentication_error", "invalid_api_key")
        if p == "/pool/info":
            return self._send(200, {"service": "ds-pool",
                                    "endpoints": ["/v1/chat/completions", "/v1/models",
                                                  "/pool/status", "/pool/usage",
                                                  "/pool/upstream/<id>/reset",
                                                  "/pool/sessions",
                                                  "/pool/switch/<dsN>/account/<account>",
                                                  "/pool/service/<svc>/<restart|start|stop|is-active|status>",
                                                  "/ui.html?token=<pool_token>", "/health"],
                                    "default_model": DEFAULT_MODEL,
                                    "upstreams": [u.id for u in UPSTREAMS]})
        if p in ("/", "/ui", "/ui.html"):
            if UI_LOADED:
                return self._send_html(UI_HTML)
            return self._err(503, "ui.html 未部署或未加载（%s）" % UI_FILE,
                             "internal_error", "ui_missing")
        if p == "/v1/models":
            names = []
            for m in UPSTREAM_MODELS + [u.model for u in UPSTREAMS] + list(MODEL_ALIAS):
                if m and m not in names:
                    names.append(m)
            names = names or [DEFAULT_MODEL]
            return self._send(200, {"object": "list",
                                    "data": [{"id": n, "object": "model", "owned_by": "ds-pool"}
                                             for n in names]})
        if p == "/pool/status":
            with STATS_LOCK:
                st = dict(STATS)
            login_state = _load_login_state()
            login_instances = (login_state or {}).get("instances") or {}
            upstream_snap = []
            for u in UPSTREAMS:
                s = u.snapshot()
                li = login_instances.get(u.id)
                if isinstance(li, dict):
                    s["login"] = li.get("state")
                    s["login_checked_at"] = li.get("checked_at")
                    s["login_error"] = li.get("error")
                    s["login_profile"] = li.get("profile")
                else:
                    s["login"] = None
                upstream_snap.append(s)
            return self._send(200, {
                "service": "ds-pool", "time": time.strftime("%Y-%m-%d %H:%M:%S"),
                "uptime": int(time.time() - START_TS), "default_model": DEFAULT_MODEL,
                "per_upstream_concurrency": PER_CONC, "cooldown": COOLDOWN,
                "queue_timeout": QUEUE_TIMEOUT, "stats": st,
                "inflight": sum(u.inflight for u in UPSTREAMS),
                "accounts_available": [x["account"] for x in _load_profiles()],
                "login_state_updated_at": (login_state or {}).get("updated_at"),
                "upstreams": upstream_snap,
                "usage": USAGE.snapshot()})
        if p == "/pool/usage":
            return self._send(200, {"service": "ds-pool", "stats_this_boot": dict(STATS),
                                    "usage": USAGE.snapshot()})
        if p == "/pool/sessions":
            try:
                snap = dsess.snapshot() if dsess else {
                    "enabled": False, "reason": "dsess 模块未加载"}
            except Exception as exc:
                snap = {"enabled": False, "reason": repr(exc)}
            return self._send(200, {"service": "ds-pool", "time": time.strftime(
                "%Y-%m-%d %H:%M:%S"), "sessions": snap})
        if p == "/pool/sessions/clear":
            try:
                if dsess:
                    with dsess._LOCK:
                        dsess._SESS.clear()
                        dsess._BOUND.clear()
                return self._send(200, {"ok": True, "cleared": True})
            except Exception as exc:
                return self._err(500, "清空失败: %r" % (exc,), "internal_error")
        return self._err(404, "not found: %s" % p, "invalid_request_error")

    def do_POST(self):
        p = self._path()
        try:
            if not self._auth_ok():
                return self._err(401, "invalid api key", "authentication_error", "invalid_api_key")
            m = re.match(r"^/pool/upstream/([\w.\-]+)/(reset|enable)$", p)
            if m:
                return self._reset(m.group(1))
            m = re.match(r"^/pool/switch/(ds[1-9])/account/([\w.\-]+)$", p)
            if m:
                return self._switch(m.group(1), m.group(2))
            m = re.match(r"^/pool/service/([\w.\-]+)/(restart|start|stop|is-active|status)$", p)
            if m:
                return self._ctl_call(m.group(1), m.group(2))
            if p in ("/v1/chat/completions", "/api/chat/completions"):
                return self._chat()
            if p == "/v1/responses":
                return self._responses()
            if p in ("/v1/completions", "/v1/messages", "/v1/embeddings"):
                return self._err(501, "%s 暂不支持，请用 /v1/chat/completions" % p,
                                 "invalid_request_error", "unsupported_endpoint")
            return self._err(404, "not found: %s" % p, "invalid_request_error")
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True
        except Exception as exc:
            log("!! handler crash %s: %r" % (p, exc))
            try:
                self._err(500, "pool internal error: %r" % (exc,), "internal_error")
            except Exception:
                self.close_connection = True

    def _switch(self, uid, acct):
        """切号：把实例 uid 的 Chrome 切到 acct 的 profile（经 ctl 转发给 root 执行）。"""
        known = [x["account"] for x in _load_profiles()]
        if acct not in known:
            return self._err(400, "未知账号: %s（可选: %s）" % (acct, ",".join(sorted(known))),
                             "invalid_request_error", "unknown_account")
        svc = None
        for u in UPSTREAMS:
            if u.id == uid:
                for s in (u.services or ()):
                    if s.startswith("chrome"):
                        svc = s
                        break
        if not svc:
            return self._err(404, "no such upstream: %s" % uid, "invalid_request_error")
        log("admin switch %s -> %s (via %s)" % (uid, acct, svc))
        try:
            status, obj = ctl_request(svc, "switch",
                                      extra_headers={"X-Switch-Account": acct},
                                      timeout=240)
        except CtlUnavailable as exc:
            return self._err(503, str(exc), "service_unavailable", "ctl_unavailable")
        return self._send(status, obj)

    def _reset(self, uid):
        for u in UPSTREAMS:
            if u.id == uid:
                with u.lock:
                    u.cooldown_until = 0.0
                    u.fail = 0
                    u.last_error = None
                log("admin reset upstream %s" % uid)
                return self._send(200, {"ok": True, "upstream": u.snapshot()})
        return self._err(404, "no such upstream: %s" % uid, "invalid_request_error")

    def _ctl_call(self, name, action):
        if name not in CTL_ALLOWED:
            return self._err(403, "forbidden service: %s" % name, "invalid_request_error")
        log("admin ctl %s %s" % (action, name))
        try:
            status, obj = ctl_request(name, action)
        except CtlUnavailable as exc:
            return self._err(503, str(exc), "service_unavailable", "ctl_unavailable")
        return self._send(status, obj)

    def log_message(self, fmt, *args):
        return  # 关掉默认 access log，只用自己的单行日志

    # ---------------------------------------------------------------- 核心转发
    def _read_body(self):
        try:
            n = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            n = 0
        return self.rfile.read(n) if n > 0 else b"{}"

    def _attempt(self, up, payload, minimal, stream, rid, requested_model, prompt_chars,
                 responses_mode=False):
        """对单台实例试一次。返回 (kind, info, cool)：
        kind = ok / retry（换一台） / status（原样把错误回给客户端）"""
        for variant in (0, 1):
            body = payload if variant == 0 else minimal
            t0 = time.time()
            try:
                conn, resp = upstream_post(up, body, stream)
            except (urllib.error.URLError, OSError, http.client.HTTPException) as exc:
                return ("retry", "%s -> %s: %s" % (up.id, type(exc).__name__, str(exc)[:150]), True)
            status = resp.status
            if status >= 400:
                try:
                    detail = (resp.read() or b"")[:400].decode("utf-8", "replace")
                except Exception:
                    detail = ""
                try:
                    conn.close()
                except Exception:
                    pass
                low = detail.lower()
                bad = any((k in detail) or (k.lower() in low) for k in BAD_MARKERS)
                if variant == 0 and (status == 422 or
                                     (status == 400 and ("field" in low or "extra" in low))):
                    continue  # 上游嫌弃多余字段，退到最小请求体再试
                if status in (408, 409, 425, 429) or status >= 500 or bad:
                    return ("retry", "%s -> HTTP %s %s" % (up.id, status, detail[:180]),
                            (status in (429, 500, 502, 503, 504)) or bad)
                return ("status", (status, detail[:300]), False)
            try:
                if responses_mode:
                    if stream:
                        res = self._deliver_responses_stream(up, conn, resp, rid,
                                                             requested_model, prompt_chars)
                    else:
                        res = self._deliver_responses_json(up, conn, resp, rid,
                                                           requested_model, prompt_chars)
                elif stream:
                    res = self._deliver_stream(up, conn, resp, rid, requested_model, prompt_chars)
                else:
                    res = self._deliver_json(up, conn, resp, rid, requested_model, prompt_chars)
            except (BrokenPipeError, ConnectionResetError):
                try:
                    conn.close()
                except Exception:
                    pass
                return ("ok", (time.time() - t0, _nores()), False)
            secs = time.time() - t0
            up.mark_ok(secs)
            return ("ok", (secs, res), False)
        return ("retry", "%s -> 请求体被上游拒绝" % up.id, False)

    def _chat(self):
        rid = uuid.uuid4().hex[:8]
        raw = self._read_body()
        try:
            body = json.loads(raw.decode("utf-8"))
            if not isinstance(body, dict):
                raise ValueError("body 不是 JSON 对象")
        except Exception as exc:
            return self._err(400, "请求体不是合法 JSON: %r" % (exc,), "invalid_request_error")

        requested_model = body.get("model") or DEFAULT_MODEL
        payload, minimal, prompt_chars = normalize_payload(body, requested_model)
        if not payload["messages"]:
            return self._err(400, "messages 为空", "invalid_request_error")
        stream = payload["stream"]

        with STATS_LOCK:
            STATS["req"] += 1
        _ls = _login_state_cached()
        _unhealthy_ids = {u.id for u in UPSTREAMS if _login_unhealthy(_ls, u.id)}
        if len(_unhealthy_ids) >= len(UPSTREAMS) and _unhealthy_ids:
            wake = _login_wake(_ls)
            ra = max(15, min(3600, int(wake - time.time()) if wake else 15))
            with STATS_LOCK:
                STATS["fail"] += 1
            USAGE.note(None, False)
            msg = ("所有实例均已停机（封号休眠/探活失败），暂时无法提供服务"
                   + ("，最近计划 %s 自动开机复查" % _fmt_ts(wake) if wake else "，等待自动恢复"))
            return self._err(503, msg, "service_unavailable", "all_upstreams_unhealthy",
                             extra={"Retry-After": str(ra)})
        if not any(not u.cooling() for u in UPSTREAMS):
            left = min(u.cooldown_left() for u in UPSTREAMS) or COOLDOWN
            with STATS_LOCK:
                STATS["fail"] += 1
            USAGE.note(None, False)
            return self._err(503, "所有上游实例都在冷却中，%ds 后自动探活恢复" % left,
                             "service_unavailable", "all_upstreams_cooling",
                             extra={"Retry-After": str(max(5, left))})

        deadline = time.time() + QUEUE_TIMEOUT
        tried = set()
        last_err = "unknown"
        last_uid = None
        attempt = 0
        full_msgs = payload["messages"]
        sess = _sess_plan(full_msgs, payload)
        while True:
            up = _sess_pick(sess, tried, deadline)
            if up is None:
                with STATS_LOCK:
                    STATS["fail"] += 1
                USAGE.note(last_uid, False)
                if tried:
                    return self._err(502, "全部实例尝试失败，最后错误: %s" % last_err,
                                     "api_error", "all_upstreams_failed")
                return self._err(429, "号池繁忙（%d 台实例 x 并发 %d），排队 %ds 无空闲" %
                                 (len(UPSTREAMS), PER_CONC, QUEUE_TIMEOUT),
                                 "rate_limit_error", "pool_busy", extra={"Retry-After": "5"})
            attempt += 1
            spay, smin, spc, mode = _sess_frame(sess, up, full_msgs, payload,
                                                minimal, prompt_chars)
            kind, info, cool = self._attempt(up, spay, smin, stream, rid,
                                             requested_model, spc)
            release(up)
            if kind == "ok":
                secs, res = info
                _sess_commit(sess, up, mode, full_msgs)
                with STATS_LOCK:
                    STATS["ok"] += 1
                USAGE.note(up.id, True, res.get("prompt_tokens", 0),
                           res.get("completion_tokens", 0))
                est = " est" if res.get("estimated") else ""
                log("[%s] up=%s acct=%s attempts=%d %.1fs prompt~%d compl~%d%s%s"
                    " mode=%s sent~%d" %
                    (rid, up.id, up.acct, attempt, secs,
                     res.get("prompt_tokens", 0), res.get("completion_tokens", 0),
                     est, " stream" if stream else "", mode, spc))
                return
            if kind == "status":
                status, detail = info
                _sess_fault(sess, up)
                up.mark_fail("%s -> HTTP %s %s" % (up.id, status, detail[:180]), False)
                with STATS_LOCK:
                    STATS["fail"] += 1
                USAGE.note(up.id, False)
                return self._send(status, {"error": {
                    "message": "upstream %s: %s" % (up.id, detail),
                    "type": "upstream_error", "code": "upstream_error"},
                    "pool_upstream": up.id})
            last_err = info
            last_uid = up.id
            _sess_fault(sess, up)
            up.mark_fail(last_err, cool)
            tried.add(up.id)
            with STATS_LOCK:
                STATS["retry"] += 1
            USAGE.note_retry()
            log("[%s] %s" % (rid, last_err))
            if len(tried) >= len(UPSTREAMS):
                with STATS_LOCK:
                    STATS["fail"] += 1
                USAGE.note(last_uid, False)
                return self._err(502, "上游全部失败，最后错误: %s" % last_err,
                                 "api_error", "all_upstreams_failed")

    # ------------------------------------------------------------ /v1/responses
    def _responses(self):
        """Responses API -> chat/completions 协议桥接，复用号池调度。
        客户端(Codex Desktop等)只认 /v1/responses，这里做转换：
        请求：responses -> chat；响应：上游 chat SSE -> responses 事件流。
        """
        rid = uuid.uuid4().hex[:8]
        raw = self._read_body()
        try:
            body = json.loads(raw.decode("utf-8"))
            if not isinstance(body, dict):
                raise ValueError("body 不是 JSON 对象")
        except Exception as exc:
            return self._err(400, "请求体不是合法 JSON: %r" % (exc,), "invalid_request_error")

        requested_model = body.get("model") or DEFAULT_MODEL
        try:
            chat_body, _pc = responses_to_chat(body)
        except ValueError as exc:
            dbg = raw[:4096].decode("utf-8", "replace").replace("\n", " ")[:800]
            log("[%s] responses 输入转换失败: %s | %s" % (rid, exc, dbg))
            return self._err(400, str(exc), "invalid_request_error")
        payload, minimal, prompt_chars = responses_payload(chat_body, requested_model)
        if not payload["messages"]:
            dbg = raw[:4096].decode("utf-8", "replace").replace("\n", " ")[:800]
            log("[%s] responses input 转换后为空 | %s" % (rid, dbg))
            return self._err(400, "input 为空", "invalid_request_error")
        stream = payload["stream"]

        with STATS_LOCK:
            STATS["req"] += 1
        _ls = _login_state_cached()
        _unhealthy_ids = {u.id for u in UPSTREAMS if _login_unhealthy(_ls, u.id)}
        if len(_unhealthy_ids) >= len(UPSTREAMS) and _unhealthy_ids:
            wake = _login_wake(_ls)
            ra = max(15, min(3600, int(wake - time.time()) if wake else 15))
            with STATS_LOCK:
                STATS["fail"] += 1
            USAGE.note(None, False)
            msg = ("所有实例均已停机（封号休眠/探活失败），暂时无法提供服务"
                   + ("，最近计划 %s 自动开机复查" % _fmt_ts(wake) if wake else "，等待自动恢复"))
            return self._err(503, msg, "service_unavailable", "all_upstreams_unhealthy",
                             extra={"Retry-After": str(ra)})
        if not any(not u.cooling() for u in UPSTREAMS):
            left = min(u.cooldown_left() for u in UPSTREAMS) or COOLDOWN
            with STATS_LOCK:
                STATS["fail"] += 1
            USAGE.note(None, False)
            return self._err(503, "所有上游实例都在冷却中，%ds 后自动探活恢复" % left,
                             "service_unavailable", "all_upstreams_cooling",
                             extra={"Retry-After": str(max(5, left))})

        deadline = time.time() + QUEUE_TIMEOUT
        tried = set()
        last_err = "unknown"
        last_uid = None
        attempt = 0
        full_msgs = payload["messages"]
        sess = _sess_plan(full_msgs, payload)
        while True:
            up = _sess_pick(sess, tried, deadline)
            if up is None:
                with STATS_LOCK:
                    STATS["fail"] += 1
                USAGE.note(last_uid, False)
                if tried:
                    return self._err(502, "全部实例尝试失败，最后错误: %s" % last_err,
                                     "api_error", "all_upstreams_failed")
                return self._err(429, "号池繁忙（%d 台实例 x 并发 %d），排队 %ds 无空闲" %
                                 (len(UPSTREAMS), PER_CONC, QUEUE_TIMEOUT),
                                 "rate_limit_error", "pool_busy", extra={"Retry-After": "5"})
            attempt += 1
            spay, smin, spc, mode = _sess_frame(sess, up, full_msgs, payload,
                                                minimal, prompt_chars)
            kind, info, cool = self._attempt(up, spay, smin, stream, rid,
                                             requested_model, spc,
                                             responses_mode=True)
            release(up)
            if kind == "ok":
                secs, res = info
                _sess_commit(sess, up, mode, full_msgs)
                with STATS_LOCK:
                    STATS["ok"] += 1
                USAGE.note(up.id, True, res.get("prompt_tokens", 0),
                           res.get("completion_tokens", 0))
                est = " est" if res.get("estimated") else ""
                log("[%s] responses up=%s acct=%s attempts=%d %.1fs prompt~%d compl~%d%s%s"
                    " mode=%s sent~%d" %
                    (rid, up.id, up.acct, attempt, secs,
                     res.get("prompt_tokens", 0), res.get("completion_tokens", 0),
                     est, " stream" if stream else "", mode, spc))
                return
            if kind == "status":
                status, detail = info
                _sess_fault(sess, up)
                up.mark_fail("%s -> HTTP %s %s" % (up.id, status, detail[:180]), False)
                with STATS_LOCK:
                    STATS["fail"] += 1
                USAGE.note(up.id, False)
                return self._send(status, {"error": {
                    "message": "upstream %s: %s" % (up.id, detail),
                    "type": "upstream_error", "code": "upstream_error"},
                    "pool_upstream": up.id})
            last_err = info
            last_uid = up.id
            _sess_fault(sess, up)
            up.mark_fail(last_err, cool)
            tried.add(up.id)
            with STATS_LOCK:
                STATS["retry"] += 1
            USAGE.note_retry()
            log("[%s] %s" % (rid, last_err))
            if len(tried) >= len(UPSTREAMS):
                with STATS_LOCK:
                    STATS["fail"] += 1
                USAGE.note(last_uid, False)
                return self._err(502, "上游全部失败，最后错误: %s" % last_err,
                                 "api_error", "all_upstreams_failed")

    def _deliver_responses_json(self, up, conn, resp, rid, requested_model, prompt_chars):
        """非流式 responses：读上游 chat JSON，转成 Responses 结构一次性回包。"""
        try:
            blob = resp.read() or b""
        except (OSError, http.client.HTTPException):
            blob = b""
        try:
            j = json.loads(blob.decode("utf-8"))
            if not isinstance(j, dict):
                raise ValueError("not an object")
        except Exception:
            log("[%s] upstream %s responses 回了非 JSON，原样透传 HTTP %s" %
                (rid, up.id, resp.status))
            self._send(resp.status, raw=blob, extra={"X-Pool-Upstream": up.id})
            _close(conn)
            return _nores()
        content, fcalls = _chat_pick_output(j)
        resp_id = "resp_pool_%s" % uuid.uuid4().hex[:16]
        out = _build_response_obj(resp_id, requested_model, content, fcalls,
                                  j.get("usage"), prompt_chars, "completed")
        self._send(200, raw=json.dumps(out, ensure_ascii=False).encode("utf-8"),
                   extra={"X-Pool-Upstream": up.id})
        _close(conn)
        u = out["usage"]
        return {"text": content, "prompt_tokens": u.get("input_tokens", 0),
                "completion_tokens": u.get("output_tokens", 0),
                "estimated": bool(u.get("estimated"))}

    def _deliver_responses_stream(self, up, conn, resp, rid, requested_model, prompt_chars):
        """流式：把上游 chat SSE 逐条转成 Responses 事件流（Codex Desktop 需要的协议）。"""
        self.close_connection = True
        resp_id = "resp_pool_%s" % uuid.uuid4().hex[:16]
        created = int(time.time())
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "close")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("X-Pool-Upstream", up.id)
            self.end_headers()
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            _close(conn)
            return ""
        wlock = threading.Lock()
        stop = threading.Event()

        def keepalive():
            if KEEPALIVE <= 0:
                return
            while not stop.wait(KEEPALIVE):
                try:
                    with wlock:
                        self.wfile.write(b": pool-keepalive\n\n")
                        self.wfile.flush()
                except Exception:
                    return

        t = threading.Thread(target=keepalive, name="pool-keepalive-resp")
        t.daemon = True
        t.start()

        def send_event(obj):
            with wlock:
                self.wfile.write(("data: %s\n\n" %
                                  json.dumps(obj, ensure_ascii=False)).encode("utf-8"))
                self.wfile.flush()

        try:
            send_event({"type": "response.created", "response": {
                "id": resp_id, "object": "response", "created_at": created,
                "status": "in_progress", "model": requested_model,
                "output": [], "usage": None}})
            send_event({"type": "response.in_progress", "response": {
                "id": resp_id, "object": "response", "created_at": created,
                "status": "in_progress", "model": requested_model,
                "output": [], "usage": None}})
        except (BrokenPipeError, ConnectionResetError, OSError):
            stop.set()
            _close(conn)
            return ""

        items = []        # 已打开的 output item（按出现顺序）
        msg = None        # 当前 message item
        fcs = {}          # chat tool_call index -> fc item
        text = []
        real_pt = real_ct = 0
        usage_seen = [False]
        try:
            while True:
                try:
                    line = resp.readline()
                except (OSError, http.client.HTTPException):
                    break
                if not line:
                    break
                tline = line.decode("utf-8", "replace").strip()
                if not tline.startswith("data:"):
                    continue
                data = tline[5:].strip()
                if data == "[DONE]":
                    break
                try:
                    obj = json.loads(data)
                except Exception:
                    continue
                if not isinstance(obj, dict):
                    continue
                u = obj.get("usage")
                if isinstance(u, dict):
                    real_pt = max(real_pt, _num(u, "prompt_tokens"))
                    real_ct = max(real_ct, _num(u, "completion_tokens"))
                    if not usage_seen[0]:
                        usage_seen[0] = True
                for ch in (obj.get("choices") or []):
                    if not isinstance(ch, dict):
                        continue
                    delta = ch.get("delta") or {}
                    if not isinstance(delta, dict):
                        delta = {}
                    c = delta.get("content")
                    if isinstance(c, str) and c:
                        if msg is None:
                            msg = {"id": _resp_item_id("msg"), "oidx": len(items),
                                   "type": "message"}
                            items.append(msg)
                            send_event({"type": "response.output_item.added",
                                        "output_index": msg["oidx"],
                                        "item": {"id": msg["id"], "type": "message",
                                                 "status": "in_progress",
                                                 "role": "assistant", "content": []}})
                            send_event({"type": "response.content_part.added",
                                        "item_id": msg["id"],
                                        "output_index": msg["oidx"],
                                        "content_index": 0,
                                        "part": {"type": "output_text", "text": ""}})
                        text.append(c)
                        send_event({"type": "response.output_text.delta",
                                    "item_id": msg["id"],
                                    "output_index": msg["oidx"],
                                    "content_index": 0, "delta": c})
                    for tc in (delta.get("tool_calls") or []):
                        if not isinstance(tc, dict):
                            continue
                        fn = tc.get("function") or {}
                        idx = tc.get("index")
                        if idx not in fcs:
                            fc = {"id": _resp_item_id("fc"), "oidx": len(items),
                                  "type": "function_call",
                                  "call_id": tc.get("id") or ("call_" + uuid.uuid4().hex[:12]),
                                  "name": str(fn.get("name") or ""), "arguments": ""}
                            fcs[idx] = fc
                            items.append(fc)
                            send_event({"type": "response.output_item.added",
                                        "output_index": fc["oidx"],
                                        "item": {"id": fc["id"], "type": "function_call",
                                                 "status": "in_progress",
                                                 "call_id": fc["call_id"],
                                                 "name": fc["name"], "arguments": ""}})
                        else:
                            fc = fcs[idx]
                        a = fn.get("arguments")
                        if isinstance(a, str) and a:
                            fc["arguments"] += a
                            send_event({"type": "response.function_call_arguments.delta",
                                        "item_id": fc["id"], "output_index": fc["oidx"],
                                        "delta": a})
        except (BrokenPipeError, ConnectionResetError, OSError):
            log("[%s] client gone up=%s (responses)" % (rid, up.id))
        finally:
            stop.set()
            _close(conn)

        full_text = "".join(text)
        resp_obj = _build_response_obj(
            resp_id, requested_model, full_text,
            [{"id": it["call_id"], "name": it["name"], "arguments": it["arguments"]}
             for it in items if it["type"] == "function_call"],
            None, prompt_chars, "completed", pt=real_pt, ct=real_ct)
        try:
            for it in items:
                if it["type"] == "message":
                    send_event({"type": "response.output_text.done",
                                "item_id": it["id"], "output_index": it["oidx"],
                                "content_index": 0, "text": full_text})
                    send_event({"type": "response.content_part.done",
                                "item_id": it["id"], "output_index": it["oidx"],
                                "content_index": 0,
                                "part": {"type": "output_text", "text": full_text}})
                    send_event({"type": "response.output_item.done",
                                "output_index": it["oidx"],
                                "item": {"id": it["id"], "type": "message",
                                         "status": "completed", "role": "assistant",
                                         "content": [{"type": "output_text",
                                                      "text": full_text}]}})
                else:
                    send_event({"type": "response.function_call_arguments.done",
                                "item_id": it["id"], "output_index": it["oidx"],
                                "arguments": it["arguments"]})
                    send_event({"type": "response.output_item.done",
                                "output_index": it["oidx"],
                                "item": {"id": it["id"], "type": "function_call",
                                         "status": "completed",
                                         "call_id": it["call_id"],
                                         "name": it["name"],
                                         "arguments": it["arguments"]}})
            send_event({"type": "response.completed", "response": resp_obj})
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        est = real_pt <= 0 and real_ct <= 0
        return {"text": full_text,
                "prompt_tokens": resp_obj["usage"]["input_tokens"],
                "completion_tokens": resp_obj["usage"]["output_tokens"],
                "estimated": est}

    def _deliver_json(self, up, conn, resp, rid, requested_model, prompt_chars):
        """非流式：读完上游 JSON，规范化 model / usage 后回给客户端。
        返回 {text, prompt_tokens, completion_tokens, estimated}，
        token 数优先用上游给的，没有才按正文估（给号池统计用）。"""
        try:
            blob = resp.read() or b""
        except (OSError, http.client.HTTPException):
            blob = b""
        try:
            j = json.loads(blob.decode("utf-8"))
            if not isinstance(j, dict):
                raise ValueError("not an object")
        except Exception:
            log("[%s] upstream %s 回了非 JSON，原样透传 HTTP %s" % (rid, up.id, resp.status))
            self._send(resp.status, raw=blob, extra={"X-Pool-Upstream": up.id})
            _close(conn)
            return _nores()
        content = ""
        for ch in (j.get("choices") or []):
            if not isinstance(ch, dict):
                continue
            msg = ch.get("message") or {}
            c = msg.get("content")
            if isinstance(c, str):
                content += c
            elif isinstance(c, list):
                content += flatten_content(c)
        j["model"] = requested_model
        usage = j.get("usage")
        if not isinstance(usage, dict):
            usage = {}
        try:
            empty = all(int(usage.get(k) or 0) == 0 for k in
                        ("prompt_tokens", "completion_tokens", "total_tokens"))
        except Exception:
            empty = True
        pt = ct = 0
        if empty:
            pt = est_tokens("字" * prompt_chars)
            ct = est_tokens(content)
            j["usage"] = {"prompt_tokens": pt, "completion_tokens": ct,
                          "total_tokens": pt + ct, "estimated": True}
        else:
            pt = _num(usage, "prompt_tokens")
            ct = _num(usage, "completion_tokens")
        pool = j.setdefault("pool", {})
        if isinstance(pool, dict):
            pool["upstream"] = up.id
            pool["account"] = up.acct
        out = json.dumps(j, ensure_ascii=False).encode("utf-8")
        self._send(200, raw=out, extra={"X-Pool-Upstream": up.id})
        _close(conn)
        return {"text": content, "prompt_tokens": pt,
                "completion_tokens": ct, "estimated": empty}

    def _deliver_stream(self, up, conn, resp, rid, requested_model, prompt_chars=0):
        """流式：SSE 逐行透传 + 注释心跳（防中间层空闲超时）。
        只有在首字节写出之前才允许换实例，因此这里的异常一律吃掉不再重投。"""
        self.close_connection = True
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "close")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("X-Pool-Upstream", up.id)
            self.end_headers()
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            _close(conn)
            return ""
        wlock = threading.Lock()
        stop = threading.Event()
        parts = []
        real_pt = real_ct = 0
        usage_seen = [False]

        def keepalive():
            if KEEPALIVE <= 0:
                return
            while not stop.wait(KEEPALIVE):
                try:
                    with wlock:
                        self.wfile.write(b": pool-keepalive\n\n")
                        self.wfile.flush()
                except Exception:
                    return

        t = threading.Thread(target=keepalive, name="pool-keepalive")
        t.daemon = True
        t.start()
        try:
            while True:
                try:
                    line = resp.readline()
                except (OSError, http.client.HTTPException):
                    break
                if not line:
                    break
                text = line.decode("utf-8", "replace").strip()
                done = False
                out = line
                if text.startswith("data:"):
                    data = text[5:].strip()
                    if data == "[DONE]":
                        if not usage_seen[0]:
                            try:
                                pt = real_pt if real_pt > 0 else est_tokens("字" * prompt_chars)
                                ct = real_ct if real_ct > 0 else est_tokens("".join(parts))
                                tail = {
                                    "id": "chatcmpl-pool-usage-%d" % int(time.time() * 1000),
                                    "object": "chat.completion.chunk",
                                    "created": int(time.time()),
                                    "model": requested_model,
                                    "choices": [],
                                    "usage": {
                                        "prompt_tokens": pt,
                                        "completion_tokens": ct,
                                        "total_tokens": pt + ct,
                                    },
                                }
                                with wlock:
                                    self.wfile.write(
                                        ("data: %s\n\n" % json.dumps(tail, ensure_ascii=False)).encode("utf-8"))
                                    self.wfile.flush()
                            except (BrokenPipeError, ConnectionResetError, OSError):
                                pass
                        done = True
                    else:
                        try:
                            obj = json.loads(data)
                        except Exception:
                            obj = None
                        if isinstance(obj, dict):
                            delta = None
                            for ch in (obj.get("choices") or []):
                                if isinstance(ch, dict):
                                    delta = ch.get("delta") or {}
                                    c = delta.get("content") if isinstance(delta, dict) else None
                                    if isinstance(c, str):
                                        parts.append(c)
                            u = obj.get("usage")
                            if isinstance(u, dict):
                                real_pt = max(real_pt, _num(u, "prompt_tokens"))
                                real_ct = max(real_ct, _num(u, "completion_tokens"))
                                if not usage_seen[0]:
                                    usage_seen[0] = True
                            if obj.get("model") != requested_model:
                                obj["model"] = requested_model
                                # 只替换这一行本身，行尾交给上游那张空行去收，
                                # 写成 \n\n 会让 SSE 多出一个空事件
                                out = ("data: %s\n" % json.dumps(obj, ensure_ascii=False)).encode("utf-8")
                try:
                    with wlock:
                        self.wfile.write(out)
                        self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError, OSError):
                    log("[%s] client gone up=%s" % (rid, up.id))
                    break
                if done:
                    break
        finally:
            stop.set()
            _close(conn)
        text = "".join(parts)
        est = real_pt <= 0 and real_ct <= 0
        return {"text": text,
                "prompt_tokens": real_pt if not est else est_tokens("字" * prompt_chars),
                "completion_tokens": real_ct if not est else est_tokens(text),
                "estimated": est}


# --------------------------------------------------------------------------- 模块级工具

def _close(conn):
    """忽略一切异常地关掉上游连接。"""
    try:
        conn.close()
    except Exception:
        pass


def hmac_equal(a, b):
    """常量时间比较，防时序侧信道猜 token。"""
    try:
        return hmac.compare_digest(a or "", b or "")
    except Exception:
        return (a or "") == (b or "")


def main():
    log("ds-pool 启动 | 实例=%d 单机并发=%d 冷却=%ds 排队=%ds 首字节=%ds 心跳=%ds" %
        (len(UPSTREAMS), PER_CONC, COOLDOWN, QUEUE_TIMEOUT,
         FIRST_BYTE_TIMEOUT, KEEPALIVE))
    for u in UPSTREAMS:
        log("  upstream %-6s -> %s  model=%s  account=%s" % (u.id, u.base, u.model, u.acct))
    log("  用量文件 %s | 每 %ds 落盘 | 保留 %d 天历史" %
        (USAGE.path or "<未启用>", USAGE.flush_secs, USAGE.keep_days))
    lt = USAGE.data["lifetime"]
    if lt["req"]:
        log("  载入历史用量: req=%d ok=%d fail=%d (本次启动前已累计)" % (lt["req"], lt["ok"], lt["fail"]))
    threading.Thread(target=USAGE.run, name="pool-usage", daemon=True).start()

    def _term(signum, _frame):
        log("收到信号 %d，退出前把用量落盘" % signum)
        USAGE.close()
        raise SystemExit(0)

    # Windows 没有 SIGHUP，逐个取一下；装不上就跳过，不影响主流程
    for _name in ("SIGTERM", "SIGINT", "SIGHUP"):
        _sig = getattr(signal, _name, None)
        if _sig is None:
            continue
        try:
            signal.signal(_sig, _term)
        except (ValueError, OSError, AttributeError, RuntimeError):
            pass
    if not UPSTREAMS:
        log("FATAL: 没有配置任何上游。请在 /opt/ds-pool/pool.env 里写 UPSTREAM_1=ds1|http://127.0.0.1:8199|...")
        raise SystemExit(2)
    if not TOKENS:
        log("WARN: POOL_TOKENS 为空 —— 本端口不做鉴权，任何人都能调用，请勿暴露公网")
    if not UPSTREAM_TOKEN and not any(u.token for u in UPSTREAMS):
        log("WARN: UPSTREAM_TOKEN 为空 —— 上游 UWA 若开了 AUTH_ENABLED 会全部 401")
    try:
        srv = ThreadingHTTPServer((POOL_HOST, POOL_PORT), Handler)
    except OSError as exc:
        log("FATAL: 监听 %s:%d 失败: %s" % (POOL_HOST, POOL_PORT, exc))
        raise SystemExit(3)
    srv.daemon_threads = True
    log("ds-pool listening on http://%s:%d/v1  (health: /health)" % (POOL_HOST, POOL_PORT))
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        srv.server_close()
        USAGE.close()
        log("ds-pool stopped")


if __name__ == "__main__":
    main()
