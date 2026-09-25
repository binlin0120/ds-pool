# -*- coding: utf-8 -*-
"""ds-pool 会话粘滞 + 增量发送。

目标：同一个 agent 会话的多轮请求打进同一个 DeepSeek 网页会话，只补发新增回合，
把每次几万字的历史重传降到只发新增的几百字，同时避开 UWA 超长分块丢上下文的坑。

做法：
  * 对归一化后的 messages 逐条算累积哈希，作为「已投递前缀」的指纹；
  * 新请求进来时，找指纹能对上、且没过期、且仍占着那台实例的会话 -> 只发尾回合，
    并用「续用」预设让 UWA 跳过新建对话；
  * 对不上（换实例 / 客户端改历史 / 过期 / 上次失败 / 首轮）一律退回全量 + 默认预设，
    默认预设会点「新建对话」，所以网页侧一定是干净的空会话。

状态只在内存：进程重启即清空，重启后第一个请求自动走全量新建，行为等同改造前。
"""

import hashlib
import json
import os
import threading
import time
import uuid
from collections import OrderedDict


def _flag(name, default):
    v = (os.environ.get(name, "") or "").strip().lower()
    if not v:
        return default
    return v not in ("0", "false", "no", "off")


def _num(name, default, cast=int):
    try:
        return cast((os.environ.get(name, "") or default))
    except (TypeError, ValueError):
        return default


ENABLED = _flag("DS_SESS_INCREMENT", True)
TTL = _num("DS_SESS_TTL", 1800)                 # 会话指纹存活秒数，超时就全量重建
CONT_PRESET = (os.environ.get("DS_PRESET_CONT", "") or "续用").strip()
MAX_SESSIONS = _num("DS_SESS_MAX", 128)         # 指纹表容量，超了丢最久没用的
MIN_REUSE_CHARS = _num("DS_SESS_MIN_REUSE", 200)  # 尾回合太小也没省，直接全量
IMAGE_NOMINAL_CHARS = _num("DS_SESS_IMAGE_CHARS", 2000)  # 一张图/文件按这么多字符估工作量

_LOCK = threading.Lock()
_SESS = OrderedDict()   # sid -> dict
_BOUND = {}             # upstream_id -> sid，记录「这台实例的网页会话现在归谁」
_METRICS = {"reuse": 0, "full": 0, "moved": 0, "dropped": 0}


# 工具调用 id 这类每次都可能变的东西，不参与指纹计算
_VOLATILE = ("id", "call_id", "tool_call_id")


def _canon(m):
    d = {}
    for k, v in m.items():
        if k in _VOLATILE:
            continue
        if k == "tool_calls" and isinstance(v, list):
            fixed = []
            for tc in v:
                if not isinstance(tc, dict):
                    fixed.append(tc)
                    continue
                tc = {kk: vv for kk, vv in tc.items() if kk not in _VOLATILE}
                fixed.append(tc)
            d[k] = fixed
        else:
            d[k] = v
    return json.dumps(d, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _chain(msgs):
    out = []
    prev = ""
    for m in msgs:
        try:
            body = _canon(m) if isinstance(m, dict) else json.dumps(
                m, ensure_ascii=False, sort_keys=True)
        except Exception:
            body = repr(m)
        prev = hashlib.sha256((prev + "\x00" + body).encode("utf-8")).hexdigest()[:24]
        out.append(prev)
    return out


def _chars(msgs):
    """估算已送进网页的文本量：字符串照旧；数组 content 数文本 part，附件给名义权重。"""
    total = 0
    for m in msgs:
        c = m.get("content") if isinstance(m, dict) else None
        if isinstance(c, str):
            total += len(c)
        elif isinstance(c, list):
            for p in c:
                if isinstance(p, str):
                    total += len(p)
                    continue
                if not isinstance(p, dict):
                    continue
                t = str(p.get("type") or "").strip().lower()
                if t in ("image_url", "input_image", "image", "file",
                         "input_file", "document"):
                    total += IMAGE_NOMINAL_CHARS
                else:
                    v = p.get("text") or p.get("content")
                    if isinstance(v, str):
                        total += len(v)
    return total


def _evict(now):
    """清掉过期指纹。调用方需持锁。"""
    dead = [sid for sid, s in _SESS.items() if now - s["last"] > TTL]
    for sid in dead:
        _drop_locked(sid)
        _METRICS["dropped"] += 1
    while len(_SESS) > MAX_SESSIONS:
        oldest = next(iter(_SESS))
        _drop_locked(oldest)
        _METRICS["dropped"] += 1


def _drop_locked(sid):
    s = _SESS.pop(sid, None)
    if s and _BOUND.get(s.get("up")) == sid:
        _BOUND.pop(s.get("up"), None)


def plan(msgs, model):
    """为这次请求挑一个可复用的网页会话。返回句柄 dict（或 None）。"""
    if not ENABLED or not msgs:
        return None
    chain = _chain(msgs)
    now = time.time()
    with _LOCK:
        _evict(now)
        best = None
        for sid, s in _SESS.items():
            if s["bad"] or s["model"] != model:
                continue
            k = s["n"]
            if k < 1 or k > len(chain):
                continue
            if s["chain"][k - 1] != chain[k - 1]:
                continue
            if now - s["last"] > TTL:
                continue
            if best is None or k > best[0]:
                best = (k, sid)
        if best:
            k, sid = best
            s = _SESS[sid]
            s["last"] = now
            _SESS.move_to_end(sid)
            return {"sid": sid, "k": k, "prefer": s["up"], "chain": chain,
                    "model": model, "total": len(msgs)}
        sid = uuid.uuid4().hex[:12]
        _SESS[sid] = {"up": None, "chain": chain, "n": 0, "last": now,
                      "model": model, "turns": 0, "bad": 0}
        _SESS.move_to_end(sid)
        return {"sid": sid, "k": 0, "prefer": None, "chain": chain,
                "model": model, "total": len(msgs)}


def frame(h, up_id, full_msgs):
    """决定发什么：(消息列表, 预设名或 None, 实发字符数, 模式)。

    复用必须同时满足：有已投递前缀、这台实例确实归本会话、尾回合非空且够省。
    任何一条不满足 -> 全量 + 默认预设（UWA 会新建对话），绝不猜。
    """
    if h is None:
        return full_msgs, None, _chars(full_msgs), "off"
    k = h["k"]
    n = len(full_msgs)
    if k and k < n:
        with _LOCK:
            owner = _BOUND.get(up_id)
        if owner == h["sid"]:
            tail = full_msgs[k:]
            tc = _chars(tail)
            if CONT_PRESET and tc >= MIN_REUSE_CHARS:
                return tail, CONT_PRESET, tc, "reuse"
            return full_msgs, None, _chars(full_msgs), "full-small"
        if k:
            _METRICS["moved"] += 1
    return full_msgs, None, _chars(full_msgs), "full"


def commit(h, up_id, full_msgs):
    """请求成功后才登记进度：这台实例的网页会话现在装着前 n 条消息。"""
    if h is None:
        return
    chain = h["chain"]
    n = len(full_msgs)
    now = time.time()
    with _LOCK:
        s = _SESS.get(h["sid"])
        if s is None:
            s = {"up": None, "chain": chain, "n": 0, "last": now,
                 "model": h["model"], "turns": 0, "bad": 0}
            _SESS[h["sid"]] = s
        prev = _BOUND.get(up_id)
        if prev and prev != h["sid"] and prev in _SESS:
            _SESS[prev]["bad"] = 1
            _SESS[prev]["n"] = 0
        _BOUND[up_id] = h["sid"]
        s["up"] = up_id
        s["chain"] = chain
        s["n"] = n
        s["last"] = now
        s["turns"] += 1
        _SESS.move_to_end(h["sid"])
        _evict(now)


def fault(h, up_id):
    """这次没成功：网页侧到底吃进去多少已经说不准，作废指纹，下次全量重建。"""
    if h is None:
        return
    with _LOCK:
        s = _SESS.get(h["sid"])
        if s is not None:
            s["bad"] = 1
            s["n"] = 0
        if _BOUND.get(up_id) == h["sid"]:
            _BOUND.pop(up_id, None)


def bound_ids():
    """现在被活会话占着的实例 id，新会话优先挑没人占的。"""
    now = time.time()
    with _LOCK:
        out = set()
        for uid_, sid in _BOUND.items():
            s = _SESS.get(sid)
            if s and not s["bad"] and now - s["last"] <= TTL:
                out.add(uid_)
        return out


def note_mode(mode):
    if mode == "reuse":
        _METRICS["reuse"] += 1
    elif mode and mode.startswith("full"):
        _METRICS["full"] += 1


def snapshot():
    now = time.time()
    with _LOCK:
        rows = []
        for sid in list(_SESS.keys())[::-1]:
            s = _SESS[sid]
            rows.append({"sid": sid, "upstream": s["up"], "turns": s["turns"],
                         "delivered": s["n"], "of": len(s["chain"]),
                         "idle": round(now - s["last"], 1),
                         "model": s["model"], "bad": bool(s["bad"])})
        return {"enabled": ENABLED, "ttl": TTL, "cont_preset": CONT_PRESET,
                "sessions": len(_SESS), "bound": dict(_BOUND),
                "metrics": dict(_METRICS), "detail": rows[:40]}
