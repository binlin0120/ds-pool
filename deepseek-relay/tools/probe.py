# -*- coding: utf-8 -*-
"""四层探针：一条命令告诉你"这个 429 到底是谁造的"。

  python tools/probe.py http://127.0.0.1:8787 sk-anything --n 12
  python tools/probe.py http://127.0.0.1:8790/v1 ck-local --n 3 --stream

它只做一件事：把 N 次连续请求的 status / content-type / 错误归属层 打出来。
这次线上最坑的地方就是"429 有 4 个来源"，光看状态码永远分不清是 nginx、WAF、
ds2api 还是官方容量，所以先分层再谈修复。
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
import urllib.error
import urllib.request

MODEL = "deepseek-chat"
BODY = json.dumps({"model": MODEL,
                   "messages": [{"role": "user", "content": "ping"}],
                   "max_tokens": 8}).encode("utf-8")


def classify(status, headers, raw):
    """返回 (layer, verdict, note)。layer ∈ edge/nginx | relay | upstream | transport."""
    ct = (headers.get("Content-Type") or "").lower()
    server = (headers.get("Server") or "").lower()
    text = (raw or b"").decode("utf-8", "replace")
    if status == 0:
        return "transport", "network_error", text[:120]
    if "text/html" in ct or text.lstrip().startswith("<"):
        who = "edge/nginx"
        if "cloudflare" in server or "block-event-id" in text.lower() or "request blocked" in text.lower():
            who = "edge/WAF"
        return who, "html_" + str(status), (text.strip().splitlines() or [""])[0][:100]
    try:
        obj = json.loads(text) if text.strip() else {}
    except Exception:
        return "unknown", "non_json", text[:120]
    if isinstance(obj, dict) and obj.get("relay"):
        r = obj["relay"]
        return "relay", str(obj.get("error", {}).get("code", "?")), \
            "retry_after=%s rid=%s" % (r.get("retry_after_seconds"), r.get("request_id"))
    err = obj.get("error") if isinstance(obj, dict) else None
    if isinstance(err, dict):
        msg = str(err.get("message") or "")
        low = msg.lower()
        if "risk" in low or "login failed" in low:
            verdict = "credential_risk"
        elif "capacity" in low or "is full" in low:
            verdict = "upstream_capacity"
        elif "insufficient" in low or "quota" in low:
            verdict = "quota"
        elif status in (401, 403):
            verdict = "auth_invalid"
        elif status == 429:
            verdict = "rpm_or_bare429"
        else:
            verdict = str(err.get("code") or err.get("type") or "upstream_error")
        return "upstream", verdict, msg[:110]
    if isinstance(obj, dict) and obj.get("choices") is not None:
        return "upstream", "ok", "content returned"
    if isinstance(obj, dict) and obj.get("data"):
        return "upstream", "ok", "models list"
    return "unknown", "shape?", text[:110]


def one(base: str, key: str, timeout: float, stream: bool):
    url = base.rstrip("/") + "/chat/completions"
    req = urllib.request.Request(url, data=BODY, method="POST",
                                 headers={"Authorization": "Bearer " + key,
                                          "Content-Type": "application/json"})
    t0 = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read(65536)
            st, hd = r.status, dict(r.headers)
    except urllib.error.HTTPError as e:
        raw = e.read(65536)
        st, hd = e.code, dict(e.headers or {})
    except Exception as exc:
        return 0, {}, b"%s: %s" % (type(exc).__name__, exc), time.monotonic() - t0, ""
    snippet = ""
    if stream:
        head = raw.decode("utf-8", "replace")
        snippet = "sse" if "data:" in head else "no-data-frame"
    return st, hd, raw, time.monotonic() - t0, snippet


def main() -> int:
    ap = argparse.ArgumentParser(prog="probe", description="分层定位 429/401 来源")
    ap.add_argument("base", help="例如 http://127.0.0.1:8787 或 .../v1")
    ap.add_argument("key", help="入站 key（ds2api 现网是 sk-anything）")
    ap.add_argument("--n", type=int, default=10, help="连续请求次数（默认 10，用于复现限流）")
    ap.add_argument("--interval", type=float, default=0.0, help="每次间隔秒；0 = 连击")
    ap.add_argument("--timeout", type=float, default=60.0)
    ap.add_argument("--stream", action="store_true", help="顺带看首帧是不是 SSE")
    args = ap.parse_args()

    rows, lat = [], []
    for i in range(args.n):
        st, hd, raw, dt, extra = one(args.base, args.key, args.timeout, args.stream)
        layer, verdict, note = classify(st, hd, raw)
        lat.append(dt)
        rows.append((i + 1, st, layer, verdict, dt, (note + (" " + extra if extra else ""))[:80]))
        print("%2d/%d  HTTP=%-4s %-11s %-18s %5.2fs  %s"
              % (i + 1, args.n, st or "-", layer, verdict, dt, note + (" " + extra if extra else "")))
        if args.interval:
            time.sleep(args.interval)

    ok = sum(1 for r in rows if r[3].startswith("ok"))
    print("\n--- 汇总 ---")
    print("成功 %d/%d | 延迟 p50=%.2fs p95=%.2fs" %
          (ok, len(rows), statistics.median(lat), sorted(lat)[max(0, int(len(lat) * .95) - 1)]))
    tally = {}
    for _i, _s, layer, verdict, _d, _n in rows:
        tally[(layer, verdict)] = tally.get((layer, verdict), 0) + 1
    for (layer, verdict), n in sorted(tally.items(), key=lambda x: -x[1]):
        print("  %-12s %-20s x%d" % (layer, verdict, n))

    print("\n--- 结论 ---")
    layers = {l for _i, _s, l, _v, _d, _n in rows}
    verdicts = {v for _i, _s, _l, v, _d, _n in rows}
    if "edge/nginx" in layers:
        print("× 有请求被 nginx 边缘限流掐掉（HTML 响应）。先修配置：fix/apply_nginx_fix.sh")
    if "edge/WAF" in layers:
        print("× 命中 DeepSeek 边缘 WAF。换出口 IP（住宅），重试无意义。")
    if any(v in ("credential_risk",) for v in verdicts):
        print("× 上游登录被风控（RISK_DEVICE_DETECTED）。这是账号/设备指纹问题，不是并发问题。")
    if "upstream_capacity" in verdicts:
        print("· 上游整体容量满。只能退避重试，别罚 key、别换出口。")
    if "auth_invalid" in verdicts:
        print("× key 无效/失效。换 key，与 IP 无关。")
    if "quota" in verdicts:
        print("× 余额问题。充值或换 key。")
    if "transport" in layers:
        print("× 连接层就没通（超时/拒绝）。查进程是否存活与安全组。")
    if ok == len(rows):
        print("√ 全绿：这条链路是健康的。把 --n 加大或 --interval 调 0 再压一轮看限流。")
    return 0 if ok == len(rows) else 1


if __name__ == "__main__":
    raise SystemExit(main())