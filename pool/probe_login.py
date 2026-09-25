#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ds-pool 登录健康探针（root 侧，systemd: ds-pool-probe.service）

职责（用户要的「健康指针探测」）：
  1. 校验每台实例的指针文件 /home/webapi/.active_profile.dsN 存在、指向的 profile 目录存在；
  2. 通过 CDP 读取实例页面 URL + DOM 快照，判定登录态：
       - 页面在 chat.deepseek.com/* 且有输入框   -> ok
       - 页面停在 /sign_in                        -> 掉登录：
             * 上轮还是 ok/banned、本轮掉到登录页 = 疑似被强制下线
               -> 不自动重登，排 12-24h 随机冷却，冷却到期才试登一次（失败再排一轮）
             * 其余情况（重启后首次/从未正常）-> 按旧逻辑在 LOGIN_MIN 短冷却后自动重登
               （邮箱+密码，凭据在服务器本地）
       - 页面正文带「已被禁言/违反使用规范」      -> banned -> 不重启、摘出轮询，解封后自动恢复
       - chat.deepseek.com/* 但无输入框           -> no_input -> 重启该实例 Chrome 并复查
       - 没有 chat.deepseek.com 标签页            -> 异常 -> 自动重启该实例的 Chrome 并复查
  3. 判定 banned 后自动停机（封号休眠）：停 chrome-webapiN + uwa-webapiN，
     按页面解封时间排 wake_at（解封+15min 缓冲）自动开机复查；
     解封时间解析失败则随机 6-12h 复查一次；开机后若还在登录页，仍走 12-24h 随机冷却。
  4. 每次动作前先问 ds-pool /pool/status：实例在途 >0 时延迟处理，绝不打断正在跑的请求；
  5. 结果写 /var/lib/ds-pool/login_state.json，供 ds-pool /pool/status 与面板展示。

安全红线：
  * 全程不打印邮箱全文 / 密码 / 任何 token；邮箱只打前 3 字符，密码只打长度；
  * 凭据只在服务器本地读取，不外传；
  * 每实例重启 Chrome 至少间隔 90s、自动重登至少间隔 300s；强制下线则 12-24h 随机冷却，
    避免重登触发风控（9-24 三实例通通因「重登即加速封禁」被禁言）。
  * 冷却/重登/重启时间戳持久化在 /var/lib/ds-pool/login_cooldown.json，
    探针进程重启不丢冷却状态；--now 同样遵守冷却，不会绕过。

用法:
  python3 probe_login.py            # 常驻循环（systemd 用），间隔取 PROBE_INTERVAL
  python3 probe_login.py --now 2    # 只查/修 ds2 一次（switch_account.sh 用）
"""

import json
import http.client
import os
import random
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request

from websocket import create_connection

POOL_ENV = "/opt/ds-pool/pool.env"
POINTER_PREFIX = "/home/webapi/.active_profile.ds"
STATE_FILE = "/var/lib/ds-pool/login_state.json"
CONFIG_JSON = "/opt/ds2api/config.json"
REMOVED_GLOB = "/opt/ds2api/removed_account_*.json"
COOLDOWN_FILE = "/var/lib/ds-pool/login_cooldown.json"
COOL_MIN_SEC = 12 * 3600.0   # 强制下线后的最短随机冷却 12h
COOL_MAX_SEC = 24 * 3600.0   # 最长随机冷却 24h
WAKE_PAD = int(os.environ.get("PROBE_WAKE_PAD", "900"))  # 解封后延迟开机缓冲秒
PARK_RECHECK_MIN_SEC = 6 * 3600.0    # 解封时间解析失败时：最短随机复查周期
PARK_RECHECK_MAX_SEC = 12 * 3600.0   # 最长随机复查周期

UWA_ENVS = {
    1: "/opt/uwa/universal-web-api-main/.env",
    2: "/opt/uwa2/universal-web-api/.env",
    3: "/opt/uwa3/universal-web-api/.env",
}

INTERVAL = int(os.environ.get("PROBE_INTERVAL", "120"))
RESTART_MIN = int(os.environ.get("PROBE_RESTART_MIN", "90"))
LOGIN_MIN = int(os.environ.get("PROBE_LOGIN_MIN", "300"))

# ---- P0 端到端探活 ----
# 登录探针只看 CDP 页面 URL，页面卡死但 URL 不变时会误判为 ok。
# 这里每隔 LIVENESS_EVERY 轮发一个极短的真实请求，验证「端到端可用」。
# 失败连续 LIVENESS_BAD_MAX 次 -> 摘除（标记 unhealthy，供 pool 读取）。
LIVENESS_ENABLED = os.environ.get("PROBE_LIVENESS", "1") not in ("0", "false", "no")
LIVENESS_EVERY = int(os.environ.get("PROBE_LIVENESS_EVERY", "5"))      # 每 N 轮探一次
LIVENESS_TIMEOUT = int(os.environ.get("PROBE_LIVENESS_TIMEOUT", "25"))  # 单次超时秒
LIVENESS_BAD_MAX = int(os.environ.get("PROBE_LIVENESS_BAD_MAX", "2"))   # 连续失败上限
LIVENESS_PROMPT = os.environ.get("PROBE_LIVENESS_PROMPT", "只回复两个字：正常")


_FORCE_LIVE = {"on": False}   # --now 单次模式强制探活
_LIVENESS_BAD = {}      # uid -> 连续探活失败次数
_LIVENESS_DETAIL = {}   # uid -> 最近一次探活详情


def log(msg):
    print(time.strftime("%Y-%m-%dT%H:%M:%S") + " [probe] " + msg, flush=True)


def load_pool_env():
    """解析 pool.env 中 UPSTREAM_n=id|base|model|label|token 与 POOL_TOKENS/POOL_PORT。"""
    data = {}
    try:
        with open(POOL_ENV, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                data[k.strip()] = v.strip()
    except OSError as exc:
        log("!! 读不到 %s: %r" % (POOL_ENV, exc))
    ups = []
    for i in range(1, 17):
        raw = data.get("UPSTREAM_%d" % i)
        if not raw:
            continue
        p = [x.strip() for x in raw.split("|")]
        if len(p) < 2:
            continue
        _u = urllib.parse.urlparse(p[1])
        ups.append({
            "id": p[0],
            "base": p[1].rstrip("/"),
            "model": p[2] if len(p) > 2 and p[2] else "chat.deepseek.com",
            "label": p[3] if len(p) > 3 and p[3] else p[0],
            "token": p[4] if len(p) > 4 and p[4] else "",
            "port": int(_u.port if _u.port
                        else (443 if p[1].startswith("https") else 80)),
        })
    pool = {
        "tokens": {t for t in data.get("POOL_TOKENS", "").split(",") if t.strip()},
        "port": int(data.get("POOL_PORT", "8288")),
        "upstreams": ups,
    }
    return pool


def account_key(label):
    """cohen-p1/bel-p2/upm-p3 -> cohen/bel/upm"""
    m = re.sub(r"-p\d+$", "", label.strip())
    return m.lower()


def uwa_browser_port(inst):
    """从对应 UWA 实例的 .env 读 BROWSER_PORT（CDP 端口），读不到回退 base+1023。"""
    path = UWA_ENVS.get(int(inst))
    if path:
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("BROWSER_PORT="):
                        v = line.split("=", 1)[1].strip().strip('"').strip("'")
                        return int(v)
        except Exception:
            pass
    return 0


def load_credentials():
    """从 ds2api 配置与弃号文件读账号凭据（本函数结果只在服务器内存中使用）。"""
    accs = []
    try:
        with open(CONFIG_JSON, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        for a in cfg.get("accounts", []):
            if a.get("email") and a.get("password"):
                accs.append({"name": str(a.get("name") or "account"), "email": a["email"],
                             "password": a["password"], "hidden": False})
    except Exception as exc:
        log("!! config.json 读取失败: %r" % exc)
    try:
        import glob
        for fp in sorted(glob.glob(REMOVED_GLOB)):
            try:
                with open(fp, "r", encoding="utf-8") as f:
                    data = json.load(f)
                items = data if isinstance(data, list) else [data]
                for a in items:
                    if a.get("email") and a.get("password"):
                        nm = fp.split("removed_account_")[-1].replace(".json", "")
                        accs.append({"name": "hidden:" + nm, "email": a["email"],
                                     "password": a["password"], "hidden": True})
            except Exception as exc:
                log("!! 弃号文件 %s 读取失败: %r" % (fp, exc))
    except Exception:
        pass
    return accs


def pick_credential(accs, acct):
    """按账号名匹配凭据；同名时优先 hidden（弃号），避免碰主账号。"""
    al = acct.lower()
    matched = [a for a in accs if a["email"].lower().startswith(al)]
    if not matched:
        return None
    for a in matched:
        if a["hidden"]:
            return a
    return matched[0]


def mask_email(e):
    try:
        return e[:3] + "***@" + e.split("@")[-1]
    except Exception:
        return "***"


def load_cooldown():
    """读取 /var/lib/ds-pool/login_cooldown.json（强制下线冷却/重登/重启时间戳的持久化副本）。"""
    try:
        with open(COOLDOWN_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("instances") if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_cooldown(cooldown):
    """原子落盘冷却状态。探针进程重启后冷却仍生效，避免 12-24h 随机冷却形同虚设。"""
    try:
        blob = {"updated_at": int(time.time()), "instances": cooldown or {}}
        os.makedirs(os.path.dirname(COOLDOWN_FILE), exist_ok=True)
        tmp = COOLDOWN_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(blob, f, ensure_ascii=False, sort_keys=True)
        os.replace(tmp, COOLDOWN_FILE)
        try:
            subprocess.run(["chown", "dspool:dspool", COOLDOWN_FILE], capture_output=True)
        except Exception:
            pass
    except Exception as exc:
        log("!! 冷却状态写盘失败: %r" % exc)


def relogin_plan(prev_page, entry, now):
    """掉登录后的处理决策（纯函数，便于单测）。

    返回 dict:
      action: "cooldown"（排队冷却，不许重登）| "login"（可以尝试恢复登录）
      reason: forced_first（首次确认强制下线）/ forced_wait（冷却中）/
              forced_retry（冷却到期重试）/ normal（普通掉登录）
      next_login_at / wait_hours / fail_count 供上层写日志与状态。
    """
    forced_at = entry.get("forced_logout_at")
    next_at = float(entry.get("next_login_at") or 0)
    fail_count = int(entry.get("login_fail_count") or 0)
    if forced_at is None and prev_page in ("ok", "banned"):
        # 上轮还正常/禁言中，本轮掉到登录页 => 被强制下线，先冷却不重登
        return {"action": "cooldown", "reason": "forced_first",
                "next_login_at": 0, "wait_hours": 0.0, "fail_count": fail_count}
    if forced_at is not None and next_at > now:
        return {"action": "cooldown", "reason": "forced_wait",
                "next_login_at": next_at, "wait_hours": (next_at - now) / 3600.0,
                "fail_count": fail_count}
    reason = "forced_retry" if forced_at is not None else "normal"
    return {"action": "login", "reason": reason,
            "next_login_at": next_at, "wait_hours": 0.0, "fail_count": fail_count}


def parse_banned_time(text):
    """把 banned_until 文本转成 epoch（本地时区）；解析失败返回 None。"""
    s = (text or "").strip().replace(" ", "")
    if not s:
        return None
    for fmt in ("%Y年%m月%d日%H:%M", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M"):
        try:
            return int(time.mktime(time.strptime(s, fmt)))
        except (ValueError, TypeError):
            continue
    return None


def fmt_wake(ts):
    try:
        return time.strftime("%m-%d %H:%M", time.localtime(ts))
    except Exception:
        return str(ts)


def service_active(name):
    try:
        r = subprocess.run(["systemctl", "is-active", name],
                           capture_output=True, text=True, timeout=15)
        return (r.stdout or "").strip() == "active"
    except Exception:
        return False


def browser_cdp_port(up, inst):
    return uwa_browser_port(inst) or (up["port"] + 1023)


def boot_instance(inst_no, cdp_port):
    """封号到点开机：start chrome -> 等 CDP -> start uwa。返回 (ok, 描述)。"""
    chrome = "chrome-webapi%s.service" % ("" if inst_no == 1 else inst_no)
    uwa = "uwa-webapi%s.service" % ("" if inst_no == 1 else inst_no)
    try:
        subprocess.run(["systemctl", "start", chrome], capture_output=True, timeout=40)
    except Exception as exc:
        return False, "启动 %s 失败: %r" % (chrome, exc)
    ready = False
    for _ in range(30):
        if cdp_tabs(cdp_port) is not None:
            ready = True
            break
        time.sleep(2)
    if not ready:
        return False, "CDP :%d 60s 未就绪" % cdp_port
    try:
        subprocess.run(["systemctl", "start", uwa], capture_output=True, timeout=40)
    except Exception as exc:
        log("!! 启动 %s 失败: %r（Chrome 已起，继续复查）" % (uwa, exc))
    return True, "chrome up + CDP :%d 就绪" % cdp_port


def stop_instance(inst_no):
    """封号停机：停 chrome + uwa（先 chrome 后 uwa，与开机顺序相反）。"""
    chrome = "chrome-webapi%s.service" % ("" if inst_no == 1 else inst_no)
    uwa = "uwa-webapi%s.service" % ("" if inst_no == 1 else inst_no)
    try:
        subprocess.run(["systemctl", "stop", chrome], capture_output=True, timeout=40)
    except Exception:
        pass
    try:
        subprocess.run(["systemctl", "stop", uwa], capture_output=True, timeout=40)
    except Exception:
        pass


def park_banned(entry, unban_text, now):
    """banned -> 排停机计划。返回 (wake_at, is_random_recheck)。
    已存在未过期的停机计划且解封时间没变则沿用，不重复停机。"""
    unban_ts = parse_banned_time(unban_text) if unban_text else None
    existing = entry.get("parked") or {}
    if existing and existing.get("unban_at") == unban_ts and float(existing.get("wake_at") or 0) > now:
        return float(existing["wake_at"]), bool(existing.get("fallback"))
    if unban_ts is not None and unban_ts > now:
        wake = unban_ts + WAKE_PAD
        fallback = False
    else:
        wake = now + random.uniform(PARK_RECHECK_MIN_SEC, PARK_RECHECK_MAX_SEC)
        fallback = True
    entry["parked"] = {
        "parked_at": int(now),
        "unban_at": unban_ts,
        "unban_text": unban_text or None,
        "wake_at": wake,
        "fallback": fallback,
        "boot_fail": 0,
    }
    return wake, fallback

def cdp_tabs(port):
    try:
        with urllib.request.urlopen("http://127.0.0.1:%d/json" % port, timeout=6) as r:
            return json.load(r)
    except Exception:
        return None


def find_chat_tab(tabs):
    if not isinstance(tabs, list):
        return None
    for t in tabs:
        if t.get("type") == "page" and "chat.deepseek.com" in (t.get("url") or ""):
            return t
    return None


JS_PAGE_SNAPSHOT = r"""
(function(){
  function vis(e){return e && e.offsetParent!==null;}
  var txt=document.body?document.body.innerText:'';
  var eds=[].slice.call(document.querySelectorAll('textarea,div[contenteditable="true"]')).filter(vis);
  var inps=[].slice.call(document.querySelectorAll('input')).filter(vis);
  return {
    has_textarea: eds.length>0,
    input_ph: (eds[0]&&eds[0].placeholder)||'',
    has_input: inps.length>0,
    body_len:(txt||'').length,
    body:(txt||'').slice(-600)
  };
})()
"""


def page_snapshot(cdp):
    """对 chat 标签页做一次 DOM 快照（有无输入框/页面文案）。返回 (ok, snap|None)。"""
    try:
        r = cdp.eval(JS_PAGE_SNAPSHOT)
        if isinstance(r, dict) and "has_textarea" in r:
            return True, r
        return False, None
    except Exception as exc:
        log("!! DOM 快照失败: %r" % exc)
        return False, None


def banned_hint(text):
    """页面正文里是否带禁言/封禁提示。命中返回 True。"""
    if not text:
        return False
    return bool(re.search(r"(已被禁言|账号.{0,10}(禁言|限制)|违反.{0,12}使用规范|被限制使用|暂时无法使用)", text))


def banned_until(text):
    """尽量从页面文案里抠出解封时间，抠不到返回 None。"""
    if not text:
        return None
    m = re.search(r"(?:禁言|限制).{0,50}?(\d{4}\s*年\s*\d{1,2}\s*月\s*\d{1,2}\s*日\s*\d{1,2}:\d{2})", text)
    if not m:
        m = re.search(r"(\d{4}[-/]\d{1,2}[-/]\d{1,2}[ T]\d{1,2}:\d{2})", text)
    return m.group(1).replace(" ", "") if m else None


def pool_inflight(uid, pool):
    """问 ds-pool 该实例当前在途请求数；查询失败按 0 处理（探针只读，不打断）。"""
    if not pool["tokens"]:
        return 0
    tok = next(iter(pool["tokens"]))
    try:
        req = urllib.request.Request(
            "http://127.0.0.1:%d/pool/status" % pool["port"],
            headers={"Authorization": "Bearer " + tok})
        with urllib.request.urlopen(req, timeout=5) as r:
            j = json.load(r)
        for u in j.get("upstreams") or []:
            if u.get("id") == uid:
                return int(u.get("inflight") or 0)
    except Exception:
        pass
    return 0


def restart_chrome(inst):
    try:
        subprocess.run(["systemctl", "restart", "chrome-webapi%s.service" % inst],
                       capture_output=True, text=True, timeout=40)
        return True
    except Exception as exc:
        log("!! 重启 chrome-webapi%s 失败: %r" % (inst, exc))
        return False


# ---------------------------------------------------------------- CDP 登录脚本
JS_PWTAB = r"""
(function(){
  var els = [].slice.call(document.querySelectorAll('button,span,a,div,[role=button],li'));
  var t = els.filter(function(e){return (e.innerText||'').trim()==='密码登录' && e.offsetParent!==null;});
  if(!t.length) return 'NO_PW_TAB';
  var el = t[t.length-1];
  (el.closest('button')||el.closest('[role=tab]')||el).click();
  return 'CLICKED_PW_TAB n=' + t.length;
})()
"""

JS_AGREE = r"""
(function(){
  function vis(e){return e && e.offsetParent!==null;}
  var box = [].slice.call(document.querySelectorAll('input[type=checkbox],[role=checkbox]'))
              .filter(function(i){return i.checked===false || i.getAttribute('aria-checked')==='false';});
  if(!box.length) return 'NO_UNCHECKED_BOX';
  var el = box[0];
  (el.closest('label')||el.parentElement||el).click();
  return 'CLICKED_AGREE n=' + box.length;
})()
"""

JS_FILL = r"""
(function(v, kind){
  function setVal(el, val){
    var setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
    setter.call(el, val);
    el.dispatchEvent(new Event('input', {bubbles:true}));
    el.dispatchEvent(new Event('change', {bubbles:true}));
  }
  var all = [].slice.call(document.querySelectorAll('input')).filter(function(i){return i.offsetParent!==null;});
  var el;
  if (kind === 'pass') {
    el = all.filter(function(i){return i.type==='password';})[0]
      || all.filter(function(i){return /密码|password/i.test(i.placeholder||'');})[0];
    if (!el) return 'NO_PASS_INPUT';
    el.focus(); setVal(el, v); return 'PASS_SET len=' + el.value.length;
  }
  el = all.filter(function(i){return i.type!=='checkbox' && i.type!=='hidden' &&
      /email|邮箱|账号|帐号|username|手机/i.test((i.placeholder||'')+(i.type||'')+(i.name||''));})[0] || all[0];
  if (!el) return 'NO_EMAIL_INPUT';
  el.focus(); setVal(el, v);
  return 'EMAIL_SET ph=' + (el.placeholder||'-') + ' type=' + el.type;
})(__SVAL__, __KIND__)
"""

JS_LOGIN = r"""
(function(){
  var els = [].slice.call(document.querySelectorAll('button,[role=button]'));
  var t = els.filter(function(e){return (e.innerText||'').trim()==='登录' && e.offsetParent!==null;});
  if(!t.length) return 'NO_LOGIN_BUTTON';
  t[0].click();
  return 'CLICKED_LOGIN';
})()
"""


class Cdp:
    def __init__(self, tab):
        self.ws = create_connection(tab["webSocketDebuggerUrl"], timeout=60, suppress_origin=True)
        self._id = [0]

    def send(self, method, params=None):
        self._id[0] += 1
        mid = self._id[0]
        self.ws.send(json.dumps({"id": mid, "method": method, "params": params or {}}))
        while True:
            m = json.loads(self.ws.recv())
            if m.get("id") == mid:
                if "error" in m:
                    return {"__error__": m["error"]}
                return m.get("result", {})

    def eval(self, expr):
        r = self.send("Runtime.evaluate",
                      {"expression": expr, "returnByValue": True, "awaitPromise": True})
        if "__error__" in r:
            return {"err": r["__error__"]}
        return r.get("result", {}).get("value")

    def close(self):
        try:
            self.ws.close()
        except Exception:
            pass


def password_login(port, email, password):
    """对 CDP 页面执行 邮箱+密码 登录，返回 (ok, 描述)。描述不含密码。"""
    tabs = cdp_tabs(port)
    tab = find_chat_tab(tabs)
    if tab is None:
        return False, "no chat tab"
    c = Cdp(tab)
    try:
        c.send("Page.enable")
        c.send("Runtime.enable")
        c.send("Page.navigate", {"url": "https://chat.deepseek.com/sign_in"})
        time.sleep(4)
        pw = c.eval(JS_PWTAB)
        time.sleep(1.5)
        fill_mail = c.eval(JS_FILL.replace("__SVAL__", json.dumps(email)).replace("__KIND__", json.dumps("mail")))
        time.sleep(0.5)
        fill_pass = c.eval(JS_FILL.replace("__SVAL__", json.dumps(password)).replace("__KIND__", json.dumps("pass")))
        time.sleep(0.5)
        clk = c.eval(JS_LOGIN)
        time.sleep(9)
        url = c.eval("location.href")
        if isinstance(url, str) and url and "/sign_in" not in url:
            return True, "%s | %s | %s" % (pw, fill_mail, fill_pass)
        # 第一次失败：多半是协议复选框没勾，勾上再试一次
        agree = c.eval(JS_AGREE)
        time.sleep(0.6)
        c.eval(JS_FILL.replace("__SVAL__", json.dumps(email)).replace("__KIND__", json.dumps("mail")))
        time.sleep(0.4)
        c.eval(JS_FILL.replace("__SVAL__", json.dumps(password)).replace("__KIND__", json.dumps("pass")))
        time.sleep(0.4)
        c.eval(JS_LOGIN)
        time.sleep(9)
        url2 = c.eval("location.href")
        if isinstance(url2, str) and url2 and "/sign_in" not in url2:
            return True, "retry-after-agree | %s | %s" % (pw, agree)
        return False, "still on sign_in (pw=%s mail=%s pass=%s agree=%s)" % (pw, fill_mail, fill_pass, agree)
    finally:
        c.close()



def uwa_liveness(inst, up, pool):
    """端到端探活：向该实例的 UWA 发一个极短的 chat 请求。

    返回 (ok, detail)。ok=True 表示这台机器能真正完成一次对话。
    仅用于判定健康，不写业务统计、不占号池队列。
    """
    if not LIVENESS_ENABLED:
        return True, "liveness disabled"
    port = up.get("port")
    if not port:
        return False, "no port"
    token = up.get("token") or ""
    body = json.dumps({
        "model": up.get("model") or "chat.deepseek.com",
        "messages": [{"role": "user", "content": LIVENESS_PROMPT}],
        "stream": False,
        "max_tokens": 16,
    }, ensure_ascii=False).encode("utf-8")
    hdrs = {"Content-Type": "application/json",
            "Content-Length": str(len(body)),
            "Accept": "application/json"}
    if token:
        hdrs["Authorization"] = "Bearer " + token
    t0 = time.time()
    try:
        conn = http.client.HTTPConnection("127.0.0.1", int(port), timeout=LIVENESS_TIMEOUT)
        conn.request("POST", "/v1/chat/completions", body=body, headers=hdrs)
        resp = conn.getresponse()
        raw = resp.read()
        conn.close()
        ms = int((time.time() - t0) * 1000)
        if resp.status >= 500:
            return False, "HTTP %d (%dms)" % (resp.status, ms)
        txt = raw.decode("utf-8", "replace")
        if not txt.strip():
            return False, "空响应 (%dms)" % ms
        try:
            j = json.loads(txt)
            ch = (j.get("choices") or [{}])[0]
            content = (ch.get("message") or {}).get("content") or ""
            if not str(content).strip():
                return False, "无内容 (%dms)" % ms
        except Exception:
            # 非 JSON 但非空，放行（可能有 SSE 噪声）
            pass
        return True, "ok (%dms)" % ms
    except Exception as exc:
        ms = int((time.time() - t0) * 1000)
        return False, "%s (%dms)" % (type(exc).__name__, ms)


def check_instance(inst, up, pool, accs, actions, cooldown=None):
    """单实例巡检。返回该实例当前 state 字符串。"""
    uid = up["id"]
    inst_no = str(inst)
    pointer = POINTER_PREFIX + inst_no
    cooldown = cooldown if isinstance(cooldown, dict) else {}
    entry = cooldown.setdefault(uid, {})
    prev_page = entry.get("prev_page") or ""
    result = {"state": "unknown", "checked_at": int(time.time()),
              "account": account_key(up["label"]), "label": up["label"], "error": None}
    try:
        with open(pointer, "r", encoding="utf-8") as f:
            profile = f.read().strip()
    except OSError:
        profile = ""
    result["profile"] = profile or None
    if not profile:
        result["state"] = "no_pointer"
        result["error"] = "指针文件缺失: %s" % pointer
        entry["prev_page"] = result["state"]
        return result
    if not os.path.isdir(profile):
        result["state"] = "no_profile"
        result["error"] = "profile 目录不存在: %s" % profile
        entry["prev_page"] = result["state"]
        return result

    cdp = uwa_browser_port(inst) or (up["port"] + 1023)
    tabs = cdp_tabs(cdp)
    if tabs is None:
        result["state"] = "cdp_down"
        result["error"] = "CDP :%d 连不上" % cdp
    else:
        tab = find_chat_tab(tabs)
        if tab is None:
            result["state"] = "no_tab"
            result["error"] = "没有 chat.deepseek.com 标签页"
        elif "/sign_in" in (tab.get("url") or ""):
            result["state"] = "signed_out"
            result["error"] = "页面停在登录页"
        else:
            # 只看 URL 会把「禁言页/无输入框」误判为 ok（正是此前 ds3 死循环的根因）。
            # DOM 快照：禁言 -> banned（不修复、摘除轮询）；有输入框 -> ok；其余 -> no_input（重启修复）。
            c = Cdp(tab)
            try:
                ok_snap, snap = page_snapshot(c)
            finally:
                c.close()
            if not ok_snap:
                # DOM 探测失败：退回旧行为，交给端到端探活兜底
                result["state"] = "ok"
                result["url"] = (tab.get("url") or "")[:80]
            elif banned_hint(snap.get("body")):
                result["state"] = "banned"
                until = banned_until(snap.get("body"))
                result["banned_until"] = until
                result["error"] = "账号被禁言" + (("，预计恢复约 %s" % until) if until else "（未见恢复时间）")
            elif snap.get("has_textarea"):
                result["state"] = "ok"
                result["url"] = (tab.get("url") or "")[:80]
                # 登录态恢复：清除之前的强制下线冷却标记（如有）
                if entry.get("forced_logout_at"):
                    log("实例 %s 登录态已恢复，清除强制下线冷却标记" % uid)
                entry["forced_logout_at"] = None
                entry["next_login_at"] = 0
            else:
                result["state"] = "no_input"
                result["error"] = "页面无输入框(URL=%s)" % (tab.get("url") or "")[:60]

    # 自动修复（前提：该实例当前没有在途请求）
    inflight = pool_inflight(uid, pool)
    now = time.time()
    fixable = result["state"] in ("no_tab", "cdp_down", "signed_out", "no_input")
    if not fixable:
        entry["prev_page"] = result["state"]
        return result
    if inflight > 0:
        result["state"] = "deferred"
        result["error"] = (result["error"] or "") + "；在途 %d，延迟处理" % inflight
        # deferred 时未真正看到页面，prev_page 保持上一轮值，避免丢掉有力的强制下线信号
        return result

    last_restart = float(entry.get("last_restart_at") or 0)
    if result["state"] in ("no_tab", "cdp_down", "no_input") and now - last_restart >= RESTART_MIN:
        log("实例 %s 异常(%s)，自动重启 chrome-webapi%s" % (uid, result["state"], inst_no))
        ok = restart_chrome(inst_no)
        entry["last_restart_at"] = now
        actions.setdefault("restart", {})[uid] = now
        time.sleep(15)
        tabs2 = cdp_tabs(cdp)
        tab2 = find_chat_tab(tabs2)
        if ok and tab2 is not None and "/sign_in" not in (tab2.get("url") or ""):
            result["state"] = "ok"
            result["error"] = None
            result["restarted"] = True
            if entry.get("forced_logout_at"):
                entry["forced_logout_at"] = None
                entry["next_login_at"] = 0
        else:
            result["error"] = (result["error"] or "") + "；重启后未恢复"

    if result["state"] == "signed_out":
        cred = pick_credential(accs, account_key(up["label"]))
        plan = relogin_plan(prev_page, entry, now)
        if plan["action"] == "cooldown":
            if plan["reason"] == "forced_first":
                # 首次确认「被强制下线」：只排 12-24h 随机冷却，本窗口绝不自动重登
                entry["forced_logout_at"] = now
                entry["next_login_at"] = now + random.uniform(COOL_MIN_SEC, COOL_MAX_SEC)
                entry["login_fail_count"] = 0
                nxt = entry["next_login_at"]
                result["state"] = "cooling"
                result["relogin_mode"] = "forced_cooldown"
                result["next_login_at"] = int(nxt)
                result["error"] = "疑似被强制下线，%.1f 小时后自动尝试重登（本窗口不自动登录）" % ((nxt - now) / 3600)
                log("实例 %s 疑似被强制下线（上轮 %s -> 本轮登录页），安排 %.1fh 随机冷却后重登" %
                    (uid, prev_page, (nxt - now) / 3600))
            else:  # forced_wait
                result["state"] = "cooling"
                result["relogin_mode"] = "forced_cooldown"
                result["next_login_at"] = int(plan["next_login_at"])
                result["error"] = "被强制下线冷却中，%.1f 小时后自动尝试重登" % plan["wait_hours"]
        else:
            # 普通掉登录（重启/首次巡检/从未正常）或强制冷却已到期：
            # 普通路径受 LOGIN_MIN 短冷却约束；强制到期路径等待 12-24h 后直接试一次
            forced_retry = plan["reason"] == "forced_retry"
            mode = "forced_retry" if forced_retry else "normal"
            if cred is None:
                result["error"] = (result["error"] or "") + "；无可用凭据，需手动登录"
            elif forced_retry or now - float(entry.get("last_login_at") or 0) >= LOGIN_MIN:
                log("实例 %s 掉登录(%s)，尝试自动重登账号 %s(%s)" %
                    (uid, mode, account_key(up["label"]), mask_email(cred["email"])))
                ok, msg = password_login(cdp, cred["email"], cred["password"])
                entry["last_login_at"] = now
                actions.setdefault("login", {})[uid] = now
                if ok:
                    result["state"] = "ok"
                    result["error"] = None
                    result["relogin"] = True
                    # 登录成功：冷却使命完成，清除强制下线标记
                    entry["forced_logout_at"] = None
                    entry["next_login_at"] = 0
                else:
                    result["state"] = "login_failed"
                    result["error"] = (result["error"] or "") + "；自动重登失败(%s)" % (msg or "?")[:200]
                    if forced_retry:
                        # 冷却到期重登仍失败：说明环境仍被风控标记，再排一轮 12-24h 随机冷却
                        entry["next_login_at"] = now + random.uniform(COOL_MIN_SEC, COOL_MAX_SEC)
                        entry["login_fail_count"] = int(entry.get("login_fail_count") or 0) + 1
                        result["state"] = "cooling"
                        result["relogin_mode"] = "forced_cooldown"
                        result["next_login_at"] = int(entry["next_login_at"])
                        result["error"] += ("；%.1f 小时后再试" %
                                            ((entry["next_login_at"] - now) / 3600))
    entry["prev_page"] = result["state"] if result["state"] != "deferred" else entry.get("prev_page") or ""
    return result


def write_state(states, pool):
    blob = {
        "updated_at": int(time.time()),
        "interval": INTERVAL,
        "liveness": {
            "enabled": LIVENESS_ENABLED,
            "every": LIVENESS_EVERY,
            "timeout": LIVENESS_TIMEOUT,
            "bad_max": LIVENESS_BAD_MAX,
        },
        "instances": {},
    }
    for inst, up in enumerate(pool["upstreams"], start=1):
        st = states.get(up["id"], {"state": "unknown", "checked_at": None, "error": "尚未巡检"})
        # P0: 把探活结论落到顶层字段，便于 pool / 面板读取
        lb = _LIVENESS_BAD.get(up["id"], 0)
        st = dict(st)
        st["liveness_bad"] = lb
        st["unhealthy"] = lb >= LIVENESS_BAD_MAX
        # 禁言/停机实例：不重启、不探活，直接视为 unhealthy，让 pool 摘出轮询直到解封自动恢复
        if st.get("state") in ("banned", "parked"):
            st["unhealthy"] = True
        if st["unhealthy"]:
            st["liveness_detail"] = _LIVENESS_DETAIL.get(up["id"]) or "探活连续失败"
        blob["instances"][up["id"]] = st
    try:
        os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(blob, f, ensure_ascii=False, sort_keys=True)
        os.replace(tmp, STATE_FILE)
        try:
            subprocess.run(["chown", "dspool:dspool", STATE_FILE], capture_output=True)
        except Exception:
            pass
    except Exception as exc:
        log("!! 状态写盘失败: %r" % exc)


_ROUND = {"n": 0}


def run_once(only_inst=None):
    pool = load_pool_env()
    accs = load_credentials()
    actions = {"restart": {}, "login": {}}
    cooldown = load_cooldown()
    states = {}
    _ROUND["n"] += 1
    # 每 LIVENESS_EVERY 轮做一次端到端探活（避免每轮都占用网页输入框）
    do_live = LIVENESS_ENABLED and (
        _FORCE_LIVE["on"] or _ROUND["n"] % max(1, LIVENESS_EVERY) == 0)
    for up in pool["upstreams"]:
        inst = int(re.sub(r"\D", "", up["id"]) or "1")
        if only_inst and str(inst) != str(only_inst):
            continue
        uid = up["id"]
        entry = cooldown.setdefault(uid, {})
        trow = time.time()

        # ---- 封号停机状态机（banned -> 停机 -> 到点开机复查）----
        parked = entry.get("parked") or {}
        if parked:
            wake_at = float(parked.get("wake_at") or 0)
            chrome_svc = "chrome-webapi%s.service" % ("" if inst == 1 else inst)
            if trow < wake_at and not service_active(chrome_svc):
                st = {"state": "parked", "checked_at": int(trow),
                      "account": account_key(up["label"]), "label": up["label"],
                      "profile": None,
                      "relogin_mode": "parked", "wake_at": int(wake_at),
                      "error": "封号停机中，计划 %s 自动开机恢复%s" %
                               (fmt_wake(wake_at), "（解封时间未知，随机复查）" if parked.get("fallback") else "")}
                states[uid] = st
                log("实例 %-4s 停机中，计划 %s 自动开机" % (uid, fmt_wake(wake_at)))
                continue
            if trow < wake_at:
                log("实例 %s 停机期间检测到 Chrome 被手动拉起，按已开机处理" % uid)
                uwa_svc = "uwa-webapi%s.service" % ("" if inst == 1 else inst)
                if not service_active(uwa_svc):
                    log("实例 %s 补启 %s" % (uid, uwa_svc))
                    try:
                        subprocess.run(["systemctl", "start", uwa_svc],
                                       capture_output=True, timeout=40)
                    except Exception as exc:
                        log("!! 启动 %s 失败: %r（Chrome 已起，继续复查）" % (uwa_svc, exc))
            else:
                log("实例 %s 停机到期，自动开机（计划 %s）" % (uid, fmt_wake(wake_at)))
                bok, bmsg = boot_instance(inst, browser_cdp_port(up, inst))
                if not bok:
                    parked["boot_fail"] = int(parked.get("boot_fail") or 0) + 1
                    parked["wake_at"] = trow + 300
                    # 开机失败：把刚拉起的 Chrome 再停掉，恢复彻底停机，等 5 分钟后重试
                    stop_instance(inst)
                    st = {"state": "parked", "checked_at": int(trow),
                          "account": account_key(up["label"]), "label": up["label"],
                          "profile": None, "relogin_mode": "parked",
                          "wake_at": int(parked["wake_at"]),
                          "error": "自动开机失败(%s)，5 分钟后重试（第 %d 次）" %
                                   (bmsg, parked["boot_fail"])}
                    states[uid] = st
                    log("!! 实例 %s 自动开机失败: %s（第 %d 次，5 分钟后重试）" %
                        (uid, bmsg, parked["boot_fail"]))
                    continue
            entry.pop("parked", None)

        st = check_instance(inst, up, pool, accs, actions, cooldown)

        # ---- banned：排封号停机计划（首次或解封时间变化时触发）----
        if st.get("state") == "banned":
            had_parked = bool(entry.get("parked"))
            wake_at, fallback = park_banned(entry, st.get("banned_until"), time.time())
            if not had_parked:
                log("实例 %-4s 执行封号停机（stop chrome+uwa）" % uid)
                stop_instance(inst)
            st["relogin_mode"] = "parked"
            st["wake_at"] = int(wake_at)
            st["error"] = (st.get("error") or "") + "；已自动停机，计划 %s 自动开机恢复" % fmt_wake(wake_at)
            log("实例 %-4s 封号停机：unban=%s -> wake=%s%s" %
                (uid, st.get("banned_until") or "未知", fmt_wake(wake_at),
                 "（解封时间未知，随机复查）" if fallback else ""))

        # ---- P0: 端到端探活 ----
        # 只在「登录态正常」且无在途请求时才探，避免干扰真实业务
        if do_live and st.get("state") == "ok":
            inflight = pool_inflight(uid, pool)
            if inflight > 0:
                log("实例 %-4s 探活跳过（在途 %d）" % (uid, inflight))
            else:
                lok, ldetail = uwa_liveness(inst, up, pool)
                _LIVENESS_DETAIL[uid] = ldetail
                if lok:
                    if _LIVENESS_BAD.get(uid):
                        log("实例 %-4s 探活恢复 ✓ (%s)" % (uid, ldetail))
                    _LIVENESS_BAD[uid] = 0
                    st["liveness"] = ldetail
                else:
                    _LIVENESS_BAD[uid] = _LIVENESS_BAD.get(uid, 0) + 1
                    log("实例 %-4s 探活失败 %d/%d: %s" %
                        (uid, _LIVENESS_BAD[uid], LIVENESS_BAD_MAX, ldetail))
                    if _LIVENESS_BAD[uid] >= LIVENESS_BAD_MAX:
                        st["state"] = "unhealthy"
                        st["error"] = "端到端探活连续失败: " + ldetail
                        log("实例 %-4s 标记 unhealthy，建议重启三件套" % uid)
                        now2 = time.time()
                        if now2 - float(entry.get("last_restart_at") or 0) >= RESTART_MIN:
                            log("实例 %-4s unhealthy -> 自动重启 chrome+uwa" % uid)
                            restart_chrome(inst)
                            entry["last_restart_at"] = now2
                            try:
                                import subprocess as _sp
                                _sp.run(["systemctl", "restart", "uwa-webapi%s" % ("" if inst == 1 else inst)],
                                        capture_output=True)
                            except Exception:
                                pass
                            time.sleep(15)
                            ok2, d2 = uwa_liveness(inst, up, pool)
                            if ok2:
                                log("实例 %-4s 重启后探活恢复 ✓ (%s)" % (uid, d2))
                                _LIVENESS_BAD[uid] = 0
                                st["state"] = "ok"
                                st["error"] = None
                            else:
                                log("实例 %-4s 重启后仍探活失败: %s" % (uid, d2))

        states[uid] = st
        log("实例 %-4s state=%-12s acct=%-8s %s" %
            (uid, st.get("state"), st.get("account") or "-",
             (st.get("error") or (st.get("url") or ""))[:90]))
    save_cooldown(cooldown)
    write_state(states, pool)
    return states


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--now":
        # 单次模式：每次都做端到端探活（--now 常用于手动巡检/切号后校验）
        if "--no-live" not in sys.argv:
            _FORCE_LIVE["on"] = True
        run_once(sys.argv[2] if len(sys.argv) > 2 else None)
        return
    log("ds-pool 登录健康探针启动（间隔 %ds）" % INTERVAL)
    while True:
        try:
            run_once()
        except Exception as exc:
            log("!! 巡检异常: %r" % exc)
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
