# -*- coding: utf-8 -*-
"""dsess 语义自测（本地跑，不碰服务器）。覆盖复用/换实例/改历史/失败作废等路径。"""
import importlib.util
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_spec = importlib.util.spec_from_file_location("dsess",
                                              os.path.join(ROOT, "pool", "dsess.py"))
dsess = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(dsess)

LONG = "内容" * 400          # 800 字，稳过 MIN_REUSE_CHARS
FAILS = []


def check(name, cond, extra=""):
    print("%-42s %s %s" % (name, "OK " if cond else "FAIL", extra))
    if not cond:
        FAILS.append(name)


def reset():
    with dsess._LOCK:
        dsess._SESS.clear()
        dsess._BOUND.clear()
        dsess._METRICS.update({"reuse": 0, "full": 0, "moved": 0, "dropped": 0})


def m(role, text):
    return {"role": role, "content": text}


# 1) 首轮：无指纹 -> 全量 + 默认预设
reset()
msgs = [m("system", "系统提示" + LONG), m("user", "第一问" + LONG)]
h = dsess.plan(msgs, "chat.deepseek.com")
check("首轮 plan k=0", h["k"] == 0, h)
sm, preset, chars, mode = dsess.frame(h, "ds1", msgs)
check("首轮 全量+默认预设", sm is msgs and preset is None and mode == "full", mode)
dsess.commit(h, "ds1", msgs)
check("commit 后 ds1 被占用", dsess.bound_ids() == {"ds1"}, dsess.bound_ids())

# 2) 同一会话追加一轮 -> 只发尾巴 + 续用预设
msgs2 = msgs + [m("assistant", "答一" + LONG), m("user", "第二问" + LONG)]
h2 = dsess.plan(msgs2, "chat.deepseek.com")
check("次轮 plan k=2", h2["k"] == 2, h2["k"])
check("次轮 prefer 粘到 ds1", h2["prefer"] == "ds1", h2["prefer"])
sm2, preset2, chars2, mode2 = dsess.frame(h2, "ds1", msgs2)
check("次轮 只发尾回合", [x["content"][:2] for x in sm2] == ["答一", "第二"],
      [x["content"][:2] for x in sm2])
check("次轮 用续用预设", preset2 == dsess.CONT_PRESET and mode2 == "reuse", mode2)
check("次轮 实发字符远小于全量", chars2 < dsess._chars(msgs2) / 2, "%d/%d" % (chars2, dsess._chars(msgs2)))
dsess.commit(h2, "ds1", msgs2)

# 3) 客户端重复发同一份历史（无新增）-> 必须退回全量，不能发空
h3 = dsess.plan(msgs2, "chat.deepseek.com")
sm3, preset3, _, mode3 = dsess.frame(h3, "ds1", msgs2)
check("重复请求退回全量", sm3 is msgs2 and preset3 is None and mode3 == "full", mode3)

# 4) 粘滞实例被别的会话占了 -> 全量 + 新建
other_msgs = [m("system", "别的会话" + LONG), m("user", "插一刀" + LONG)]
other = dsess.plan(other_msgs, "chat.deepseek.com")
dsess.commit(other, "ds1", other_msgs)
msgsC = msgs2 + [m("user", "第三问" + LONG)]
h4 = dsess.plan(msgsC, "chat.deepseek.com")
sm4, preset4, _, mode4 = dsess.frame(h4, "ds1", msgsC)
check("实例易主后退回全量", preset4 is None and sm4 is msgsC, mode4)

# 4b) 会话还活着但被调度到别的实例 -> 也必须全量，并计入 moved
reset()
mm = [m("user", "起始" + LONG)]
dsess.commit(dsess.plan(mm, "chat.deepseek.com"), "ds5", mm)
mm2 = mm + [m("user", "追问" + LONG)]
h4b = dsess.plan(mm2, "chat.deepseek.com")
check("会话仍在原实例名下 k=1", h4b["k"] == 1 and h4b["prefer"] == "ds5", h4b["k"])
sm4b, preset4b, _, mode4b = dsess.frame(h4b, "ds6", mm2)
check("换实例退回全量", preset4b is None and sm4b is mm2, mode4b)
check("换实例计入 moved", dsess._METRICS["moved"] >= 1, dsess._METRICS)

# 5) 改历史（前缀对不上）-> 新会话全量
reset()
dsess.commit(dsess.plan(msgs, "chat.deepseek.com"), "ds2", msgs)
edited = [m("system", "系统提示被改了" + LONG), m("user", "第一问" + LONG)]
h5 = dsess.plan(edited, "chat.deepseek.com")
check("改历史不复用", h5["k"] == 0, h5["k"])

# 6) 失败作废 -> 下一次一定全量
reset()
msgsA = [m("user", "起始" + LONG)]
hA = dsess.plan(msgsA, "chat.deepseek.com")
dsess.commit(hA, "ds3", msgsA)
msgsB = msgsA + [m("assistant", "回" + LONG), m("user", "追问" + LONG)]
hB = dsess.plan(msgsB, "chat.deepseek.com")
check("失败前可复用", dsess.frame(hB, "ds3", msgsB)[3] == "reuse", hB["k"])
dsess.fault(hB, "ds3")
hC = dsess.plan(msgsB, "chat.deepseek.com")
check("失败后作废", hC["k"] == 0, hC["k"])

# 7) 模型不同不串会话
reset()
dsess.commit(dsess.plan(msgs, "a-model"), "ds1", msgs)
hD = dsess.plan(msgs + [m("user", "新问" + LONG)], "b-model")
check("换模型不复用", hD["k"] == 0, hD["k"])

# 8) 工具调用 id 抖动不影响指纹
reset()
withid = [{"role": "assistant", "content": "", "tool_calls": [
    {"id": "call_1", "type": "function",
     "function": {"name": "shell", "arguments": "{\"cmd\":\"ls\"}"}}]},
    {"role": "tool", "content": "输出" + LONG, "tool_call_id": "call_1"}]
hE = dsess.plan(withid, "chat.deepseek.com")
dsess.commit(hE, "ds1", withid)
shuffled = [{"role": "assistant", "content": "", "tool_calls": [
    {"id": "call_ZZZ", "type": "function",
     "function": {"name": "shell", "arguments": "{\"cmd\":\"ls\"}"}}]},
    {"role": "tool", "content": "输出" + LONG, "tool_call_id": "call_ZZZ"},
    {"role": "user", "content": "继续" + LONG}]
hF = dsess.plan(shuffled, "chat.deepseek.com")
check("tool_call id 抖动仍复用", hF["k"] == 2, hF["k"])

print("\n指标:", dsess.snapshot()["metrics"])
print("结果:", "全部通过" if not FAILS else "失败项 %s" % FAILS)
sys.exit(1 if FAILS else 0)
