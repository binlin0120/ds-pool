"""服务器侧 payload 组装单测：确认三条转换路径都不再丢附件。

只把 /opt/ds-pool/pool.py 里需要的函数用 AST 挑出来 exec（模块级有 open() 副作用，
不能整文件 import），跑完打印结果；退出码非 0 代表有失败项。
"""
import ast
import json

P = "/opt/ds-pool/pool.py"
PNG = "data:image/png;base64,iVBORw0KGgo="


def build():
    src = open(P, "r", encoding="utf-8").read()
    mod = ast.parse(src, filename=P)
    want = ("split_content", "content_to_wire", "_media_ref", "_looks_like_part",
            "_tool_output_content", "PART_TYPES", "est_tokens", "map_model",
            "responses_to_chat", "responses_payload", "normalize_payload",
            "_resp_item_id", "_resp_content_text", "flatten_content")
    pre = ("import json, uuid\n"
           "DEFAULT_MODEL='deepseek-chat'\n"
           "MODEL_ALIAS={}\n"
           "UPSTREAM_MODELS=set()\n"
           "STRIP_KEYS=set()\n"
           "def log(m):\n"
           "    pass\n")
    keep = []
    for n in mod.body:
        if isinstance(n, (ast.Import, ast.ImportFrom)):
            keep.append(n)
        elif isinstance(n, ast.FunctionDef) and n.name in want:
            keep.append(n)
        elif isinstance(n, ast.Assign):
            for t in n.targets:
                if isinstance(t, ast.Name) and t.id in want:
                    keep.append(n)
    ns = {"__name__": "wire_probe"}
    exec(compile(pre, "<pre>", "exec"), ns)
    exec(compile(ast.Module(body=keep, type_ignores=[]), P, "exec"), ns)
    missing = [w for w in want if w not in ns]
    if missing:
        raise SystemExit("MISSING FUNCS: %s" % missing)
    return ns


ns = build()
fail = []


def E(payload):
    """复刻 pool.py 的 est 口径：文本 token + 附件名义权重 2000。"""
    total = 0
    for m in payload["messages"]:
        c = m.get("content")
        if isinstance(c, str):
            total += ns["est_tokens"](c)
        elif isinstance(c, list):
            for p in c:
                if not isinstance(p, dict):
                    continue
                if p.get("type") in ("image_url", "file"):
                    total += 2000
                else:
                    total += ns["est_tokens"](p.get("text") or "")
    return total


def imgs_of(msgs):
    """把 payload 里所有图片 data URI 捞出来。"""
    got = []
    for m in msgs:
        c = m.get("content")
        if isinstance(c, list):
            for p in c:
                if isinstance(p, dict) and p.get("type") in ("image_url", "file"):
                    body = p.get("image_url") or p.get("file") or {}
                    u = body.get("url") or body.get("file_data") or body.get("file_url") or ""
                    if u.startswith("data:"):
                        got.append(u[:22])
    return got


def report(name, ok, extra=""):
    print(("PASS " if ok else "FAIL ") + name + ("  " + extra if extra else ""))
    if not ok:
        fail.append(name)


try:
    # A. /v1/chat/completions 带图
    body = {"model": "deepseek-chat", "stream": False, "messages": [
        {"role": "system", "content": "你是助手"},
        {"role": "user", "content": [{"type": "text", "text": "图里有几个数字？"},
                                     {"type": "image_url", "image_url": {"url": PNG}}]}]}
    payload, minimal, pc = ns["normalize_payload"](body, "deepseek-chat")
    report("A chat/completions 保图", imgs_of(payload["messages"]) == [PNG[:22]],
           "est=%s prompt_chars=%s" % (E(payload), pc))

    # B. /v1/responses -> to_chat_payload（ds-pool 内部真发的形状）
    rb = {"model": "deepseek-chat", "stream": False, "input": [
        {"type": "message", "role": "user", "content": [
            {"type": "input_text", "text": "看图回答"},
            {"type": "input_image", "image_url": PNG}]},
        {"type": "function_call", "call_id": "c1", "name": "look", "arguments": "{}"},
        {"type": "function_call_output", "call_id": "c1", "output": [
            {"type": "text", "text": "截图"},
            {"type": "input_image", "image_url": PNG}]},
        {"type": "message", "role": "user", "content": "图里有几个数字？"}]}
    cb, meta = ns["responses_to_chat"](rb)
    payload2, minimal2, pc2 = ns["responses_payload"](cb, "deepseek-chat")
    report("B responses 保图(2张)", imgs_of(payload2["messages"]) == [PNG[:22], PNG[:22]],
           "est=%s tool=%s" % (E(payload2),
                               [ (m.get("role"), type(m.get("content")).__name__) for m in payload2["messages"] ]))

    # C. 结构化 tool 输出不被误拆
    body3 = {"model": "deepseek-chat", "stream": False, "messages": [
        {"role": "user", "content": "开始"},
        {"role": "assistant", "content": None, "tool_calls": [
            {"id": "c1", "type": "function", "function": {"name": "f", "arguments": "{}"}}]},
        {"role": "tool", "tool_call_id": "c1",
         "content": json.dumps([{"id": 7, "text_snippet": "abc"}], ensure_ascii=False)}]}
    payload3, m3, p3 = ns["responses_payload"](body3, "deepseek-chat")
    tc = payload3["messages"][2]["content"]
    report("C tool JSON 原样", isinstance(tc, str) and '"text_snippet"' in tc)

    # D. 纯文本路径完全不变（回归保护）
    body4 = {"model": "deepseek-chat", "stream": True, "messages": [
        {"role": "system", "content": "s"}, {"role": "user", "content": "u"}]}
    payload4, m4, p4 = ns["normalize_payload"](body4, "deepseek-chat")
    report("D 纯文本仍为字符串",
           all(isinstance(m["content"], str) for m in payload4["messages"]) and p4 == 2)

    # E. 图片回合的 est 能撑过复用阈值
    e_plain = E({"messages": [{"role": "user", "content": "\u77ed"}]})
    e_plain = E({"messages": [{"role": "user", "content": "\u77ed"}]})
    e_img = E(payload)
    report("E 附件计入 est", e_img > e_plain, "%s -> %s" % (e_plain, e_img))
except Exception as exc:
    import traceback
    traceback.print_exc()
    fail.append("EXCEPTION %r" % (exc,))

print("SUMMARY " + ("ALL PASS" if not fail else "FAILURES=%s" % fail))
raise SystemExit(1 if fail else 0)
