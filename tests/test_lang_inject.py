# -*- coding: utf-8 -*-
"""方案B 单测：POOL_LANG_DIRECTIVE 注入（chat + responses 两条路径）。
用 AST 只挑目标函数，避开 pool.py 的启动副作用。
"""
import ast
import io
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
POOL = os.path.join(ROOT, "pool", "pool.py")

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

DIRECTIVE = "请始终使用简体中文回复。"


def load_names(path, names, pre_exec=""):
    with open(path, "r", encoding="utf-8") as f:
        tree = ast.parse(f.read(), filename=path)
    keep = []
    for node in tree.body:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            keep.append(node)
        elif isinstance(node, ast.FunctionDef) and node.name in names:
            keep.append(node)
        elif isinstance(node, ast.Assign):
            targets = [t.id for t in node.targets if isinstance(t, ast.Name)]
            if any(t in names for t in targets):
                keep.append(node)
    mod = ast.Module(body=keep, type_ignores=[])
    ns = {"__name__": "lang_unit"}
    exec(compile(pre_exec, path + ":pre", "exec"), ns)
    exec(compile(mod, path, "exec"), ns)
    missing = [n for n in names if n not in ns]
    if missing:
        raise RuntimeError("missing %s" % missing)
    return ns


NEEDED = ("_env", "_env_int", "LANG_DIRECTIVE", "maybe_inject_lang", "normalize_payload",
          "responses_payload", "STRIP_KEYS", "MODEL_ALIAS", "UPSTREAM_MODELS",
          "DEFAULT_MODEL", "split_content", "content_to_wire", "map_model")
pool = load_names(POOL, NEEDED, pre_exec="LANG_DIRECTIVE = %r" % DIRECTIVE)
# 模块里 LANG_DIRECTIVE = _env(...) 的赋值节点会被一并载入并覆盖预置值，这里显式设回
pool["LANG_DIRECTIVE"] = DIRECTIVE

PASS = []
FAIL = []


def check(name, cond, extra=""):
    if cond:
        PASS.append(name)
    else:
        FAIL.append("%s | extra=%s" % (name, extra))


def call(fn, body):
    return fn(body, body.get("model", "chat.deepseek.com"))


# 1) chat 路径：指令注入为第一条 system，prompt_chars 计入
payload, minimal, pc = call(pool["normalize_payload"],
                            {"model": "deepseek-v4-pro",
                             "messages": [{"role": "user", "content": "你好"}]})
check("chat 注入首条 system", payload["messages"][0],
      {"role": "system", "content": DIRECTIVE})
check("chat 原消息保留", payload["messages"][1].get("content"), "你好")
check("chat prompt_chars 含指令长度", pc == len("你好") + len(DIRECTIVE), pc)

# 2) chat 路径：客户端已自带同一条指令 -> 不重复
body2 = {"model": "m", "messages": [{"role": "system", "content": DIRECTIVE},
                                    {"role": "user", "content": "hi"}]}
p2, _, pc2 = call(pool["normalize_payload"], body2)
check("chat 幂等不重复", len(p2["messages"]) == 2, p2["messages"])
check("chat 幂等 chars", pc2 == len("hi") + len(DIRECTIVE), pc2)

# 3) chat 路径：客户端自带别的 system -> 指令插到最前，原 system 保留
body3 = {"model": "m", "messages": [{"role": "system", "content": "你是助手"},
                                    {"role": "user", "content": "x"}]}
p3, _, _ = call(pool["normalize_payload"], body3)
check("chat 双 system 顺序", [m["content"] for m in p3["messages"]][:2],
      [DIRECTIVE, "你是助手"])

# 4) responses 路径：responses_payload 同样注入
body4 = {"model": "m", "messages": [{"role": "user", "content": "看图"},
                                    {"role": "user", "content": "见图片"}]}
p4, _, pc4 = call(pool["responses_payload"], body4)
check("responses 注入首条 system", p4["messages"][0],
      {"role": "system", "content": DIRECTIVE})
check("responses prompt_chars 含指令", pc4 == len("看图") + len("见图片") + len(DIRECTIVE), pc4)

# 5) 关闭注入（空指令）-> 零改动
pool["LANG_DIRECTIVE"] = ""
p5, _, pc5 = call(pool["normalize_payload"],
                  {"model": "m", "messages": [{"role": "user", "content": "hi"}]})
check("空指令不注入", p5["messages"] == [{"role": "user", "content": "hi"}], p5["messages"])
check("空指令 chars 不加", pc5 == 2, pc5)

# 6) maybe_inject_lang 直接语义
pool["LANG_DIRECTIVE"] = DIRECTIVE
m, inj = pool["maybe_inject_lang"]([{"role": "user", "content": "a"}])
check("helper 注入标记", inj and m[0]["role"] == "system", (inj, m))
m2, inj2 = pool["maybe_inject_lang"]([{"role": "system", "content": DIRECTIVE},
                                      {"role": "user", "content": "a"}])
check("helper 幂等标记", (not inj2) and len(m2) == 2, (inj2, m2))

print("=" * 62)
print("lang-inject 单测 PASS=%d FAIL=%d" % (len(PASS), len(FAIL)))
print("=" * 62)
if FAIL:
    print("\n".join(FAIL))
    sys.exit(1)
