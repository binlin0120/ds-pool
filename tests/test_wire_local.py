"""本地单测：ds-pool 附件透传（split_content / content_to_wire / _tool_output_content / _chars）。

用 AST 从镜像文件里挑出目标函数单独 exec，避免把整个 pool.py 的启动副作用拽进来。
"""
import ast
import io
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
POOL = os.path.join(ROOT, "pool", "pool.py")      # 仓库里的权威副本（与服务器一致）
DSESS = os.path.join(ROOT, "pool", "dsess.py")

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")


def load_names(path, names, pre_exec=""):
    """只把 path 里 imports + 指定顶层节点 + pre_exec 建出一个命名空间。"""
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
    ns = {"__name__": "wire_unit"}
    exec(compile(pre_exec, path + ":pre", "exec"), ns)
    exec(compile(mod, path, "exec"), ns)
    missing = [n for n in names if n not in ns]
    if missing:
        raise RuntimeError("未能从 %s 载入 %s" % (path, missing))
    return ns


pool = load_names(POOL, ("_media_ref", "split_content", "content_to_wire", "PART_TYPES",
                         "_looks_like_part", "_tool_output_content"))
split_content = pool["split_content"]
content_to_wire = pool["content_to_wire"]
tool_output = pool["_tool_output_content"]

dsess = load_names(DSESS, ("_num", "IMAGE_NOMINAL_CHARS", "_chars"))
dsess_chars = dsess["_chars"]

PASS = []
FAIL = []


def check(name, got, want):
    if got == want:
        PASS.append(name)
    else:
        FAIL.append("%s\n    got  = %s\n    want = %s" % (name, got, want))


PNG = "data:image/png;base64,iVBORw0KGgo="
JPEG = "data:image/jpeg;base64,/9j/4AAQ"

# 1) 无附件必须保持字符串 content（旧行为零改动）
check("t01_text_only_str", split_content("你好"), ("你好", []))
check("t02_none", split_content(None), ("", []))
check("t03_wire_no_parts", content_to_wire("hi", []), "hi")
check("t04_wire_empty", content_to_wire("", []), "")

# 2) chat 风格 image_url（dict 形式）
t, p = split_content([{"type": "text", "text": "看图"},
                      {"type": "image_url", "image_url": {"url": PNG, "detail": "high"}}])
check("t05_chat_image", (t, p), ("看图", [{"type": "image_url",
                                          "image_url": {"url": PNG, "detail": "high"}}]))

# 3) chat 风格 image_url（str 形式，agent 常这么发）
t, p = split_content([{"type": "text", "text": "x"}, {"type": "image_url", "image_url": PNG}])
check("t06_image_url_str", p, [{"type": "image_url", "image_url": {"url": PNG}}])

# 4) Responses 风格 input_image（字段直接是字符串）
t, p = split_content([{"type": "input_text", "text": "A"},
                      {"type": "input_image", "image_url": JPEG}])
check("t07_input_image", (t, p), ("A", [{"type": "image_url", "image_url": {"url": JPEG}}]))

# 5) Anthropic source/base64 写法
t, p = split_content([{"type": "image", "source": {"type": "base64",
                                                   "media_type": "image/webp",
                                                   "data": "TUVG"}}])
check("t08_anthropic_source", p,
      [{"type": "image_url", "image_url": {"url": "data:image/webp;base64,TUVG"}}])

# 6) file part：http URL 走 file_url，保留 filename
t, p = split_content([{"type": "text", "text": "读这个"},
                      {"type": "file", "file": {"file_url": "https://e.com/a.pdf",
                                                "filename": "a.pdf"}}])
check("t09_file_url", p, [{"type": "file",
                           "file": {"file_url": "https://e.com/a.pdf", "filename": "a.pdf"}}])

# 7) data URI 且无 filename -> 补 attachment.bin（UWA safe_filename 需要名字）
t, p = split_content([{"type": "input_file", "file_data": PNG}])
check("t10_data_file_name", p, [{"type": "file",
                                 "file": {"file_data": PNG, "filename": "attachment.bin"}}])

# 8) 本地路径 / file_id 一律丢弃，只留文本
t, p = split_content([{"type": "text", "text": "本地图"},
                      {"type": "image_url", "image_url": {"url": "file:///C:/x.png"}},
                      {"type": "file", "file": {"file_id": "file-abc"}}])
check("t11_local_path_dropped", (t, p), ("本地图", []))

# 9) 音视频不支持 -> 静默丢弃
t, p = split_content([{"type": "text", "text": "T"},
                      {"type": "input_audio", "input_audio": {"data": "AAA"}},
                      {"type": "video_url", "video_url": {"url": "https://e.com/v.mp4"}}])
check("t12_av_dropped", (t, p), ("T", []))

# 10) 多图顺序保持 + 升级成数组 wire
t, p = split_content([{"type": "image_url", "image_url": {"url": PNG}},
                      {"type": "text", "text": "两张图"},
                      {"type": "image_url", "image_url": {"url": JPEG}}])
wire = content_to_wire(t, p)
check("t13_wire_shape", wire, [{"type": "text", "text": "两张图"},
                               {"type": "image_url", "image_url": {"url": PNG}},
                               {"type": "image_url", "image_url": {"url": JPEG}}])

# 11) 多模态里 text 为空的纯图回合
t, p = split_content([{"type": "image_url", "image_url": {"url": PNG}}])
check("t14_image_only", content_to_wire(t, p),
      [{"type": "image_url", "image_url": {"url": PNG}}])

# 12) _tool_output_content：None / str 保持旧行为
check("t15_tool_none", tool_output(None), ("", []))
check("t16_tool_str", tool_output("结果"), ("结果", []))

# 13) 结构化 JSON 结果仍走 json.dumps（不能误判成内容块）
t, p = tool_output([{"id": 1, "text_snippet": "x"}, {"id": 2}])
check("t17_tool_json", (t, p), ('[{"id": 1, "text_snippet": "x"}, {"id": 2}]', []))
t, p = tool_output({"answer": 42})
check("t18_tool_dict", (t, p), ('{"answer": 42}', []))

# 14) 工具输出里带图：拆出来，不再把 base64 当文本灌进网页
t, p = tool_output([{"type": "text", "text": "截图如下"},
                    {"type": "image_url", "image_url": {"url": PNG}}])
check("t19_tool_image", (t, p), ("截图如下", [{"type": "image_url",
                                              "image_url": {"url": PNG}}]))
check("t20_tool_image_wire", content_to_wire(t, p),
      [{"type": "text", "text": "截图如下"},
       {"type": "image_url", "image_url": {"url": PNG}}])

# 15) 字符串+块混合的 output 也算内容块
t, p = tool_output(["a", {"type": "input_image", "image_url": JPEG}])
check("t21_tool_mixed", (t, p), ("a", [{"type": "image_url", "image_url": {"url": JPEG}}]))

# 16) 空列表 output 回到 json.dumps("[]")，与旧行为一致
check("t22_tool_empty_list", tool_output([]), ("[]", []))

# 17) dsess._chars：字符串照旧，数组 content 要能计入（否则带图回合永远达不到复用阈值）
check("t23_chars_str", dsess_chars([{"role": "user", "content": "12345"}]), 5)
msgs = [{"role": "user", "content": [{"type": "text", "text": "abcd"},
                                      {"type": "image_url", "image_url": {"url": PNG}}]}]
check("t24_chars_list", dsess_chars(msgs), 4 + dsess["IMAGE_NOMINAL_CHARS"])
check("t25_chars_none", dsess_chars([{"role": "user"}, {"role": "tool"}]), 0)
check("t26_chars_str_part", dsess_chars([{"role": "user", "content": ["ab", "cd"]}]), 4)

# 18) 图片 part 的 file 分支也算附件权重
check("t27_chars_file_part",
      dsess_chars([{"role": "user", "content": [{"type": "file",
                                                 "file": {"file_url": "https://e.com/a.pdf"}}]}]),
      dsess["IMAGE_NOMINAL_CHARS"])

print("=" * 62)
print("ds-pool 附件透传单测  PASS=%d FAIL=%d" % (len(PASS), len(FAIL)))
for f in FAIL:
    print("  [FAIL] " + f)
print("=" * 62)
sys.exit(1 if FAIL else 0)
