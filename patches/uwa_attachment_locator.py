# -*- coding: utf-8 -*-
"""给 UWA 的附件上传路径补上 DrissionPage 定位前缀归一化（默认 dry-run）。

用法（在服务器上）: python3 uwa_attachment_locator.py apply
备份 + py_compile 校验，不过则自动回滚；不 apply 只报告差异。
"""
import os
import py_compile
import re
import shutil
import subprocess
import sys

DIRS = [
    ("/opt/uwa/universal-web-api-main", "uwa-webapi"),
    ("/opt/uwa2/universal-web-api", "uwa-webapi2"),
    ("/opt/uwa3/universal-web-api", "uwa-webapi3"),
]

HELPER = '''


def _ds_loc(selector):
    """Normalize a site selector for DrissionPage.

    Site configs store bare CSS (e.g. input[type='file']), but DrissionPage's
    auto locator does not understand bare CSS; it needs an explicit css: prefix.
    Already-prefixed locators pass through untouched, so this is idempotent.
    """
    text = str(selector or "").strip()
    if not text:
        return None
    if text.startswith(("tag:", "@", "xpath:", "css:")) or "@@" in text:
        return text
    return "css:" + text
'''

REPL = [
    ("            return self.tab.ele(selector, timeout=0.6)",
     "            return self.tab.ele(_ds_loc(selector), timeout=0.6)"),
    ("                return list(self.tab.eles(configured, timeout=0.6) or [])",
     "                return list(self.tab.eles(_ds_loc(configured), timeout=0.6) or [])"),
]

APPLY = len(sys.argv) > 1 and sys.argv[1] == "apply"
TS = "attachloc-20260920"


def main():
    changed = 0
    for root, svc in DIRS:
        path = os.path.join(root, "app/core/workflow/attachment_upload.py")
        if not os.path.exists(path):
            print("!! 不存在:", path)
            continue
        src = open(path, encoding="utf-8").read()
        if "_ds_loc" in src:
            print("== %s: 已打过补丁，跳过" % svc)
            continue
        missing = [a for a, _ in REPL if a not in src]
        if missing:
            print("!! %s: 找不到锚点，跳过（源码结构与预期不符）" % svc)
            for m in missing:
                print("   ", m.strip()[:90])
            continue
        new = src
        for a, b in REPL:
            new = new.replace(a, b, 1)
        anchor = "from app.utils.attachments import attachment_config, type_allowed, MIB"
        if anchor in new:
            new = new.replace(anchor, anchor + HELPER, 1)
        else:
            new = HELPER.lstrip("\n") + "\n" + new
        print("== %s: 将修改 %d 处 + 注入 _ds_loc" % (svc, len(REPL)))
        if not APPLY:
            continue
        bak = path + ".bak-" + TS
        shutil.copy2(path, bak)
        open(path, "w", encoding="utf-8").write(new)
        rc = subprocess.run("python3 -m py_compile %s" % path, shell=True).returncode
        if rc != 0:
            shutil.copy2(bak, path)
            print("   !! 语法不过，已回滚:", bak)
            continue
        rc = subprocess.run("systemctl restart %s" % svc, shell=True).returncode
        print("   ok 备份=%s 重启 rc=%d" % (os.path.basename(bak), rc))
        changed += 1

    if not APPLY:
        print("\n(dry-run，未写入。执行请加 apply)")
        return
    print("\n等待服务就绪…")
    subprocess.run("sleep 12", shell=True)
    for root, svc in DIRS:
        st = subprocess.run("systemctl is-active %s" % svc, shell=True,
                            capture_output=True).stdout.decode().strip()
        print("  %-14s %s" % (svc, st))
    import http.client
    # 上游令牌从 pool.env 的 UPSTREAM_n=id|url|model|account|token 里取，脚本本身不含密钥
    probe = []
    try:
        for line in open("/opt/ds-pool/pool.env", encoding="utf-8"):
            m = re.match(r"^UPSTREAM_\d+=([^|\s]+)\|[^|\s]*:(\d+)\|[^|]*\|[^|]*\|(\S+)", line.strip())
            if m:
                probe.append((m.group(1), int(m.group(2)), m.group(3)))
    except Exception as e:
        print("  读取 upstream 失败:", str(e)[:80])
    for uid, port, tok in probe:
        try:
            c = http.client.HTTPConnection("127.0.0.1", port, timeout=15)
            c.request("GET", "/health", headers={"Authorization": "Bearer " + tok})
            r = c.getresponse()
            body = r.read().decode("utf-8", "replace")[:80]
            c.close()
            print("  %-4s port %d /health -> %s %s" % (uid, port, r.status, body))
        except Exception as e:
            print("  %-4s port %d 检查失败: %s" % (uid, port, str(e)[:80]))
    print("改动实例数:", changed)


if __name__ == "__main__":
    main()
