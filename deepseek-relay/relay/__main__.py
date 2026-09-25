# -*- coding: utf-8 -*-
import argparse
import sys

from .config import Config
from .server import serve


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="deepseek-relay",
                                 description="OpenAI 兼容反代（key 池 + 429 分诊），纯标准库")
    ap.add_argument("--host", default=None)
    ap.add_argument("--port", type=int, default=None)
    ap.add_argument("--base-url", default=None)
    ap.add_argument("--keys", default=None, help="逗号分隔的上游 key")
    ap.add_argument("--client-keys", default=None, help="逗号分隔的入站鉴权 key")
    ap.add_argument("--proxy", default=None, help="http://host:port 出口代理")
    ap.add_argument("--print-config", action="store_true")
    args = ap.parse_args(argv)

    cfg = Config.from_env()
    if args.host:
        cfg.host = args.host
    if args.port:
        cfg.port = args.port
    if args.base_url:
        cfg.base_url = args.base_url.rstrip("/")
    if args.keys:
        from .config import CredentialConfig
        cfg.credentials = [CredentialConfig(key=k.strip(),
                                            label=k.strip()[:6] + "***")
                           for k in args.keys.split(",") if k.strip()]
    if args.client_keys:
        cfg.client_keys = [x.strip() for x in args.client_keys.split(",") if x.strip()]
    if args.proxy:
        cfg.proxy_url = args.proxy

    if args.print_config:
        print("host=%s port=%d base_url=%s api_path=%s" %
              (cfg.host, cfg.port, cfg.base_url, cfg.api_path))
        print("credentials=%d client_keys=%d proxy=%s max_attempts=%d" %
              (len(cfg.credentials), len(cfg.client_keys), cfg.proxy_url or "-",
               cfg.max_attempts))
        print("inbound: rpm=%d burst=%d | upstream timeouts: %s/%s" %
              (cfg.inbound_rpm, cfg.inbound_burst, cfg.upstream_timeout_connect,
               cfg.upstream_timeout_read))
        print("pool: window=%ds trip=%d cooldown=%ss->%ss isolation=%ss" %
              (cfg.window_seconds, cfg.trip_threshold, cfg.cooldown_base,
               cfg.cooldown_max, cfg.isolation_seconds))
        print("credentials: " + ", ".join(c.label or "(blank)" for c in cfg.credentials))
        return 0
    if not cfg.credentials or all(not c.key for c in cfg.credentials):
        print("错误：没有上游 key。设置 RELAY_KEYS=sk-xx,sk-yy 或 --keys", file=sys.stderr)
        return 2
    if not cfg.client_keys:
        print("警告：未配置 RELAY_CLIENT_KEYS，任何人都能白用你的 key 池；"
              "确认只监听 127.0.0.1 再这样跑", file=sys.stderr)
    serve(cfg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
