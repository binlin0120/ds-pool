# -*- coding: utf-8 -*-
"""运行时配置：全部可用环境变量覆盖，零第三方依赖。"""
from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def _env_int(name: str, default: int) -> int:
    raw = _env(name)
    try:
        return int(raw) if raw else default
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = _env(name)
    try:
        return float(raw) if raw else default
    except ValueError:
        return default


def _env_list(name: str) -> list:
    return [x.strip() for x in _env(name).split(",") if x.strip()]


@dataclass
class CredentialConfig:
    key: str
    label: str = ""
    base_url: str = ""          # 单 key 覆盖全局 base_url
    rpm: int = 0                # 该 key 的每分钟请求上限，0 = 不限
    weight: int = 1


@dataclass
class Config:
    host: str = "127.0.0.1"
    port: int = 8790
    base_url: str = "https://api.deepseek.com"
    api_path: str = "/chat/completions"
    credentials: list = field(default_factory=list)
    client_keys: list = field(default_factory=list)   # 入站鉴权 key
    admin_token: str = ""
    upstream_timeout_connect: float = 10.0
    upstream_timeout_read: float = 300.0
    max_attempts: int = 3
    max_inflight: int = 16
    # 入站限流（治 nginx limit_req_status 429 那种自伤）：宽松 + JSON 429
    inbound_rpm: int = 120
    inbound_burst: int = 40
    proxy_url: str = ""          # http://host:port 走 CONNECT；socks5 不支持（见 README）
    user_agent: str = "deepseek-relay/1.0 (+https://api.deepseek.com)"
    window_seconds: int = 120    # 失败滑窗
    trip_threshold: int = 3      # 滑窗内几次真实失败才进冷却
    cooldown_base: float = 60.0
    cooldown_max: float = 600.0
    isolation_seconds: float = 1800.0
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "Config":
        cfg = cls(
            host=_env("RELAY_HOST", "127.0.0.1"),
            port=_env_int("RELAY_PORT", 8790),
            base_url=_env("RELAY_BASE_URL", "https://api.deepseek.com").rstrip("/"),
            api_path=_env("RELAY_API_PATH", "/chat/completions"),
            client_keys=_env_list("RELAY_CLIENT_KEYS"),
            admin_token=_env("RELAY_ADMIN_TOKEN"),
            upstream_timeout_connect=_env_float("RELAY_CONNECT_TIMEOUT", 10.0),
            upstream_timeout_read=_env_float("RELAY_READ_TIMEOUT", 300.0),
            max_attempts=_env_int("RELAY_MAX_ATTEMPTS", 3),
            max_inflight=_env_int("RELAY_MAX_INFLIGHT", 16),
            inbound_rpm=_env_int("RELAY_INBOUND_RPM", 120),
            inbound_burst=_env_int("RELAY_INBOUND_BURST", 40),
            proxy_url=_env("RELAY_PROXY_URL"),
            user_agent=_env("RELAY_USER_AGENT", "deepseek-relay/1.0"),
            window_seconds=_env_int("RELAY_WINDOW_SECONDS", 120),
            trip_threshold=max(1, _env_int("RELAY_TRIP_THRESHOLD", 3)),
            cooldown_base=_env_float("RELAY_COOLDOWN_BASE", 60.0),
            cooldown_max=_env_float("RELAY_COOLDOWN_MAX", 600.0),
            isolation_seconds=_env_float("RELAY_ISOLATION_SECONDS", 1800.0),
            log_level=_env("RELAY_LOG_LEVEL", "INFO"),
        )
        creds = []
        # RELAY_KEYS="sk-aaa,sk-bbb"；可选 RELAY_KEY_RPM / RELAY_KEY_BASE_URL 逗号对齐
        keys = _env_list("RELAY_KEYS")
        rpms = _env_list("RELAY_KEY_RPM")
        bases = _env_list("RELAY_KEY_BASE_URL")
        for i, k in enumerate(keys):
            creds.append(CredentialConfig(
                key=k,
                label=k[:6] + "***" + k[-3:] if len(k) > 12 else k,
                base_url=bases[i] if i < len(bases) else "",
                rpm=int(rpms[i]) if i < len(rpms) and rpms[i].isdigit() else 0,
            ))
        cfg.credentials = creds or [CredentialConfig(key="", label="none")]
        return cfg
