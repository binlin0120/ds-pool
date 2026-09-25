# -*- coding: utf-8 -*-
"""key/账号池：加权选择 + RPM 预算 + 分级退避 + 熔断，且绝不把最后一个可用项踢出。

设计抄 codex-lb：
- 裸 429（unknown_burst / upstream_capacity / rpm / tpm / network_error）不写冷却，
  只做 5~30s burst backoff；
- 只有真实凭证类失败（auth_invalid / quota / credential_risk）才立刻下线；
- 滑窗 window_seconds 内 penalize 事件 >= trip_threshold 才进 cooldown（60s 起，
  每次翻倍，封顶 cooldown_max），cooldown 连续 3 轮 -> isolation（人工/半自动恢复）；
- 池里只剩 1 个可用时绝不剔除（否则全池空 = 用户看到的"一直 429"）。
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from .config import Config, CredentialConfig
from .triage import Verdict

IMMEDIATE_DOWN = {"auth_invalid", "quota", "credential_risk"}


@dataclass
class CredentialState:
    cfg: CredentialConfig
    failures: List[float] = field(default_factory=list)   # penalize 事件时间
    soft_backoff_until: float = 0.0
    cooldown_until: float = 0.0
    cooldown_rounds: int = 0
    isolated_until: float = 0.0
    last_error: str = ""
    last_kind: str = ""
    wrr_current: int = 0
    requests_ok: int = 0
    requests_failed: int = 0
    _rr: int = 0

    @property
    def label(self) -> str:
        return self.cfg.label or "cred"

    def available(self, now: float) -> bool:
        if self.isolated_until > now:
            return False
        if self.cooldown_until > now:
            return False
        return True

    def usable_now(self, now: float) -> bool:
        return self.available(now) and self.soft_backoff_until <= now

    def snapshot(self, now: Optional[float] = None) -> Dict[str, object]:
        now = now if now is not None else time.time()
        return {
            "label": self.label,
            "base_url": self.cfg.base_url or "-",
            "rpm_limit": self.cfg.rpm,
            "available": self.available(now),
            "usable": self.usable_now(now),
            "in_window_failures": len([t for t in self.failures
                                       if now - t < self.window_ttl]),
            "cooldown_left": round(max(0.0, self.cooldown_until - now), 1),
            "isolated_left": round(max(0.0, self.isolated_until - now), 1),
            "backoff_left": round(max(0.0, self.soft_backoff_until - now), 1),
            "ok": self.requests_ok,
            "failed": self.requests_failed,
            "last_kind": self.last_kind,
            "last_error": self.last_error[:160],
        }

    window_ttl = 120  # 会被 KeyPool 覆盖


class KeyPool:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.lock = threading.RLock()
        self.states: List[CredentialState] = []
        for c in cfg.credentials:
            st = CredentialState(cfg=c)
            st.window_ttl = cfg.window_seconds
            self.states.append(st)
        self._total_weight = sum(max(1, s.cfg.weight) for s in self.states) or 1
        self._tick = 0

    # ---------- 选择 ----------
    def candidates(self, now: Optional[float] = None) -> List[CredentialState]:
        now = now if now is not None else time.time()
        with self.lock:
            usable = [s for s in self.states if s.usable_now(now)]
            if usable:
                return usable
            # 只剩一个可用时绝不剔除：宁可硬试也不能返回"无可用上游"
            avail = [s for s in self.states if s.available(now)]
            if len(self.states) == 1 or len(avail) <= 1:
                return avail or list(self.states)
            return sorted(self.states,
                          key=lambda s: max(s.cooldown_until, s.soft_backoff_until))[:1]

    def pick(self, exclude: Optional[List[str]] = None,
             now: Optional[float] = None) -> Optional[CredentialState]:
        """平滑加权轮询（nginx SWRR），跳过本次已失败的凭证。"""
        exclude = set(exclude or [])
        pool = [s for s in self.candidates(now) if s.label not in exclude]
        if not pool:
            pool = self.candidates(now)
        if not pool:
            return None
        with self.lock:
            total = sum(max(1, s.cfg.weight) for s in pool)
            best = None
            for s in pool:
                s.wrr_current += max(1, s.cfg.weight)
                if best is None or s.wrr_current > best.wrr_current:
                    best = s
            best.wrr_current -= total
            best._rr += 1
            return best

    # ---------- 记账 ----------
    def note_result(self, st: CredentialState, verdict: Verdict,
                    now: Optional[float] = None) -> None:
        now = now if now is not None else time.time()
        with self.lock:
            st.last_kind = verdict.kind
            st.last_error = verdict.reason
            if not verdict.is_error:
                st.requests_ok += 1
                st.failures = []
                st.cooldown_rounds = 0
                return
            st.requests_failed += 1
            if not verdict.penalize_credential:
                # 不是这个 key 的错：只做短时软退避，不写冷却
                if verdict.kind in ("waf_block", "network_error"):
                    st.soft_backoff_until = now + clamp_backoff(
                        verdict.backoff_seconds or 5.0, 5, 30)
                return
            st.failures.append(now)
            st.failures = [t for t in st.failures
                           if now - t < self.cfg.window_seconds]
            if verdict.kind in IMMEDIATE_DOWN:
                self._trip(st, now, force=True)
            elif len(st.failures) >= self.cfg.trip_threshold:
                self._trip(st, now)

    def _trip(self, st: CredentialState, now: float, force: bool = False) -> None:
        st.cooldown_rounds += 1
        if force and st.cooldown_rounds < self.cfg.trip_threshold:
            st.cooldown_rounds = self.cfg.trip_threshold
        if st.cooldown_rounds >= self.cfg.trip_threshold + 3:
            st.isolated_until = now + self.cfg.isolation_seconds
            st.last_error = (st.last_error or "") + " [isolated]"
            st.cooldown_until = 0.0
        else:
            seconds = min(self.cfg.cooldown_max,
                          self.cfg.cooldown_base * (2 ** max(0, st.cooldown_rounds
                                                             - self.cfg.trip_threshold)))
            st.cooldown_until = now + seconds
        st.soft_backoff_until = 0.0

    def release(self, st: CredentialState, now: Optional[float] = None) -> None:
        """人工/探测通过后立即回池。"""
        now = now if now is not None else time.time()
        with self.lock:
            st.cooldown_until = 0.0
            st.isolated_until = 0.0
            st.soft_backoff_until = 0.0
            st.cooldown_rounds = 0
            st.failures = []

    def status(self) -> List[Dict[str, object]]:
        now = time.time()
        with self.lock:
            return [s.snapshot(now) for s in self.states]

    def __len__(self) -> int:
        return len(self.states)


def clamp_backoff(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(value)))
