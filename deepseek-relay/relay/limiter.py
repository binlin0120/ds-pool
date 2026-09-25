# -*- coding: utf-8 -*-
"""入站限流：令牌桶 + 并发闸门。

为什么还要自己做：上一版把限流全交给 nginx 的 limit_req(2r/s, burst=8,
limit_req_status 429)，结果健康检查、面板轮询、正常客户端重试全被打成 429，
而且 nginx 的 429 是 HTML body，OpenAI 客户端解析不出来，表现就是
"一直 429、拿不到上游"。这里限流放宽、只针对真实 IP、且 429 一定是 JSON。
"""
from __future__ import annotations

import threading
import time
from collections import deque
from typing import Deque, Dict, Optional, Tuple


class TokenBucket:
    __slots__ = ("rate", "capacity", "tokens", "updated")

    def __init__(self, rate_per_sec: float, capacity: float):
        self.rate = max(0.0001, rate_per_sec)
        self.capacity = max(1.0, capacity)
        self.tokens = self.capacity
        self.updated = time.monotonic()

    def take(self, cost: float = 1.0) -> Tuple[bool, float]:
        now = time.monotonic()
        self.tokens = min(self.capacity, self.tokens + (now - self.updated) * self.rate)
        self.updated = now
        if self.tokens >= cost:
            self.tokens -= cost
            return True, 0.0
        return False, (cost - self.tokens) / self.rate


class InboundLimiter:
    """按真实客户端 IP 分桶，带 GC，避免 nginx 那种 $binary_remote_addr 全量共享。"""

    def __init__(self, rpm: int = 120, burst: int = 40, max_keys: int = 4096,
                 idle_ttl: float = 600.0):
        self.rpm = max(1, rpm)
        self.burst = max(1, burst)
        self.max_keys = max(64, max_keys)
        self.idle_ttl = idle_ttl
        self._buckets: Dict[str, TokenBucket] = {}
        self._seen: Dict[str, float] = {}
        self._lock = threading.Lock()

    def allow(self, key: str) -> Tuple[bool, float]:
        rate = self.rpm / 60.0
        with self._lock:
            b = self._buckets.get(key)
            if b is None:
                self._gc_locked()
                b = self._buckets[key] = TokenBucket(rate, self.burst)
            self._seen[key] = time.time()
            ok, wait = b.take(1.0)
            return ok, wait

    def _gc_locked(self) -> None:
        now = time.time()
        if len(self._buckets) < self.max_keys:
            for k, seen in list(self._seen.items()):
                if now - seen > self.idle_ttl:
                    self._buckets.pop(k, None)
                    self._seen.pop(k, None)
            return
        cutoff = now - self.idle_ttl
        for k, seen in list(self._seen.items()):
            if seen < cutoff:
                self._buckets.pop(k, None)
                self._seen.pop(k, None)

    def stats(self) -> Dict[str, object]:
        with self._lock:
            return {"rpm": self.rpm, "burst": self.burst, "tracked_ips": len(self._buckets)}


class Gate:
    """并发闸门：超阈值直接 503，不做无界排队。"""

    def __init__(self, max_inflight: int = 16):
        self._sem = threading.Semaphore(max(1, max_inflight))
        self.max_inflight = max(1, max_inflight)
        self.current = 0
        self.rejected = 0
        self._lock = threading.Lock()

    def __enter__(self):
        if not self._sem.acquire(timeout=0.001):
            with self._lock:
                self.rejected += 1
            raise MemoryError("inflight_full")  # 用统一异常，调用方捕获
        with self._lock:
            self.current += 1
        return self

    def __exit__(self, *exc):
        with self._lock:
            self.current -= 1
        self._sem.release()
        return False

    def stats(self) -> Dict[str, object]:
        with self._lock:
            return {"max": self.max_inflight, "current": self.current,
                    "rejected": self.rejected}
