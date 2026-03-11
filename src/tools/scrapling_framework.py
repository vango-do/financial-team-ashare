from __future__ import annotations

import json
import os
import random
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Any, Callable

import requests


class CircuitOpenError(RuntimeError):
    """Raised when a circuit breaker is open."""


@dataclass(slots=True)
class StabilityConfig:
    timeout_sec: float = 15.0
    max_retries: int = 4
    backoff_base_sec: float = 0.6
    backoff_cap_sec: float = 8.0
    backoff_jitter_ratio: float = 0.25
    rate_limit_qps: float = 2.0
    breaker_fail_threshold: int = 4
    breaker_open_sec: float = 30.0


class CircuitBreaker:
    def __init__(self, fail_threshold: int, open_sec: float):
        self._fail_threshold = max(1, int(fail_threshold))
        self._open_sec = max(1.0, float(open_sec))
        self._failure_count = 0
        self._open_until = 0.0
        self._lock = Lock()

    def assert_available(self) -> None:
        with self._lock:
            now = time.monotonic()
            if now < self._open_until:
                remain = self._open_until - now
                raise CircuitOpenError(f"Circuit is open for another {remain:.1f}s")
            if self._open_until > 0 and now >= self._open_until:
                # half-open: allow a trial and reset open flag
                self._open_until = 0.0

    def on_success(self) -> None:
        with self._lock:
            self._failure_count = 0
            self._open_until = 0.0

    def on_failure(self) -> None:
        with self._lock:
            self._failure_count += 1
            if self._failure_count >= self._fail_threshold:
                self._open_until = time.monotonic() + self._open_sec
                self._failure_count = 0


class RateLimiter:
    def __init__(self, qps: float):
        self._interval_sec = 0.0 if qps <= 0 else 1.0 / qps
        self._next_allowed = 0.0
        self._lock = Lock()

    def wait(self) -> None:
        if self._interval_sec <= 0:
            return
        with self._lock:
            now = time.monotonic()
            if now < self._next_allowed:
                time.sleep(self._next_allowed - now)
                now = time.monotonic()
            self._next_allowed = max(now, self._next_allowed) + self._interval_sec


def _candidate_scrapling_roots() -> list[Path]:
    project_root = Path(__file__).resolve().parents[2]
    env_path = os.getenv("SCRAPLING_CN_LIB") or os.getenv("SCRAPLING_HOME")
    paths = []
    if env_path:
        paths.append(Path(env_path))

    # Project-relative fallbacks for open-source users.
    paths.extend(
        [
            project_root / "scrapling_cn_lib",
            project_root / "vendor" / "scrapling",
            project_root / "third_party" / "scrapling",
        ]
    )
    unique: list[Path] = []
    seen: set[str] = set()
    for p in paths:
        key = str(p).lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(p)
    return unique


def _ensure_scrapling_importable() -> None:
    for root in _candidate_scrapling_roots():
        pkg = root / "scrapling" / "__init__.py"
        if pkg.exists():
            text_root = str(root)
            if text_root not in sys.path:
                sys.path.insert(0, text_root)
            return


def load_scrapling_fetchers() -> dict[str, Any]:
    """
    Lazy-load Scrapling fetchers.
    Returns an empty dict when Scrapling is unavailable.
    """
    try:
        from scrapling.fetchers import DynamicFetcher, Fetcher, StealthyFetcher

        return {
            "dynamic": DynamicFetcher,
            "stealthy": StealthyFetcher,
            "requests": Fetcher,
        }
    except Exception:
        _ensure_scrapling_importable()
        try:
            from scrapling.fetchers import DynamicFetcher, Fetcher, StealthyFetcher

            return {
                "dynamic": DynamicFetcher,
                "stealthy": StealthyFetcher,
                "requests": Fetcher,
            }
        except Exception:
            return {}


def response_to_html(resp: Any) -> str:
    if resp is None:
        return ""
    if isinstance(resp, str):
        return resp
    body = getattr(resp, "body", None)
    if isinstance(body, (bytes, bytearray)):
        raw = bytes(body)
        for enc in ("utf-8", "gb18030", "gbk"):
            try:
                text = raw.decode(enc)
                if "<html" in text.lower():
                    return text
            except Exception:
                continue
        return raw.decode("utf-8", errors="ignore")
    text = getattr(resp, "text", None)
    if isinstance(text, str):
        return text
    return str(resp)


class StableFetcher:
    """
    Unified stability layer for requests/SDK calls:
    - retry + exponential backoff + jitter
    - per-provider rate limit
    - per-provider circuit breaker
    - unified timeout
    """

    def __init__(self, config: StabilityConfig | None = None):
        self.config = config or StabilityConfig()
        self._session = requests.Session()
        self._breakers: dict[str, CircuitBreaker] = {}
        self._limiters: dict[str, RateLimiter] = {}
        self._dict_lock = Lock()

    def _breaker(self, provider: str) -> CircuitBreaker:
        with self._dict_lock:
            if provider not in self._breakers:
                self._breakers[provider] = CircuitBreaker(
                    fail_threshold=self.config.breaker_fail_threshold,
                    open_sec=self.config.breaker_open_sec,
                )
            return self._breakers[provider]

    def _limiter(self, provider: str) -> RateLimiter:
        with self._dict_lock:
            if provider not in self._limiters:
                self._limiters[provider] = RateLimiter(self.config.rate_limit_qps)
            return self._limiters[provider]

    def execute(
        self,
        provider: str,
        fn: Callable[[], Any],
        max_retries: int | None = None,
    ) -> Any:
        retries = self.config.max_retries if max_retries is None else max(1, int(max_retries))
        breaker = self._breaker(provider)
        limiter = self._limiter(provider)
        last_error: Exception | None = None

        for attempt in range(retries):
            breaker.assert_available()
            limiter.wait()
            try:
                value = fn()
                breaker.on_success()
                return value
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                breaker.on_failure()
                if attempt >= retries - 1:
                    break
                base = min(self.config.backoff_cap_sec, self.config.backoff_base_sec * (2**attempt))
                jitter = random.uniform(1.0 - self.config.backoff_jitter_ratio, 1.0 + self.config.backoff_jitter_ratio)
                time.sleep(max(0.05, base * jitter))

        if last_error:
            raise last_error
        raise RuntimeError(f"StableFetcher.execute failed without explicit exception: provider={provider}")

    def request(
        self,
        provider: str,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout_sec: float | None = None,
    ) -> requests.Response:
        timeout = timeout_sec or self.config.timeout_sec

        def _do() -> requests.Response:
            resp = self._session.request(
                method=method.upper(),
                url=url,
                params=params,
                headers=headers,
                timeout=timeout,
            )
            resp.raise_for_status()
            return resp

        return self.execute(provider=provider, fn=_do)

    def get_json(
        self,
        provider: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout_sec: float | None = None,
    ) -> dict[str, Any]:
        resp = self.request(
            provider=provider,
            method="GET",
            url=url,
            params=params,
            headers=headers,
            timeout_sec=timeout_sec,
        )
        try:
            if not resp.encoding or resp.encoding.lower() in {"iso-8859-1", "ascii"}:
                resp.encoding = resp.apparent_encoding or "utf-8"
        except Exception:
            pass
        return json.loads(resp.text)

    def get_text(
        self,
        provider: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout_sec: float | None = None,
    ) -> str:
        resp = self.request(
            provider=provider,
            method="GET",
            url=url,
            params=params,
            headers=headers,
            timeout_sec=timeout_sec,
        )
        try:
            if not resp.encoding or resp.encoding.lower() in {"iso-8859-1", "ascii"}:
                resp.encoding = resp.apparent_encoding or "utf-8"
        except Exception:
            pass
        return resp.text

    def fetch_html_with_scrapling(
        self,
        url: str,
        *,
        provider_prefix: str = "scrapling",
        wait_selector: str | None = None,
        timeout_sec: float | None = None,
    ) -> str:
        timeout_ms = int((timeout_sec or self.config.timeout_sec) * 1000)
        fetchers = load_scrapling_fetchers()

        dynamic = fetchers.get("dynamic")
        stealthy = fetchers.get("stealthy")
        requests_fetcher = fetchers.get("requests")

        runners: list[tuple[str, Callable[[], Any]]] = []
        if dynamic:
            runners.append(
                (
                    f"{provider_prefix}.dynamic",
                    lambda: dynamic.fetch(
                        url,
                        headless=True,
                        network_idle=True,
                        timeout=timeout_ms,
                        wait_selector=wait_selector,
                    ),
                )
            )
        if stealthy:
            runners.append(
                (
                    f"{provider_prefix}.stealthy",
                    lambda: stealthy.fetch(
                        url,
                        headless=True,
                        network_idle=True,
                        timeout=timeout_ms,
                        wait_selector=wait_selector,
                    ),
                )
            )
        if requests_fetcher:
            runners.append((f"{provider_prefix}.requests", lambda: requests_fetcher.get(url)))

        for provider, runner in runners:
            try:
                resp = self.execute(provider=provider, fn=runner, max_retries=2)
                html = response_to_html(resp)
                if html and "<html" in html.lower():
                    return html
            except Exception:
                continue

        # Final plain requests fallback.
        return self.get_text(provider=f"{provider_prefix}.plain", url=url, timeout_sec=timeout_sec)


class DataQualityGuard:
    """
    Centralized data quality normalizer for ratio and valuation fields.
    """

    ratio_bounds: dict[str, tuple[float, float]] = {
        "gross_margin": (-1.0, 1.0),
        "operating_margin": (-1.0, 1.0),
        "net_margin": (-1.0, 1.0),
        "return_on_equity": (-2.0, 2.0),
        "return_on_assets": (-1.0, 1.0),
        "return_on_invested_capital": (-2.0, 2.0),
        "debt_to_equity": (0.0, 20.0),
        "debt_to_assets": (0.0, 1.5),
        "current_ratio": (0.0, 20.0),
        "quick_ratio": (0.0, 20.0),
        "cash_ratio": (0.0, 20.0),
        "operating_cash_flow_ratio": (-20.0, 20.0),
        "interest_coverage": (-200.0, 500.0),
        "free_cash_flow_yield": (-2.0, 2.0),
        "revenue_growth": (-5.0, 20.0),
        "earnings_growth": (-5.0, 20.0),
        "book_value_growth": (-5.0, 20.0),
        "earnings_per_share_growth": (-5.0, 20.0),
        "free_cash_flow_growth": (-5.0, 20.0),
        "operating_income_growth": (-5.0, 20.0),
        "ebitda_growth": (-5.0, 20.0),
        "payout_ratio": (-5.0, 5.0),
    }

    value_bounds: dict[str, tuple[float, float]] = {
        "price_to_earnings_ratio": (-300.0, 500.0),
        "price_to_book_ratio": (-50.0, 50.0),
        "price_to_sales_ratio": (0.0, 200.0),
        "enterprise_value_to_ebitda_ratio": (-300.0, 500.0),
        "enterprise_value_to_revenue_ratio": (0.0, 300.0),
        "peg_ratio": (-100.0, 100.0),
        "asset_turnover": (0.0, 100.0),
        "inventory_turnover": (0.0, 300.0),
        "receivables_turnover": (0.0, 500.0),
        "days_sales_outstanding": (0.0, 2000.0),
        "operating_cycle": (0.0, 3000.0),
        "working_capital_turnover": (-500.0, 500.0),
    }

    @staticmethod
    def normalize_number(value: Any) -> float | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            x = float(value)
            if x != x or x in (float("inf"), float("-inf")):
                return None
            return x
        text = str(value).strip().replace(",", "")
        if text in {"", "--", "None", "none", "NULL", "null", "NaN", "nan"}:
            return None
        scale = 1.0
        if text.endswith("亿"):
            scale = 1e8
            text = text[:-1]
        elif text.endswith("万"):
            scale = 1e4
            text = text[:-1]
        is_pct = text.endswith("%")
        if is_pct:
            text = text[:-1]
        try:
            parsed = float(text) * scale
        except ValueError:
            return None
        return parsed / 100.0 if is_pct else parsed

    @staticmethod
    def percent_to_ratio(value: Any) -> float | None:
        x = DataQualityGuard.normalize_number(value)
        if x is None:
            return None
        if abs(x) > 5:
            return x / 100.0
        return x

    @staticmethod
    def in_bounds(value: float | None, bounds: tuple[float, float]) -> float | None:
        if value is None:
            return None
        lo, hi = bounds
        if value < lo or value > hi:
            return None
        return value

    @classmethod
    def sanitize_payload(cls, payload: dict[str, Any]) -> dict[str, Any]:
        out = dict(payload)
        for key, bounds in cls.ratio_bounds.items():
            out[key] = cls.in_bounds(cls.normalize_number(out.get(key)), bounds)
        for key, bounds in cls.value_bounds.items():
            out[key] = cls.in_bounds(cls.normalize_number(out.get(key)), bounds)
        return out


def dump_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2, default=str)
