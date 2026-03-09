"""Retry and circuit-breaker utilities for HTTP based extractors."""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections import deque
from dataclasses import dataclass
from enum import Enum
from typing import Any, Awaitable, Callable, Deque, Optional, TypeVar

import httpx

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


@dataclass(frozen=True)
class RetryConfig:
    """Configuration for exponential-backoff retries."""

    max_attempts: int = 5
    base_delay: float = 0.5
    max_delay: float = 30.0
    multiplier: float = 2.0
    jitter: float = 0.2
    retryable_exceptions: tuple[type[Exception], ...] = (
        httpx.TimeoutException,
        httpx.ConnectError,
        httpx.ReadError,
        httpx.RemoteProtocolError,
    )
    retryable_status_codes: frozenset[int] = frozenset({408, 425, 429, 500, 502, 503, 504})


def _compute_delay(config: RetryConfig, attempt: int) -> float:
    delay = min(config.base_delay * (config.multiplier ** (attempt - 1)), config.max_delay)
    if config.jitter > 0:
        jitter = random.uniform(-config.jitter, config.jitter) * delay  # noqa: S311
        delay = max(0.0, delay + jitter)
    return delay


def with_retry(config: RetryConfig) -> Callable[[F], F]:
    """Decorator that retries async or sync functions with exponential backoff."""

    def decorator(func: F) -> F:
        if asyncio.iscoroutinefunction(func):

            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                last_exc: Optional[Exception] = None
                for attempt in range(1, config.max_attempts + 1):
                    try:
                        result = await func(*args, **kwargs)
                        if isinstance(result, httpx.Response) and result.status_code in config.retryable_status_codes:
                            result.raise_for_status()
                        return result
                    except config.retryable_exceptions as exc:
                        last_exc = exc
                    except httpx.HTTPStatusError as exc:
                        if exc.response.status_code not in config.retryable_status_codes:
                            raise
                        last_exc = exc

                    if attempt == config.max_attempts:
                        break
                    delay = _compute_delay(config, attempt)
                    logger.warning("Retrying %s attempt=%s/%s in %.2fs", func.__name__, attempt, config.max_attempts, delay)
                    await asyncio.sleep(delay)

                assert last_exc is not None
                raise last_exc

            return async_wrapper  # type: ignore[return-value]

        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exc: Optional[Exception] = None
            for attempt in range(1, config.max_attempts + 1):
                try:
                    result = func(*args, **kwargs)
                    if isinstance(result, httpx.Response) and result.status_code in config.retryable_status_codes:
                        result.raise_for_status()
                    return result
                except config.retryable_exceptions as exc:
                    last_exc = exc
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code not in config.retryable_status_codes:
                        raise
                    last_exc = exc

                if attempt == config.max_attempts:
                    break
                delay = _compute_delay(config, attempt)
                logger.warning("Retrying %s attempt=%s/%s in %.2fs", func.__name__, attempt, config.max_attempts, delay)
                time.sleep(delay)

            assert last_exc is not None
            raise last_exc

        return sync_wrapper  # type: ignore[return-value]

    return decorator


class CircuitState(str, Enum):
    OPEN = "open"
    CLOSED = "closed"
    HALF_OPEN = "half_open"


@dataclass(frozen=True)
class CircuitBreakerConfig:
    failure_threshold: float = 0.5
    window_seconds: float = 300.0
    recovery_timeout: float = 30.0
    min_calls_in_window: int = 20


@dataclass
class _CallRecord:
    timestamp: float
    success: bool


class CircuitBreaker:
    """Simple sliding-window circuit breaker."""

    def __init__(self, config: CircuitBreakerConfig | None = None) -> None:
        self.config = config or CircuitBreakerConfig()
        self._state: CircuitState = CircuitState.CLOSED
        self._last_state_change = time.monotonic()
        self._calls: Deque[_CallRecord] = deque()

    @property
    def state(self) -> CircuitState:
        return self._state

    def _prune_window(self, now: float) -> None:
        cutoff = now - self.config.window_seconds
        while self._calls and self._calls[0].timestamp < cutoff:
            self._calls.popleft()

    def allow_request(self) -> bool:
        now = time.monotonic()
        self._prune_window(now)

        if self._state == CircuitState.OPEN:
            if now - self._last_state_change >= self.config.recovery_timeout:
                self._state = CircuitState.HALF_OPEN
                self._last_state_change = now
                return True
            return False
        return True

    def record_success(self) -> None:
        now = time.monotonic()
        self._calls.append(_CallRecord(timestamp=now, success=True))
        self._prune_window(now)
        if self._state in {CircuitState.OPEN, CircuitState.HALF_OPEN}:
            self._state = CircuitState.CLOSED
            self._last_state_change = now

    def record_failure(self) -> None:
        now = time.monotonic()
        self._calls.append(_CallRecord(timestamp=now, success=False))
        self._prune_window(now)

        if self._state == CircuitState.HALF_OPEN:
            self._state = CircuitState.OPEN
            self._last_state_change = now
            return

        if len(self._calls) < self.config.min_calls_in_window:
            return

        failures = sum(1 for call in self._calls if not call.success)
        failure_rate = failures / len(self._calls)
        if failure_rate >= self.config.failure_threshold:
            self._state = CircuitState.OPEN
            self._last_state_change = now


async def call_with_circuit_breaker(
    breaker: CircuitBreaker,
    func: Callable[..., Awaitable[Any]],
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Execute an async callable under circuit-breaker control."""
    if not breaker.allow_request():
        raise RuntimeError("Circuit breaker is open; request blocked")

    try:
        result = await func(*args, **kwargs)
        breaker.record_success()
        return result
    except Exception:
        breaker.record_failure()
        raise
