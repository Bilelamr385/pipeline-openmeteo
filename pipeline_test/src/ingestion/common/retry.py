"""Retry and circuit-breaker utilities for extractors."""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections import deque
from dataclasses import dataclass
from enum import Enum
from typing import Any, Awaitable, Callable, Deque, Optional, TypeVar
from urllib.error import HTTPError

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


@dataclass(frozen=True)
class RetryConfig:
    """Configuration for exponential-backoff retries."""

    max_attempts: int = 5
    base_delay: float = 1.0
    max_delay: float = 60.0
    multiplier: float = 2.0
    jitter: float = 0.2
    jitter_on_retry_after: bool = False
    retryable_exceptions: tuple[type[Exception], ...] = (TimeoutError, OSError)
    retryable_status_codes: frozenset[int] = frozenset({408, 425, 429, 500, 502, 503, 504})


def _apply_jitter(delay: float, jitter_factor: float) -> float:
    if jitter_factor <= 0:
        return delay
    jitter = random.uniform(-jitter_factor, jitter_factor) * delay  # noqa: S311
    return max(0.0, delay + jitter)


def _compute_delay(config: RetryConfig, attempt: int) -> float:
    base = min(config.base_delay * (config.multiplier ** (attempt - 1)), config.max_delay)
    return _apply_jitter(base, config.jitter)


def _retry_after_delay(exc: Exception) -> float | None:
    if not isinstance(exc, HTTPError):
        return None
    header_value = exc.headers.get("Retry-After") if exc.headers else None
    if not header_value:
        return None
    try:
        return max(0.0, float(header_value))
    except ValueError:
        return None


def _is_retryable_exception(exc: Exception, config: RetryConfig) -> bool:
    if isinstance(exc, HTTPError):
        return exc.code in config.retryable_status_codes
    return isinstance(exc, config.retryable_exceptions)


def with_retry(config: RetryConfig) -> Callable[[F], F]:
    """Decorator that retries async or sync functions with exponential backoff."""

    def decorator(func: F) -> F:
        if asyncio.iscoroutinefunction(func):

            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                last_exc: Optional[Exception] = None
                for attempt in range(1, config.max_attempts + 1):
                    try:
                        return await func(*args, **kwargs)
                    except Exception as exc:  # noqa: BLE001
                        if not _is_retryable_exception(exc, config):
                            raise
                        last_exc = exc
                        if attempt == config.max_attempts:
                            break
                        retry_after = _retry_after_delay(exc)
                        if retry_after is not None:
                            delay = _apply_jitter(retry_after, config.jitter if config.jitter_on_retry_after else 0.0)
                        else:
                            delay = _compute_delay(config, attempt)
                        logger.warning(
                            "Retrying %s attempt=%s/%s in %.2fs (error=%s)",
                            func.__name__,
                            attempt,
                            config.max_attempts,
                            delay,
                            exc,
                        )
                        await asyncio.sleep(delay)
                assert last_exc is not None
                raise last_exc

            return async_wrapper  # type: ignore[return-value]

        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exc: Optional[Exception] = None
            for attempt in range(1, config.max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as exc:  # noqa: BLE001
                    if not _is_retryable_exception(exc, config):
                        raise
                    last_exc = exc
                    if attempt == config.max_attempts:
                        break
                    retry_after = _retry_after_delay(exc)
                    if retry_after is not None:
                        delay = _apply_jitter(retry_after, config.jitter if config.jitter_on_retry_after else 0.0)
                    else:
                        delay = _compute_delay(config, attempt)
                    logger.warning(
                        "Retrying %s attempt=%s/%s in %.2fs (error=%s)",
                        func.__name__,
                        attempt,
                        config.max_attempts,
                        delay,
                        exc,
                    )
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
