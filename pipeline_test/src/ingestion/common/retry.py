"""
Retry decorator with exponential backoff and circuit breaker pattern
"""
import logging
from dataclasses import dataclass
import time 
from typing import Any , Callable , Optional
import httpx
from collections import deque

logger = logging.getLogger(name=__name__)

@dataclass(frozen=True)
class RetryConfig :
    """Configuration for the retry decorator"""
    max_attempts : int = 5
    base_delay : float = 2.0
    max_delay : float = 120
    multiplier : float = 2.0
    retryable_exeptions = (httpx.TimeoutExeptions)
    retryable_status_code : frozenset[int] = frozenset[Any]({429,502,503,504,})

def with_retry(config: RetryConfig) -> Callable:
    """
    Decorator that retries a function with exponential backoff and jitter
    noqa : S311
    """




"""
Retry decorator
"""

"""
Circuit breaker
"""

class CircuitState(Enum):
    """
    States of the circuit breaker
    """

    OPEN  = "open"
    CLOSE = "closed"
    HALF_OPEN = "half_open"

class CircuitBreakerConfig:
    """
    Configuration of the circuit breaker
    """
    failure_threshold : float = 0.5
    window_seconds : float = 300.0
    recovery_failure : float = 60
    min_calls_in_window : int =  10


@dataclass
class _CallRecord :
    """
    a single call within the slidin window
    """

    timestamp : float 
    sucess : bool 


    class CircuitBreaker:
        """
        Sliding-window circuit breaker

        state transitions : 
        CLOSED --> OPEN when failure rate > threshold(min calls met)
        OPEN --> HALF-OPEN after recovery time out elapsed
        HALF-OPEN --> OPEN on sucess
        HALF-OPEN --> CLOSED on failure
        """

        def __init__(self, config:CircuitBreakerConfig|None = None) -> None:
            ...

@property
def state(self) -> CircuitState:
    return self.state
def allow_request(self) -> bool:
    """
    check wether a request is allowed
    
    """
    #create the rest of the internals
    ...