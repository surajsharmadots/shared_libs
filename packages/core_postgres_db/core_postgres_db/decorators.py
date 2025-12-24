# packages/core_postgres_db/core_postgres_db/decorators.py
"""
Decorators for database operations - Sonarqube compliant
"""
import time
import logging
import asyncio
from functools import wraps
from typing import Callable

from .constants import (
    MAX_DEADLOCK_RETRIES, INITIAL_RETRY_WAIT
)

logger = logging.getLogger(__name__)


def retry_on_deadlock(max_retries: int = MAX_DEADLOCK_RETRIES, 
                     initial_wait: float = INITIAL_RETRY_WAIT):
    """
    Retry decorator for deadlock and serialization failures
    
    Args:
        max_retries: Maximum number of retry attempts
        initial_wait: Initial wait time between retries (exponential backoff)
    """
    def decorator(func: Callable) -> Callable:
        
        if asyncio.iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                last_exception = None
                
                for attempt in range(max_retries):
                    try:
                        return await func(*args, **kwargs)
                    except Exception as e:
                        last_exception = e
                        
                        # Check if retryable error
                        if not _is_retryable_error(e) or attempt >= max_retries - 1:
                            raise
                        
                        # Calculate wait time with exponential backoff
                        wait_time = initial_wait * (2 ** attempt)
                        
                        logger.warning(
                            f"Retryable error in {func.__name__}, "
                            f"retrying in {wait_time:.2f}s "
                            f"(attempt {attempt + 1}/{max_retries}): {e}"
                        )
                        
                        await asyncio.sleep(wait_time)
                
                raise last_exception if last_exception else RuntimeError("Retry logic failed")
            
            return async_wrapper
        
        else:
            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                last_exception = None
                
                for attempt in range(max_retries):
                    try:
                        return func(*args, **kwargs)
                    except Exception as e:
                        last_exception = e
                        
                        # Check if retryable error
                        if not _is_retryable_error(e) or attempt >= max_retries - 1:
                            raise
                        
                        # Calculate wait time with exponential backoff
                        wait_time = initial_wait * (2 ** attempt)
                        
                        logger.warning(
                            f"Retryable error in {func.__name__}, "
                            f"retrying in {wait_time:.2f}s "
                            f"(attempt {attempt + 1}/{max_retries}): {e}"
                        )
                        
                        time.sleep(wait_time)
                
                raise last_exception if last_exception else RuntimeError("Retry logic failed")
            
            return sync_wrapper
    
    return decorator


def timeout(seconds: int):
    """
    Timeout decorator for database operations
    
    Args:
        seconds: Timeout in seconds
    """
    def decorator(func: Callable) -> Callable:
        
        if asyncio.iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                try:
                    return await asyncio.wait_for(
                        func(*args, **kwargs),
                        timeout=seconds
                    )
                except asyncio.TimeoutError:
                    raise TimeoutError(
                        f"Async operation {func.__name__} "
                        f"timed out after {seconds} seconds"
                    )
            
            return async_wrapper
        
        else:
            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                # For sync functions, we can't easily timeout
                # without threads, so we just log a warning
                logger.warning(
                    f"Timeout decorator used on sync function {func.__name__}. "
                    f"Consider using async version for proper timeout handling."
                )
                return func(*args, **kwargs)
            
            return sync_wrapper
    
    return decorator


def log_query_execution(func: Callable) -> Callable:
    """
    Log query execution time and details
    
    Args:
        func: Function to decorate
    """
    @wraps(func)
    async def async_wrapper(*args, **kwargs):
        start_time = time.time()
        func_name = func.__name__
        
        try:
            result = await func(*args, **kwargs)
            elapsed = time.time() - start_time
            
            if elapsed > 1.0:  # Log slow queries
                logger.warning(
                    f"Slow async query {func_name} executed in {elapsed:.3f}s"
                )
            else:
                logger.debug(
                    f"Async query {func_name} executed in {elapsed:.3f}s"
                )
            
            return result
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(
                f"Async query {func_name} failed after {elapsed:.3f}s: {e}"
            )
            raise
    
    @wraps(func)
    def sync_wrapper(*args, **kwargs):
        start_time = time.time()
        func_name = func.__name__
        
        try:
            result = func(*args, **kwargs)
            elapsed = time.time() - start_time
            
            if elapsed > 1.0:  # Log slow queries
                logger.warning(
                    f"Slow sync query {func_name} executed in {elapsed:.3f}s"
                )
            else:
                logger.debug(
                    f"Sync query {func_name} executed in {elapsed:.3f}s"
                )
            
            return result
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(
                f"Sync query {func_name} failed after {elapsed:.3f}s: {e}"
            )
            raise
    
    if asyncio.iscoroutinefunction(func):
        return async_wrapper
    return sync_wrapper


def _is_retryable_error(error: Exception) -> bool:
    """
    Check if error is retryable (deadlock, serialization, etc.)
    
    Args:
        error: Exception to check
    
    Returns:
        True if error is retryable
    """
    error_str = str(error).lower()
    
    # Check for deadlock/serialization errors
    retryable_keywords = [
        "deadlock",
        "serialization",
        "could not serialize",
        "lock conflict",
        "try again",
        "retry",
        "timeout",
        "connection",
    ]
    
    return any(keyword in error_str for keyword in retryable_keywords)