"""
Decorators for database operations
"""
import time
import logging
import signal
from functools import wraps
from typing import Callable, Any
import asyncio

logger = logging.getLogger(__name__)

def retry_on_deadlock(max_retries: int = 3, initial_wait: float = 0.1):
    """
    Retry decorator for deadlock situations
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    error_str = str(e).lower()
                    
                    # Check for deadlock or serialization failure
                    is_deadlock = any(
                        keyword in error_str 
                        for keyword in ["deadlock", "serialization", "could not serialize"]
                    )
                    
                    if is_deadlock and attempt < max_retries - 1:
                        wait_time = initial_wait * (2 ** attempt)
                        logger.warning(
                            f"Deadlock detected in {func.__name__}, "
                            f"retrying in {wait_time:.2f}s (attempt {attempt + 1}/{max_retries})"
                        )
                        time.sleep(wait_time)
                        continue
                    raise
            
            raise last_exception
        
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    error_str = str(e).lower()
                    
                    # Check for deadlock or serialization failure
                    is_deadlock = any(
                        keyword in error_str 
                        for keyword in ["deadlock", "serialization", "could not serialize"]
                    )
                    
                    if is_deadlock and attempt < max_retries - 1:
                        wait_time = initial_wait * (2 ** attempt)
                        logger.warning(
                            f"Deadlock detected in {func.__name__}, "
                            f"retrying in {wait_time:.2f}s (attempt {attempt + 1}/{max_retries})"
                        )
                        await asyncio.sleep(wait_time)
                        continue
                    raise
            
            raise last_exception
        
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return wrapper
    
    return decorator

def timeout(seconds: int):
    """
    Timeout decorator for database operations
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            def handler(signum, frame):
                raise TimeoutError(f"Operation {func.__name__} timed out after {seconds} seconds")
            
            # Set the signal handler
            signal.signal(signal.SIGALRM, handler)
            signal.alarm(seconds)
            
            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)  # Disable the alarm
            
            return result
        
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            
            try:
                return await asyncio.wait_for(
                    func(*args, **kwargs),
                    timeout=seconds
                )
            except asyncio.TimeoutError:
                raise TimeoutError(f"Async operation {func.__name__} timed out after {seconds} seconds")
        
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return wrapper
    
    return decorator

def log_query_execution(func: Callable) -> Callable:
    """
    Log query execution time and details
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        func_name = func.__name__
        
        try:
            result = func(*args, **kwargs)
            elapsed = time.time() - start_time
            
            logger.debug(
                f"Query {func_name} executed in {elapsed:.3f}s"
            )
            
            return result
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(
                f"Query {func_name} failed after {elapsed:.3f}s: {e}"
            )
            raise
    
    @wraps(func)
    async def async_wrapper(*args, **kwargs):
        start_time = time.time()
        func_name = func.__name__
        
        try:
            result = await func(*args, **kwargs)
            elapsed = time.time() - start_time
            
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
    
    if asyncio.iscoroutinefunction(func):
        return async_wrapper
    return wrapper