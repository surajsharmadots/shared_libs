"""
Helper functions for tests
"""
import asyncio
from typing import Any, Dict, List, Optional
from contextlib import contextmanager

def create_test_tables(db) -> None:
    """Create test tables"""
    db.execute_raw_sql("""
        CREATE TABLE IF NOT EXISTS test_users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(255) UNIQUE NOT NULL,
            age INTEGER,
            active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """, fetch_results=False)

async def async_create_test_tables(db) -> None:
    """Async create test tables"""
    await db.aexecute_raw_sql("""
        CREATE TABLE IF NOT EXISTS test_users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(255) UNIQUE NOT NULL,
            age INTEGER,
            active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """, fetch_results=False)

def cleanup_test_tables(db) -> None:
    """Cleanup test tables"""
    db.execute_raw_sql("DROP TABLE IF EXISTS test_users", fetch_results=False)

async def async_cleanup_test_tables(db) -> None:
    """Async cleanup test tables"""
    await db.aexecute_raw_sql("DROP TABLE IF EXISTS test_users", fetch_results=False)

def generate_test_data(count: int = 10) -> List[Dict[str, Any]]:
    """Generate test user data"""
    return [
        {
            "name": f"Test User {i}",
            "email": f"test{i}@example.com",
            "age": 20 + (i % 30),
            "active": i % 3 != 0
        }
        for i in range(count)
    ]

@contextmanager
def timer():
    """Context manager for timing operations"""
    import time
    start = time.time()
    yield
    end = time.time()
    print(f"Execution time: {end - start:.4f} seconds")

async def async_timer(coro):
    """Async timer for coroutines"""
    import time
    start = time.time()
    result = await coro
    end = time.time()
    print(f"Async execution time: {end - start:.4f} seconds")
    return result