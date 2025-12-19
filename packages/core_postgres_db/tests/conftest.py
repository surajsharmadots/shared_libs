"""
Test configuration and fixtures for PostgreSQL library tests
"""
import os
import pytest
import asyncio
from typing import Dict, Any, Generator
from unittest.mock import Mock, AsyncMock

import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core_postgresql_db import SyncPostgresDB, AsyncPostgresDB
from core_postgresql_db.connection import get_database_config

# Test database configuration
TEST_DATABASE_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql://test:test@localhost:5432/test_db"
)

# Skip database tests if no database available
SKIP_DB_TESTS = os.environ.get("SKIP_DB_TESTS", "false").lower() == "true"

# Fixture for event loop
@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

# Database connection fixtures
@pytest.fixture(scope="session")
def test_database_config() -> Dict[str, Any]:
    """Get test database configuration"""
    return get_database_config(
        connection_string=TEST_DATABASE_URL,
        pool_size=5,
        max_overflow=10,
        pool_timeout=10,
        pool_recycle=1800,
        statement_timeout=10000,
        echo=False
    )

@pytest.fixture(scope="session")
def sync_test_db(test_database_config):
    """Create sync test database connection"""
    if SKIP_DB_TESTS:
        pytest.skip("Database tests are disabled")
    
    db = SyncPostgresDB(config=test_database_config)
    
    # Create test tables
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
    
    db.execute_raw_sql("""
        CREATE TABLE IF NOT EXISTS test_products (
            id SERIAL PRIMARY KEY,
            name VARCHAR(200) NOT NULL,
            price DECIMAL(10, 2) NOT NULL,
            stock INTEGER DEFAULT 0,
            category VARCHAR(100)
        )
    """, fetch_results=False)
    
    yield db
    
    # Cleanup
    db.execute_raw_sql("DROP TABLE IF EXISTS test_users", fetch_results=False)
    db.execute_raw_sql("DROP TABLE IF EXISTS test_products", fetch_results=False)
    db.close()

@pytest.fixture(scope="session")
async def async_test_db(test_database_config):
    """Create async test database connection"""
    if SKIP_DB_TESTS:
        pytest.skip("Database tests are disabled")
    
    db = AsyncPostgresDB(config=test_database_config)
    
    # Create test tables
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
    
    await db.aexecute_raw_sql("""
        CREATE TABLE IF NOT EXISTS test_products (
            id SERIAL PRIMARY KEY,
            name VARCHAR(200) NOT NULL,
            price DECIMAL(10, 2) NOT NULL,
            stock INTEGER DEFAULT 0,
            category VARCHAR(100)
        )
    """, fetch_results=False)
    
    yield db
    
    # Cleanup
    await db.aexecute_raw_sql("DROP TABLE IF EXISTS test_users", fetch_results=False)
    await db.aexecute_raw_sql("DROP TABLE IF EXISTS test_products", fetch_results=False)
    await db.aclose()

# Test data fixtures
@pytest.fixture
def sample_user_data() -> Dict[str, Any]:
    return {
        "name": "John Doe",
        "email": "john@example.com",
        "age": 30,
        "active": True
    }

@pytest.fixture
def sample_product_data() -> Dict[str, Any]:
    return {
        "name": "Test Product",
        "price": 99.99,
        "stock": 10,
        "category": "electronics"
    }

@pytest.fixture
def sample_bulk_users() -> list[Dict[str, Any]]:
    return [
        {"name": f"User {i}", "email": f"user{i}@example.com", "age": 20 + i}
        for i in range(1, 6)
    ]

# Mock fixtures
@pytest.fixture
def mock_sync_engine():
    """Mock SQLAlchemy engine for sync tests"""
    mock_engine = Mock()
    mock_connection = Mock()
    mock_result = Mock()
    
    mock_engine.connect.return_value.__enter__.return_value = mock_connection
    mock_engine.begin.return_value.__enter__.return_value = mock_connection
    mock_connection.execute.return_value = mock_result
    
    return mock_engine

@pytest.fixture
def mock_async_engine():
    """Mock SQLAlchemy async engine"""
    mock_engine = AsyncMock()
    mock_connection = AsyncMock()
    mock_result = AsyncMock()
    
    mock_engine.connect.return_value.__aenter__.return_value = mock_connection
    mock_engine.begin.return_value.__aenter__.return_value = mock_connection
    mock_connection.execute.return_value = mock_result
    
    return mock_engine

@pytest.fixture
def mock_query_stats():
    """Mock query stats"""
    mock_stats = Mock()
    mock_stats.record_read = Mock()
    mock_stats.record_write = Mock()
    mock_stats.record_raw_sql = Mock()
    mock_stats.snapshot.return_value = {"test": "stats"}
    
    return mock_stats