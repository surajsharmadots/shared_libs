"""
Core PostgreSQL Database Library for E-commerce Applications
"""
from .sync_crud_operations import SyncPostgresDB
from .async_crud_operations import AsyncPostgresDB
from .connection import get_database_config, DatabaseConfig
from .exceptions import (
    DatabaseError, ConnectionError, TimeoutError,
    DuplicateEntryError, ConstraintViolationError,
    RecordNotFoundError, TransactionError, DeadlockError
)
from .session_manager import SessionManager, AsyncSessionManager
from .transactions import TransactionManager, AsyncTransactionManager

__version__ = "1.0.0"
__all__ = [
    "SyncPostgresDB",
    "AsyncPostgresDB",
    "get_database_config",
    "DatabaseConfig",
    "DatabaseError",
    "ConnectionError",
    "TimeoutError",
    "DuplicateEntryError",
    "ConstraintViolationError",
    "RecordNotFoundError",
    "TransactionError",
    "DeadlockError",
    "SessionManager",
    "AsyncSessionManager",
    "TransactionManager",
    "AsyncTransactionManager",
]