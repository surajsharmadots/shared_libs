# packages/core_postgres_db/core_postgres_db/__init__.py
"""
Core PostgreSQL Database Module

This module contains all the core functionality for database operations.
It's designed to be imported by microservices that need database access.
"""

# Re-export main components for easy access
from .async_postgres import AsyncPostgresDB
from .sync_postgres import SyncPostgresDB
from .config import DatabaseConfig, get_database_config
from .exceptions import *
from .types import *
from .utils import rows_to_dicts, build_where_clause
from .query_builder import QueryBuilder
from .performance_monitor import QueryStats

__all__ = [
    # Main classes
    "AsyncPostgresDB",
    "SyncPostgresDB",
    "DatabaseConfig",
    "get_database_config",
    
    # Exceptions
    "DatabaseError",
    "ConnectionError", 
    "QueryError",
    "IntegrityError",
    "DuplicateEntryError",
    "ConstraintViolationError",
    "ForeignKeyViolationError",
    "TransactionError",
    "TimeoutError",
    "ConfigurationError",
    "ValidationError",
    "ResourceNotFoundError",
    "DatabaseOperationError",
    
    # Types
    "QueryOptions",
    "BulkInsertOptions",
    "UpdateOptions",
    "PaginationParams",
    "PaginatedResult",
    
    # Utilities
    "rows_to_dicts",
    "build_where_clause",
    "QueryBuilder",
    "QueryStats",
]