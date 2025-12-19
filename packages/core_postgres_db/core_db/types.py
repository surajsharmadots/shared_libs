"""
Type definitions for PostgreSQL operations
"""
from typing import Any, Dict, List, Optional, Tuple, Union, Callable
from dataclasses import dataclass
from enum import Enum

@dataclass
class DatabaseConfig:
    """Database configuration from environment"""
    connection_string: str
    pool_size: int = 20
    max_overflow: int = 30
    pool_timeout: int = 30
    pool_recycle: int = 3600
    statement_timeout: int = 30000  # milliseconds
    schema: Optional[str] = None
    echo: bool = False
    use_ssl: bool = False

@dataclass
class QueryOptions:
    """Query configuration options"""
    limit: Optional[int] = None
    offset: int = 0
    order_by: Optional[List[Tuple[str, bool]]] = None
    columns: Optional[List[str]] = None
    for_update: bool = False
    distinct: bool = False
    join_related: Optional[List[str]] = None

@dataclass
class BulkInsertOptions:
    """Bulk insert configuration"""
    batch_size: int = 1000
    return_rows: bool = True
    ignore_duplicates: bool = False
    on_conflict_do_nothing: bool = False
    on_conflict_do_update: Optional[Dict[str, Any]] = None

@dataclass
class UpdateOptions:
    """Update operation options"""
    returning: bool = True
    atomic: bool = True

class IsolationLevel(Enum):
    """Transaction isolation levels"""
    READ_COMMITTED = "READ COMMITTED"
    REPEATABLE_READ = "REPEATABLE READ"
    SERIALIZABLE = "SERIALIZABLE"