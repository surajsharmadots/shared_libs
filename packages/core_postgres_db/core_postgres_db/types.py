# packages/core_postgres_db/core_postgres_db/types.py
"""
Type definitions for PostgreSQL operations - Sonarqube compliant
"""
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

from .constants import DEFAULT_BATCH_SIZE, DEFAULT_PAGE_SIZE


@dataclass
class QueryOptions:
    """Query configuration with sensible defaults"""
    limit: Optional[int] = None
    offset: int = 0
    order_by: Optional[List[Tuple[str, bool]]] = None
    columns: Optional[List[str]] = None
    for_update: bool = False
    distinct: bool = False
    join_related: Optional[List[str]] = None
    
    def __post_init__(self):
        """Validate query options"""
        if self.limit is not None and self.limit <= 0:
            raise ValueError("Limit must be positive")
        if self.offset < 0:
            raise ValueError("Offset cannot be negative")


@dataclass
class BulkInsertOptions:
    """Bulk insert configuration"""
    batch_size: int = DEFAULT_BATCH_SIZE
    return_rows: bool = True
    ignore_duplicates: bool = False
    on_conflict_do_nothing: bool = False
    on_conflict_do_update: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        """Validate bulk insert options"""
        if self.batch_size <= 0:
            raise ValueError("Batch size must be positive")


@dataclass
class UpdateOptions:
    """Update operation options"""
    returning: bool = True
    atomic: bool = True


@dataclass
class PaginationParams:
    """Pagination parameters"""
    page: int = 1
    per_page: int = DEFAULT_PAGE_SIZE
    total_count: Optional[int] = None
    
    def __post_init__(self):
        """Validate pagination parameters"""
        if self.page < 1:
            raise ValueError("Page must be at least 1")
        if self.per_page <= 0:
            raise ValueError("Per page must be positive")


@dataclass
class PaginatedResult:
    """Paginated query result"""
    items: List[Dict[str, Any]]
    total: int
    page: int
    per_page: int
    total_pages: int
    
    @property
    def has_next(self) -> bool:
        """Check if there's a next page"""
        return self.page < self.total_pages
    
    @property
    def has_prev(self) -> bool:
        """Check if there's a previous page"""
        return self.page > 1
    
    @property
    def next_page(self) -> Optional[int]:
        """Get next page number"""
        return self.page + 1 if self.has_next else None
    
    @property
    def prev_page(self) -> Optional[int]:
        """Get previous page number"""
        return self.page - 1 if self.has_prev else None


class Operator(Enum):
    """SQL operators for query building"""
    EQUAL = "eq"
    NOT_EQUAL = "ne"
    GREATER_THAN = "gt"
    GREATER_EQUAL = "ge"
    LESS_THAN = "lt"
    LESS_EQUAL = "le"
    IN = "in"
    NOT_IN = "not_in"
    LIKE = "like"
    ILIKE = "ilike"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"
    BETWEEN = "between"