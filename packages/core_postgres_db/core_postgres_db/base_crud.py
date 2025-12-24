# packages/core_postgres_db/core_postgres_db/base_crud.py
"""
Abstract base class for CRUD operations - Sonarqube compliant
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union
from contextlib import AbstractContextManager

from .types import QueryOptions, BulkInsertOptions, PaginatedResult


class BaseDatabaseClient(ABC):
    """
    Abstract base class defining interface for all database operations
    Sonarqube compliant: Proper ABC with type hints
    """
    
    # ============= CRUD Operations =============
    
    @abstractmethod
    def create(
        self, 
        table_name: str, 
        data: Dict[str, Any],
        returning: bool = True
    ) -> Optional[Dict[str, Any]]:
        """Create a single record"""
        pass
    
    @abstractmethod
    def read(
        self, 
        table_name: str, 
        conditions: Optional[Dict[str, Any]] = None,
        options: Optional[QueryOptions] = None
    ) -> List[Dict[str, Any]]:
        """Read records with filtering"""
        pass
    
    @abstractmethod
    def update(
        self, 
        table_name: str, 
        data: Dict[str, Any], 
        conditions: Dict[str, Any],
        returning: bool = True
    ) -> List[Dict[str, Any]]:
        """Update records"""
        pass
    
    @abstractmethod
    def delete(
        self, 
        table_name: str, 
        conditions: Dict[str, Any]
    ) -> int:
        """Delete records and return count"""
        pass
    
    # ============= Bulk Operations =============
    
    @abstractmethod
    def bulk_create(
        self, 
        table_name: str, 
        data_list: List[Dict[str, Any]], 
        options: Optional[BulkInsertOptions] = None
    ) -> List[Dict[str, Any]]:
        """Bulk insert records"""
        pass
    
    # ============= Query Operations =============
    
    @abstractmethod
    def read_one(
        self, 
        table_name: str, 
        conditions: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Read single record"""
        pass
    
    @abstractmethod
    def read_by_id(
        self, 
        table_name: str, 
        record_id: Any
    ) -> Optional[Dict[str, Any]]:
        """Read record by ID column"""
        pass
    
    @abstractmethod
    def exists(
        self, 
        table_name: str, 
        conditions: Dict[str, Any]
    ) -> bool:
        """Check if record exists"""
        pass
    
    @abstractmethod
    def count(
        self, 
        table_name: str, 
        conditions: Optional[Dict[str, Any]] = None
    ) -> int:
        """Count records"""
        pass
    
    # ============= Pagination =============
    
    @abstractmethod
    def paginate(
        self,
        table_name: str,
        conditions: Optional[Dict[str, Any]] = None,
        page: int = 1,
        per_page: int = 20,
        order_by: Optional[List[tuple]] = None
    ) -> PaginatedResult:
        """Paginate records"""
        pass
    
    # ============= Raw SQL =============
    
    @abstractmethod
    def execute_raw_sql(
        self, 
        sql_query: str, 
        parameters: Optional[Dict[str, Any]] = None,
        fetch_results: bool = True
    ) -> Union[List[Dict[str, Any]], int, None]:
        """Execute raw SQL query"""
        pass
    
    # ============= Transactions =============
    
    @abstractmethod
    def transaction(self) -> AbstractContextManager:
        """Get transaction context manager"""
        pass
    
    # ============= Utility Methods =============
    
    @abstractmethod
    def get_stats(self) -> Dict[str, Any]:
        """Get database statistics"""
        pass
    
    @abstractmethod
    def health_check(self) -> bool:
        """Check database connection health"""
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Close database connections"""
        pass
    
    # ============= Async Interface (Optional) =============
    
    @abstractmethod
    async def acreate(
        self, 
        table_name: str, 
        data: Dict[str, Any],
        returning: bool = True
    ) -> Optional[Dict[str, Any]]:
        """Async create a single record"""
        pass
    
    @abstractmethod
    async def aread(
        self, 
        table_name: str, 
        conditions: Optional[Dict[str, Any]] = None,
        options: Optional[QueryOptions] = None
    ) -> List[Dict[str, Any]]:
        """Async read records"""
        pass
    
    # Add other async methods as needed...


# ============= Async Base Interface =============

class AsyncBaseDatabaseClient(BaseDatabaseClient, ABC):
    """Base class for async database clients"""
    
    @abstractmethod
    async def atransaction(self):
        """Async transaction context manager"""
        pass
    
    @abstractmethod
    async def aclose(self) -> None:
        """Async close connections"""
        pass