"""
Abstract base class for CRUD operations.
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union, Tuple
from contextlib import AbstractContextManager

from .types import QueryOptions, BulkInsertOptions, Pagination

class BasePostgresDB(ABC):
    """
    Abstract base class defining interface for database operations.
    """
    
    @abstractmethod
    def create(
        self, 
        table_name: str, 
        data: Dict[str, Any],
        returning: bool = True
    ) -> Optional[Dict[str, Any]]:
        """Create a single record."""
        pass
    
    @abstractmethod
    def read(
        self, 
        table_name: str, 
        conditions: Optional[Dict[str, Any]] = None,
        options: Optional[QueryOptions] = None
    ) -> List[Dict[str, Any]]:
        """Read records with filtering."""
        pass
    
    @abstractmethod
    def read_one(
        self, 
        table_name: str, 
        conditions: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Read single record."""
        pass
    
    @abstractmethod
    def update(
        self, 
        table_name: str, 
        data: Dict[str, Any], 
        conditions: Dict[str, Any],
        returning: bool = True
    ) -> List[Dict[str, Any]]:
        """Update records."""
        pass
    
    @abstractmethod
    def delete(
        self, 
        table_name: str, 
        conditions: Dict[str, Any]
    ) -> int:
        """Delete records."""
        pass
    
    @abstractmethod
    def bulk_create(
        self, 
        table_name: str, 
        data_list: List[Dict[str, Any]], 
        options: Optional[BulkInsertOptions] = None
    ) -> List[Dict[str, Any]]:
        """Bulk insert records."""
        pass
    
    @abstractmethod
    def execute_raw_sql(
        self, 
        sql_query: str, 
        parameters: Optional[Dict[str, Any]] = None,
        fetch_results: bool = True
    ) -> Union[List[Dict[str, Any]], int, None]:
        """Execute raw SQL."""
        pass
    
    @abstractmethod
    def transaction(self) -> AbstractContextManager:
        """Get transaction context manager."""
        pass
    
    @abstractmethod
    def count(
        self, 
        table_name: str, 
        conditions: Optional[Dict[str, Any]] = None
    ) -> int:
        """Count records."""
        pass
    
    @abstractmethod
    def exists(
        self, 
        table_name: str, 
        conditions: Dict[str, Any]
    ) -> bool:
        """Check if record exists."""
        pass
    
    @abstractmethod
    def paginate(
        self,
        table_name: str,
        conditions: Optional[Dict[str, Any]] = None,
        page: int = 1,
        per_page: int = 20,
        order_by: Optional[List[Tuple[str, bool]]] = None
    ) -> Dict[str, Any]:
        """Paginate records."""
        pass
    
    @abstractmethod
    def get_stats(self) -> Dict[str, Any]:
        """Get database statistics."""
        pass
    
    @abstractmethod
    def health_check(self) -> bool:
        """Perform health check."""
        pass
    
    @abstractmethod
    def close(self):
        """Close database connection."""
        pass