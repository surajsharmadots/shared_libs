"""
Base classes for search operations
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union
from contextlib import AbstractContextManager

from .types import (
    SearchQuery, FacetConfig, SortOption, IndexSettings,
    IndexMappings, BulkOperationResult, SearchResult
)


class BaseSearchClient(ABC):
    """
    Abstract base class for search operations
    
    Provides common interface for both async and sync implementations
    """
    
    # ============= CORE SEARCH OPERATIONS =============
    
    @abstractmethod
    def search(
        self,
        index_name: str,
        query: Dict[str, Any],
        size: int = 10,
        from_: int = 0,
        sort: Optional[List[Union[str, SortOption, Dict[str, Any]]]] = None,
        aggs: Optional[Dict[str, Any]] = None,
        highlight: Optional[Dict[str, Any]] = None,
        source: Optional[Union[bool, List[str]]] = None
    ) -> SearchResult:
        """
        Search documents
        
        Args:
            index_name: Target index
            query: OpenSearch query DSL
            size: Number of results
            from_: Pagination offset
            sort: Sorting criteria
            aggs: Aggregations
            highlight: Highlight configuration
            source: Source filtering
            
        Returns:
            Search results
        """
        pass
    
    @abstractmethod
    def get_document(
        self,
        index_name: str,
        document_id: str,
        source: Optional[Union[bool, List[str]]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get single document by ID
        
        Args:
            index_name: Target index
            document_id: Document ID
            source: Source filtering
            
        Returns:
            Document or None if not found
        """
        pass
    
    @abstractmethod
    def exists(
        self,
        index_name: str,
        document_id: str
    ) -> bool:
        """
        Check if document exists
        
        Args:
            index_name: Target index
            document_id: Document ID
            
        Returns:
            True if document exists
        """
        pass
    
    # ============= DOCUMENT OPERATIONS =============
    
    @abstractmethod
    def index_document(
        self,
        index_name: str,
        document: Dict[str, Any],
        document_id: Optional[str] = None,
        refresh: bool = False
    ) -> str:
        """
        Index a single document
        
        Args:
            index_name: Target index
            document: Document data
            document_id: Optional document ID (auto-generated if not provided)
            refresh: Whether to refresh index
            
        Returns:
            Document ID
        """
        pass
    
    @abstractmethod
    def update_document(
        self,
        index_name: str,
        document_id: str,
        updates: Dict[str, Any],
        refresh: bool = False
    ) -> bool:
        """
        Update document
        
        Args:
            index_name: Target index
            document_id: Document ID
            updates: Fields to update
            refresh: Whether to refresh index
            
        Returns:
            True if successful
        """
        pass
    
    @abstractmethod
    def delete_document(
        self,
        index_name: str,
        document_id: str,
        refresh: bool = False
    ) -> bool:
        """
        Delete document
        
        Args:
            index_name: Target index
            document_id: Document ID
            refresh: Whether to refresh index
            
        Returns:
            True if successful
        """
        pass
    
    # ============= BULK OPERATIONS =============
    
    @abstractmethod
    def bulk_index(
        self,
        index_name: str,
        documents: List[Dict[str, Any]],
        refresh: bool = False,
        batch_size: int = 1000
    ) -> BulkOperationResult:
        """
        Bulk index documents
        
        Args:
            index_name: Target index
            documents: List of documents
            refresh: Whether to refresh after each batch
            batch_size: Documents per batch
            
        Returns:
            Bulk operation result
        """
        pass
    
    @abstractmethod
    def bulk_update(
        self,
        index_name: str,
        updates: List[Dict[str, Any]],
        refresh: bool = False
    ) -> BulkOperationResult:
        """
        Bulk update documents
        
        Args:
            index_name: Target index
            updates: List of update operations
            refresh: Whether to refresh index
            
        Returns:
            Bulk operation result
        """
        pass
    
    @abstractmethod
    def bulk_delete(
        self,
        index_name: str,
        document_ids: List[str],
        refresh: bool = False
    ) -> BulkOperationResult:
        """
        Bulk delete documents
        
        Args:
            index_name: Target index
            document_ids: List of document IDs
            refresh: Whether to refresh index
            
        Returns:
            Bulk operation result
        """
        pass
    
    # ============= INDEX MANAGEMENT =============
    
    @abstractmethod
    def create_index(
        self,
        index_name: str,
        mappings: Optional[IndexMappings] = None,
        settings: Optional[IndexSettings] = None,
        aliases: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Create new index
        
        Args:
            index_name: Index name
            mappings: Index mappings
            settings: Index settings
            aliases: Index aliases
            
        Returns:
            True if successful
        """
        pass
    
    @abstractmethod
    def delete_index(
        self,
        index_name: str
    ) -> bool:
        """
        Delete index
        
        Args:
            index_name: Index name
            
        Returns:
            True if successful
        """
        pass
    
    @abstractmethod
    def index_exists(
        self,
        index_name: str
    ) -> bool:
        """
        Check if index exists
        
        Args:
            index_name: Index name
            
        Returns:
            True if index exists
        """
        pass
    
    @abstractmethod
    def get_index_settings(
        self,
        index_name: str
    ) -> Dict[str, Any]:
        """
        Get index settings
        
        Args:
            index_name: Index name
            
        Returns:
            Index settings
        """
        pass
    
    @abstractmethod
    def update_index_settings(
        self,
        index_name: str,
        settings: Dict[str, Any]
    ) -> bool:
        """
        Update index settings
        
        Args:
            index_name: Index name
            settings: Settings to update
            
        Returns:
            True if successful
        """
        pass
    
    # ============= SCROLL OPERATIONS =============
    
    @abstractmethod
    def scroll_search(
        self,
        index_name: str,
        query: Dict[str, Any],
        scroll: str = "2m",
        size: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Scroll search for large result sets
        
        Args:
            index_name: Target index
            query: Search query
            scroll: Scroll timeout
            size: Batch size
            
        Returns:
            All matching documents
        """
        pass
    
    # ============= E-COMMERCE SPECIFIC =============
    
    @abstractmethod
    def product_search(
        self,
        query_text: str,
        filters: Optional[Dict[str, Any]] = None,
        category: Optional[str] = None,
        price_range: Optional[tuple] = None,
        sort_by: str = "relevance",
        page: int = 1,
        per_page: int = 20
    ) -> SearchResult:
        """
        E-commerce product search with filtering
        
        Args:
            query_text: Search query
            filters: Additional filters
            category: Product category filter
            price_range: (min_price, max_price)
            sort_by: Sort option
            page: Page number
            per_page: Results per page
            
        Returns:
            Search results with facets
        """
        pass
    
    @abstractmethod
    def autocomplete(
        self,
        index_name: str,
        field: str,
        prefix: str,
        size: int = 5
    ) -> List[str]:
        """
        Autocomplete suggestions
        
        Args:
            index_name: Target index
            field: Field to search
            prefix: User input prefix
            size: Number of suggestions
            
        Returns:
            List of suggestions
        """
        pass
    
    @abstractmethod
    def more_like_this(
        self,
        index_name: str,
        document_id: str,
        fields: List[str],
        max_results: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Find similar documents
        
        Args:
            index_name: Target index
            document_id: Reference document ID
            fields: Fields to compare
            max_results: Maximum results
            
        Returns:
            Similar documents
        """
        pass
    
    # ============= ANALYTICS =============
    
    @abstractmethod
    def aggregate(
        self,
        index_name: str,
        aggregations: Dict[str, Any],
        query: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Perform aggregations
        
        Args:
            index_name: Target index
            aggregations: Aggregation definitions
            query: Optional query to filter documents
            
        Returns:
            Aggregation results
        """
        pass
    
    @abstractmethod
    def get_index_stats(
        self,
        index_name: str
    ) -> Dict[str, Any]:
        """
        Get index statistics
        
        Args:
            index_name: Index name
            
        Returns:
            Index statistics
        """
        pass
    
    @abstractmethod
    def cluster_health(
        self
    ) -> Dict[str, Any]:
        """
        Get cluster health
        
        Returns:
            Cluster health information
        """
        pass
    
    # ============= UTILITIES =============
    
    @abstractmethod
    def refresh_index(
        self,
        index_name: str
    ) -> bool:
        """
        Refresh index (make operations visible)
        
        Args:
            index_name: Index name
            
        Returns:
            True if successful
        """
        pass
    
    @abstractmethod
    def flush_index(
        self,
        index_name: str
    ) -> bool:
        """
        Flush index (persist to disk)
        
        Args:
            index_name: Index name
            
        Returns:
            True if successful
        """
        pass
    
    @abstractmethod
    def close(self):
        """Close client connection"""
        pass


class AsyncBaseSearchClient(BaseSearchClient, ABC):
    """Base class for async search clients"""
    
    @abstractmethod
    async def asearch(self, *args, **kwargs):
        """Async search"""
        pass
    
    @abstractmethod
    async def aget_document(self, *args, **kwargs):
        """Async get document"""
        pass
    
    @abstractmethod
    async def aindex_document(self, *args, **kwargs):
        """Async index document"""
        pass
    
    # Add other async methods...