"""
Core OpenSearch Module

This module provides async-first OpenSearch client for e-commerce search,
analytics, and real-time data operations.
"""

from .async_opensearch import AsyncOpenSearchDB
from .sync_opensearch import SyncOpenSearchDB
from .config import OpenSearchConfig, get_opensearch_config
from .types import *
from .exceptions import *
from .utils import OpenSearchQueryBuilder, BulkProcessor, IndexManager
from .performance_monitor import OpenSearchStats

# Factory function
def create_opensearch_client(
    hosts: list = None,
    use_async: bool = True,
    **kwargs
):
    """
    Factory function to create OpenSearch client
    
    Args:
        hosts: List of OpenSearch hosts
        use_async: Whether to create async client (default: True)
        **kwargs: Additional configuration
    
    Returns:
        AsyncOpenSearchDB or SyncOpenSearchDB instance
    """
    if use_async:
        return AsyncOpenSearchDB(hosts=hosts, **kwargs)
    else:
        return SyncOpenSearchDB(hosts=hosts, **kwargs)

__all__ = [
    # Main clients
    "AsyncOpenSearchDB",
    "SyncOpenSearchDB",
    "OpenSearchConfig",
    "get_opensearch_config",
    "create_opensearch_client",
    
    # Types
    "SearchQuery",
    "FacetConfig",
    "SortOption",
    "IndexSettings",
    "IndexMappings",
    "BulkOperationResult",
    
    # Utilities
    "OpenSearchQueryBuilder",
    "BulkProcessor",
    "IndexManager",
    "OpenSearchStats",
    
    # Exceptions
    "OpenSearchError",
    "IndexNotFoundError",
    "DocumentNotFoundError",
    "BulkOperationError",
    "SearchQueryError",
    "ConnectionError",
    "TimeoutError",
]