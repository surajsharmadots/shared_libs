"""
Async OpenSearch client for e-commerce search and analytics
"""
import asyncio
import logging
import time
from typing import Any, Dict, List, Optional, Union
from contextlib import asynccontextmanager

from opensearchpy import AsyncOpenSearch
from opensearchpy.helpers import async_bulk

from .base_search import AsyncBaseSearchClient
from .config import OpenSearchConfig, get_opensearch_config
from .constants import (
    DEFAULT_TIMEOUT, MAX_RETRIES, DEFAULT_PAGE_SIZE,
    DEFAULT_BULK_SIZE, AWS_SERVICE_NAME
)
from .exceptions import (
    OpenSearchError, IndexNotFoundError, DocumentNotFoundError,
    BulkOperationError, wrap_opensearch_error
)
from .types import (
    SearchQuery, SortOption, IndexSettings, IndexMappings,
    BulkOperationResult, SearchResult, SortOrder
)
from .utils import OpenSearchUtils, RetryHandler
from .query_builder import OpenSearchQueryBuilder

logger = logging.getLogger(__name__)


class AsyncOpenSearchDB(AsyncBaseSearchClient):
    """
    Async OpenSearch client optimized for e-commerce workloads
    
    Features:
    - Full-text search with advanced querying
    - Real-time indexing and updates
    - Faceted search and aggregations
    - Autocomplete and suggestions
    - Bulk operations with retry logic
    - AWS OpenSearch Service compatible
    - Connection pooling and load balancing
    - E-commerce specific search patterns
    """
    
    def __init__(
        self,
        hosts: List[str] = None,
        config: Optional[OpenSearchConfig] = None,
        **kwargs
    ):
        """
        Initialize async OpenSearch client
        
        Args:
            hosts: List of OpenSearch hosts
            config: Pre-configured OpenSearchConfig object
            **kwargs: Additional configuration overrides
        """
        # Get configuration
        if config:
            self.config = config
        else:
            self.config = get_opensearch_config(hosts=hosts, **kwargs)
        
        # Initialize query builder
        self.query_builder = OpenSearchQueryBuilder()
        
        # Create client configuration
        client_kwargs = {
            "hosts": self.config.hosts,
            "timeout": self.config.timeout,
            "max_retries": self.config.max_retries,
            "retry_on_timeout": self.config.retry_on_timeout,
            "headers": self.config.headers,
            **self.config.extra_kwargs
        }
        
        # Add authentication if provided
        if self.config.http_auth:
            client_kwargs["http_auth"] = self.config.http_auth
        
        # SSL configuration
        client_kwargs.update({
            "use_ssl": self.config.use_ssl,
            "verify_certs": self.config.verify_certs,
            "ssl_show_warn": self.config.ssl_show_warn,
        })
        
        # Connection pooling
        client_kwargs.update({
            "connections_per_node": self.config.connection_pool_size,
            "sniff_on_start": self.config.sniff_on_start,
            "sniff_on_connection_fail": self.config.sniff_on_connection_fail,
            "sniffer_timeout": self.config.sniffer_timeout,
            "sniff_timeout": self.config.sniff_timeout,
        })
        
        # AWS SigV4 signing
        if self.config.aws_region:
            from opensearchpy import AWSV4SignerAsyncAuth
            import boto3
            
            credentials = boto3.Session(
                aws_access_key_id=self.config.aws_access_key_id,
                aws_secret_access_key=self.config.aws_secret_access_key,
                aws_session_token=self.config.aws_session_token,
                region_name=self.config.aws_region
            ).get_credentials()
            
            auth = AWSV4SignerAsyncAuth(
                credentials=credentials.get_frozen_credentials(),
                region=self.config.aws_region,
                service=self.config.aws_service
            )
            
            client_kwargs["http_auth"] = auth
        
        # Create async client
        self.async_client = AsyncOpenSearch(**client_kwargs)
        
        logger.info(f"AsyncOpenSearchDB initialized for hosts: {self.config.hosts}")
        if self.config.aws_region:
            logger.info(f"AWS region: {self.config.aws_region}, service: {self.config.aws_service}")
    
    # ============= CORE SEARCH OPERATIONS =============
    
    async def search(
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
        Search documents with advanced features
        
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
            Search results with metadata
        """
        try:
            # Build search body
            search_body = {"query": query}
            
            if size:
                search_body["size"] = min(size, 10000)
            
            if from_:
                search_body["from"] = from_
            
            if sort:
                search_body["sort"] = self._format_sort(sort)
            
            if aggs:
                search_body["aggs"] = aggs
            
            if highlight:
                search_body["highlight"] = highlight
            
            if source is not None:
                search_body["_source"] = source
            
            # Execute search
            response = await self.async_client.search(
                index=index_name,
                body=search_body
            )
            
            # Extract hits
            hits = OpenSearchUtils.extract_hits(response)
            
            return SearchResult(
                hits=hits,
                total=response["hits"]["total"]["value"],
                took=response["took"],
                aggregations=response.get("aggregations"),
                shards=response.get("_shards")
            )
            
        except Exception as e:
            error_context = f"Search failed for index {index_name}"
            raise wrap_opensearch_error(e, error_context)
    
    async def get_document(
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
        try:
            kwargs = {}
            if source is not None:
                kwargs["_source"] = source
            
            response = await self.async_client.get(
                index=index_name,
                id=document_id,
                **kwargs
            )
            
            if response.get("found"):
                doc = response.get("_source", {})
                doc["_id"] = response["_id"]
                doc["_index"] = response["_index"]
                doc["_version"] = response.get("_version")
                return doc
            
            return None
            
        except Exception as e:
            if "not_found" in str(e):
                return None
            error_context = f"Get document failed for {document_id} in index {index_name}"
            raise wrap_opensearch_error(e, error_context)
    
    async def exists(
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
        try:
            return await self.async_client.exists(
                index=index_name,
                id=document_id
            )
        except Exception as e:
            error_context = f"Exists check failed for {document_id} in index {index_name}"
            raise wrap_opensearch_error(e, error_context)
    
    # ============= DOCUMENT OPERATIONS =============
    
    async def index_document(
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
            document_id: Optional document ID
            refresh: Whether to refresh index
            
        Returns:
            Document ID
        """
        try:
            # Normalize document
            normalized_doc = OpenSearchUtils.normalize_document(document)
            
            kwargs = {
                "index": index_name,
                "body": normalized_doc,
                "refresh": refresh
            }
            
            if document_id:
                kwargs["id"] = document_id
            
            response = await self.async_client.index(**kwargs)
            
            return response.get("_id", document_id)
            
        except Exception as e:
            error_context = f"Index document failed for index {index_name}"
            raise wrap_opensearch_error(e, error_context)
    
    async def update_document(
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
        try:
            response = await self.async_client.update(
                index=index_name,
                id=document_id,
                body={"doc": updates},
                refresh=refresh
            )
            
            return response.get("result") in ["updated", "noop"]
            
        except Exception as e:
            error_context = f"Update document failed for {document_id} in index {index_name}"
            raise wrap_opensearch_error(e, error_context)
    
    async def delete_document(
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
        try:
            response = await self.async_client.delete(
                index=index_name,
                id=document_id,
                refresh=refresh
            )
            
            return response.get("result") == "deleted"
            
        except Exception as e:
            if "not_found" in str(e):
                return False
            error_context = f"Delete document failed for {document_id} in index {index_name}"
            raise wrap_opensearch_error(e, error_context)
    
    # ============= BULK OPERATIONS =============
    
    async def bulk_index(
        self,
        index_name: str,
        documents: List[Dict[str, Any]],
        refresh: bool = False,
        batch_size: int = DEFAULT_BULK_SIZE
    ) -> BulkOperationResult:
        """
        Bulk index documents with retry logic
        
        Args:
            index_name: Target index
            documents: List of documents
            refresh: Whether to refresh after each batch
            batch_size: Documents per batch
            
        Returns:
            Bulk operation result
        """
        if not documents:
            return BulkOperationResult()
        
        result = BulkOperationResult(total=len(documents))
        
        try:
            # Process in batches
            for batch in OpenSearchUtils.chunk_documents(documents, batch_size):
                batch_result = await self._process_bulk_batch(
                    index_name=index_name,
                    documents=batch,
                    refresh=refresh
                )
                
                result.successful += batch_result.successful
                result.failed += batch_result.failed
                result.errors.extend(batch_result.errors)
                result.took += batch_result.took
            
            result.has_errors = result.failed > 0
            
            return result
            
        except Exception as e:
            error_context = f"Bulk index failed for index {index_name}"
            raise wrap_opensearch_error(e, error_context)
    
    async def _process_bulk_batch(
        self,
        index_name: str,
        documents: List[Dict[str, Any]],
        refresh: bool
    ) -> BulkOperationResult:
        """Process a single batch of bulk operations"""
        result = BulkOperationResult(total=len(documents))
        
        try:
            # Prepare actions for bulk indexing
            actions = []
            for doc in documents:
                action = {
                    "_index": index_name,
                    "_source": OpenSearchUtils.normalize_document(doc)
                }
                
                # Use provided _id or generate one
                if "_id" in doc:
                    action["_id"] = doc.pop("_id")
                
                actions.append({"index": action})
            
            # Execute bulk with retry
            success, errors = await RetryHandler.retry_with_backoff(
                async_bulk,
                client=self.async_client,
                actions=actions,
                refresh=refresh,
                raise_on_error=False,
                max_attempts=3
            )
            
            result.successful = success
            result.failed = len(errors) if errors else 0
            result.errors = errors if errors else []
            
            if errors:
                result.has_errors = True
            
            return result
            
        except Exception as e:
            # Mark all documents in batch as failed
            result.failed = len(documents)
            result.has_errors = True
            result.errors.append({"batch_error": str(e)})
            return result
    
    async def bulk_update(
        self,
        index_name: str,
        updates: List[Dict[str, Any]],
        refresh: bool = False
    ) -> BulkOperationResult:
        """
        Bulk update documents
        
        Args:
            index_name: Target index
            updates: List of update operations (must contain _id)
            refresh: Whether to refresh index
            
        Returns:
            Bulk operation result
        """
        # Similar implementation to bulk_index
        # Create update actions instead of index actions
        pass
    
    async def bulk_delete(
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
        # Similar implementation to bulk_index
        # Create delete actions
        pass
    
    # ============= E-COMMERCE SPECIFIC METHODS =============
    
    async def product_search(
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
        # Build query using query builder
        query = self.query_builder.build_product_search_query(
            query_text=query_text,
            filters=filters,
            category=category,
            price_range=price_range
        )
        
        # Build aggregations for facets
        aggs = self.query_builder.build_product_facets()
        
        # Build sort
        sort = self._get_sort_option(sort_by)
        
        from_ = (page - 1) * per_page
        
        return await self.search(
            index_name="products",
            query=query,
            size=per_page,
            from_=from_,
            sort=sort,
            aggs=aggs,
            source=True
        )
    
    async def autocomplete(
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
        try:
            response = await self.async_client.search(
                index=index_name,
                body={
                    "suggest": {
                        "autocomplete": {
                            "prefix": prefix,
                            "completion": {
                                "field": f"{field}.suggest",
                                "size": size,
                                "skip_duplicates": True
                            }
                        }
                    },
                    "_source": False
                }
            )
            
            suggestions = []
            options = response.get("suggest", {}).get("autocomplete", [])
            
            for option_group in options:
                for option in option_group.get("options", []):
                    suggestions.append(option["text"])
            
            return suggestions[:size]
            
        except Exception as e:
            error_context = f"Autocomplete failed for field {field} in index {index_name}"
            raise wrap_opensearch_error(e, error_context)
    
    async def more_like_this(
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
        try:
            response = await self.async_client.search(
                index=index_name,
                body={
                    "query": {
                        "more_like_this": {
                            "fields": fields,
                            "like": [{"_id": document_id}],
                            "min_term_freq": 1,
                            "max_query_terms": 12
                        }
                    },
                    "size": max_results
                }
            )
            
            return OpenSearchUtils.extract_hits(response)
            
        except Exception as e:
            error_context = f"More like this failed for {document_id} in index {index_name}"
            raise wrap_opensearch_error(e, error_context)
    
    # ============= INDEX MANAGEMENT =============
    
    async def create_index(
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
        try:
            # Validate index name
            OpenSearchUtils.validate_index_name(index_name)
            
            # Build index body
            index_body = {}
            
            if settings:
                index_body["settings"] = settings.to_dict()
            
            if mappings:
                index_body["mappings"] = mappings.to_dict()
            
            if aliases:
                index_body["aliases"] = aliases
            
            response = await self.async_client.indices.create(
                index=index_name,
                body=index_body
            )
            
            return response.get("acknowledged", False)
            
        except Exception as e:
            if "resource_already_exists" in str(e):
                logger.warning(f"Index {index_name} already exists")
                return True
            error_context = f"Create index failed for {index_name}"
            raise wrap_opensearch_error(e, error_context)
    
    async def delete_index(
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
        try:
            response = await self.async_client.indices.delete(index=index_name)
            return response.get("acknowledged", False)
        except Exception as e:
            if "index_not_found" in str(e):
                return True  # Already deleted
            error_context = f"Delete index failed for {index_name}"
            raise wrap_opensearch_error(e, error_context)
    
    async def index_exists(
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
        try:
            return await self.async_client.indices.exists(index=index_name)
        except Exception as e:
            error_context = f"Index exists check failed for {index_name}"
            raise wrap_opensearch_error(e, error_context)
    
    # ============= UTILITY METHODS =============
    
    def _format_sort(self, sort_spec):
        """Format sort specification"""
        formatted = []
        for item in sort_spec:
            if isinstance(item, str):
                formatted.append(item)
            elif isinstance(item, SortOption):
                formatted.append(item.to_dict())
            elif isinstance(item, dict):
                formatted.append(item)
        return formatted
    
    def _get_sort_option(self, sort_by: str):
        """Get sort option for product search"""
        sort_mapping = {
            "relevance": [],  # Default by score
            "price_asc": [SortOption("price", SortOrder.ASC)],
            "price_desc": [SortOption("price", SortOrder.DESC)],
            "newest": [SortOption("created_at", SortOrder.DESC)],
            "popular": [SortOption("view_count", SortOrder.DESC)],
            "rating": [SortOption("average_rating", SortOrder.DESC)],
        }
        return sort_mapping.get(sort_by, [])
    
    async def refresh_index(self, index_name: str) -> bool:
        """Refresh index"""
        try:
            response = await self.async_client.indices.refresh(index=index_name)
            return response.get("_shards", {}).get("failed", 0) == 0
        except Exception as e:
            error_context = f"Refresh index failed for {index_name}"
            raise wrap_opensearch_error(e, error_context)
    
    async def flush_index(self, index_name: str) -> bool:
        """Flush index"""
        try:
            response = await self.async_client.indices.flush(index=index_name)
            return response.get("_shards", {}).get("failed", 0) == 0
        except Exception as e:
            error_context = f"Flush index failed for {index_name}"
            raise wrap_opensearch_error(e, error_context)
    
    async def close(self):
        """Close client connection"""
        await self.async_client.close()
        logger.info("OpenSearch client closed")
    
    # ============= ASYNC METHODS IMPLEMENTATION =============
    
    async def asearch(self, *args, **kwargs):
        """Async search alias"""
        return await self.search(*args, **kwargs)
    
    async def aget_document(self, *args, **kwargs):
        """Async get document alias"""
        return await self.get_document(*args, **kwargs)
    
    async def aindex_document(self, *args, **kwargs):
        """Async index document alias"""
        return await self.index_document(*args, **kwargs)
    
    # Implement other async methods as needed...