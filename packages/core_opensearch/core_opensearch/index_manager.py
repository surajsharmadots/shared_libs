"""
Index management utilities for OpenSearch
"""
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime

from .constants import DEFAULT_SHARDS, DEFAULT_REPLICAS
from .exceptions import IndexNotFoundError
from .types import IndexSettings
from .utils import OpenSearchUtils

logger = logging.getLogger(__name__)


class IndexManager:
    """
    Advanced index management for OpenSearch
    
    Features:
    - Index lifecycle management
    - Index templates
    - Index aliases and rollover
    - Index optimization
    - Reindex operations
    - Snapshot management
    """
    
    def __init__(self, client: Any, is_async: bool = True):
        """
        Initialize index manager
        
        Args:
            client: OpenSearch client instance
            is_async: Whether client is async
        """
        self.client = client
        self.is_async = is_async
    
    async def create_index_with_settings(
        self,
        index_name: str,
        mappings: Optional[Dict[str, Any]] = None,
        settings: Optional[Dict[str, Any]] = None,
        aliases: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Create index with custom settings
        
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
                index_body["settings"] = settings
            
            if mappings:
                index_body["mappings"] = mappings
            
            if aliases:
                index_body["aliases"] = aliases
            
            # Default settings if not provided
            if "settings" not in index_body:
                default_settings = IndexSettings().to_dict()
                index_body["settings"] = default_settings
            
            if self.is_async:
                response = await self.client.indices.create(
                    index=index_name,
                    body=index_body
                )
            else:
                response = self.client.indices.create(
                    index=index_name,
                    body=index_body
                )
            
            acknowledged = response.get("acknowledged", False)
            
            if acknowledged:
                logger.info(f"Index '{index_name}' created successfully")
            else:
                logger.warning(f"Index '{index_name}' creation not acknowledged")
            
            return acknowledged
            
        except Exception as e:
            if "resource_already_exists" in str(e):
                logger.warning(f"Index '{index_name}' already exists")
                return True
            
            error_context = f"Failed to create index '{index_name}'"
            logger.error(f"{error_context}: {e}")
            raise
    
    async def create_time_series_index(
        self,
        index_prefix: str,
        date_format: str = "yyyy.MM.dd",
        mappings: Optional[Dict[str, Any]] = None,
        settings: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Create time-series index with date suffix
        
        Args:
            index_prefix: Index name prefix
            date_format: Date format for suffix
            mappings: Index mappings
            settings: Index settings
            
        Returns:
            Created index name
        """
        # Generate index name with date suffix
        current_date = datetime.utcnow().strftime("%Y.%m.%d")
        index_name = f"{index_prefix}-{current_date}"
        
        # Create index
        success = await self.create_index_with_settings(
            index_name=index_name,
            mappings=mappings,
            settings=settings
        )
        
        if success:
            # Create alias pointing to this index
            await self.create_alias(
                alias_name=index_prefix,
                index_name=index_name
            )
            
            logger.info(f"Time-series index '{index_name}' created with alias '{index_prefix}'")
        
        return index_name if success else ""
    
    async def create_alias(
        self,
        alias_name: str,
        index_name: str,
        filter: Optional[Dict[str, Any]] = None,
        routing: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Create index alias
        
        Args:
            alias_name: Alias name
            index_name: Target index
            filter: Optional filter
            routing: Optional routing configuration
            
        Returns:
            True if successful
        """
        try:
            actions = [{"add": {"index": index_name, "alias": alias_name}}]
            
            if filter:
                actions[0]["add"]["filter"] = filter
            
            if routing:
                actions[0]["add"]["routing"] = routing
            
            if self.is_async:
                response = await self.client.indices.update_aliases(body={"actions": actions})
            else:
                response = self.client.indices.update_aliases(body={"actions": actions})
            
            return response.get("acknowledged", False)
            
        except Exception as e:
            logger.error(f"Failed to create alias '{alias_name}' for index '{index_name}': {e}")
            return False
    
    async def get_index_aliases(self, index_name: str) -> List[str]:
        """
        Get aliases for an index
        
        Args:
            index_name: Index name
            
        Returns:
            List of alias names
        """
        try:
            if self.is_async:
                response = await self.client.indices.get_alias(index=index_name)
            else:
                response = self.client.indices.get_alias(index=index_name)
            
            aliases = []
            for index_data in response.values():
                if "aliases" in index_data:
                    aliases.extend(list(index_data["aliases"].keys()))
            
            return aliases
            
        except Exception as e:
            if "index_not_found" in str(e):
                raise IndexNotFoundError(index_name, e)
            logger.error(f"Failed to get aliases for index '{index_name}': {e}")
            return []
    
    async def reindex(
        self,
        source_index: str,
        dest_index: str,
        query: Optional[Dict[str, Any]] = None,
        batch_size: int = 1000,
        wait_for_completion: bool = True
    ) -> Dict[str, Any]:
        """
        Reindex documents from source to destination
        
        Args:
            source_index: Source index name
            dest_index: Destination index name
            query: Optional query to filter documents
            batch_size: Documents per batch
            wait_for_completion: Wait for reindex to complete
            
        Returns:
            Reindex response
        """
        try:
            reindex_body = {
                "source": {
                    "index": source_index,
                    "size": batch_size
                },
                "dest": {
                    "index": dest_index
                }
            }
            
            if query:
                reindex_body["source"]["query"] = query
            
            if self.is_async:
                response = await self.client.reindex(
                    body=reindex_body,
                    wait_for_completion=wait_for_completion
                )
            else:
                response = self.client.reindex(
                    body=reindex_body,
                    wait_for_completion=wait_for_completion
                )
            
            logger.info(
                f"Reindex from '{source_index}' to '{dest_index}' completed: "
                f"{response.get('total', 0)} total, "
                f"{response.get('created', 0)} created, "
                f"{response.get('updated', 0)} updated"
            )
            
            return response
            
        except Exception as e:
            error_context = f"Reindex failed from '{source_index}' to '{dest_index}'"
            logger.error(f"{error_context}: {e}")
            raise
    
    async def optimize_index(
        self,
        index_name: str,
        max_num_segments: int = 1,
        only_expunge_deletes: bool = False,
        flush: bool = True
    ) -> bool:
        """
        Optimize index (force merge segments)
        
        Args:
            index_name: Index name
            max_num_segments: Maximum number of segments
            only_expunge_deletes: Only expunge deleted documents
            flush: Flush after optimization
            
        Returns:
            True if successful
        """