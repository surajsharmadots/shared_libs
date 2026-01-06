"""
Utility functions for OpenSearch operations
"""
import re
import hashlib
import json
import logging
import time
from typing import Any, Dict, List, Optional, Union, Tuple
from datetime import datetime, date
from decimal import Decimal

from .constants import DEFAULT_BULK_SIZE
from .exceptions import ValidationError

logger = logging.getLogger(__name__)


class OpenSearchUtils:
    """Utility class for OpenSearch operations"""
    
    # Valid index name pattern (OpenSearch restrictions)
    INDEX_NAME_PATTERN = r'^[a-z0-9][a-z0-9_-]*$'
    
    @staticmethod
    def validate_index_name(name: str) -> None:
        """
        Validate index name against OpenSearch restrictions
        
        Args:
            name: Index name to validate
            
        Raises:
            ValidationError: If index name is invalid
        """
        if not name:
            raise ValidationError("Index name cannot be empty")
        
        # Check length (OpenSearch has 255 byte limit)
        if len(name.encode('utf-8')) > 255:
            raise ValidationError(f"Index name '{name}' exceeds 255 bytes")
        
        # Check pattern
        if not re.match(OpenSearchUtils.INDEX_NAME_PATTERN, name):
            raise ValidationError(
                f"Invalid index name '{name}'. Must start with lowercase letter or number, "
                f"and contain only lowercase letters, numbers, hyphens, and underscores."
            )
        
        # Reserved names
        reserved_names = [".", ".."]
        if name in reserved_names:
            raise ValidationError(f"Index name '{name}' is reserved")
    
    @staticmethod
    def sanitize_index_name(name: str) -> str:
        """
        Sanitize index name to make it OpenSearch compatible
        
        Args:
            name: Original index name
            
        Returns:
            Sanitized index name
        """
        # Convert to lowercase
        name = name.lower()
        
        # Replace invalid characters
        name = re.sub(r'[^a-z0-9_-]', '_', name)
        
        # Ensure it starts with letter or number
        if not re.match(r'^[a-z0-9]', name):
            name = 'idx_' + name
        
        # Truncate if too long
        while len(name.encode('utf-8')) > 255:
            name = name[:-1]
        
        return name
    
    @staticmethod
    def generate_document_id(data: Dict[str, Any]) -> str:
        """
        Generate deterministic document ID from data
        
        Args:
            data: Document data
            
        Returns:
            Generated document ID
        """
        # Sort keys to ensure consistent hashing
        sorted_data = json.dumps(data, sort_keys=True, default=str)
        
        # Create SHA256 hash
        hash_obj = hashlib.sha256(sorted_data.encode())
        return hash_obj.hexdigest()[:32]  # Use first 32 chars
    
    @staticmethod
    def normalize_document(document: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize document data for OpenSearch
        
        Args:
            document: Original document
            
        Returns:
            Normalized document
        """
        normalized = {}
        
        for key, value in document.items():
            if value is None:
                continue  # Skip null values
            
            # Handle different data types
            normalized[key] = OpenSearchUtils._normalize_value(value)
        
        return normalized
    
    @staticmethod
    def _normalize_value(value: Any) -> Any:
        """Normalize a single value for OpenSearch"""
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        elif isinstance(value, Decimal):
            return float(value)
        elif isinstance(value, dict):
            return OpenSearchUtils.normalize_document(value)
        elif isinstance(value, list):
            return [OpenSearchUtils._normalize_value(item) for item in value]
        elif hasattr(value, '__dict__'):
            # Convert objects to dict
            return OpenSearchUtils.normalize_document(value.__dict__)
        else:
            return value
    
    @staticmethod
    def chunk_documents(
        documents: List[Dict[str, Any]], 
        chunk_size: int = DEFAULT_BULK_SIZE
    ) -> List[List[Dict[str, Any]]]:
        """
        Split documents into chunks for bulk operations
        
        Args:
            documents: List of documents
            chunk_size: Size of each chunk
            
        Returns:
            List of document chunks
        """
        if chunk_size <= 0:
            raise ValidationError("Chunk size must be positive")
        
        return [documents[i:i + chunk_size] for i in range(0, len(documents), chunk_size)]
    
    @staticmethod
    def build_alias_name(index_name: str, suffix: str = None) -> str:
        """
        Build alias name from index name
        
        Args:
            index_name: Base index name
            suffix: Optional suffix
            
        Returns:
            Alias name
        """
        if suffix:
            return f"{index_name}_{suffix}"
        return index_name
    
    @staticmethod
    def extract_hits(response: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract hits from OpenSearch response
        
        Args:
            response: OpenSearch search response
            
        Returns:
            List of documents
        """
        hits = response.get('hits', {}).get('hits', [])
        
        # Extract _source and include metadata
        documents = []
        for hit in hits:
            doc = hit.get('_source', {})
            
            # Include metadata
            doc['_id'] = hit.get('_id')
            doc['_index'] = hit.get('_index')
            doc['_score'] = hit.get('_score')
            
            if 'highlight' in hit:
                doc['highlight'] = hit['highlight']
            
            documents.append(doc)
        
        return documents
    
    @staticmethod
    def calculate_backoff_delay(
        attempt: int, 
        base_delay: float = 1.0, 
        max_delay: float = 30.0
    ) -> float:
        """
        Calculate exponential backoff delay
        
        Args:
            attempt: Current attempt number (0-indexed)
            base_delay: Base delay in seconds
            max_delay: Maximum delay in seconds
            
        Returns:
            Delay in seconds
        """
        delay = base_delay * (2 ** attempt)
        return min(delay, max_delay)
    
    @staticmethod
    def build_scroll_query(
        query: Dict[str, Any], 
        scroll: str = "2m",
        size: int = 100
    ) -> Dict[str, Any]:
        """
        Build query for scroll search
        
        Args:
            query: Original query
            scroll: Scroll timeout
            size: Batch size
            
        Returns:
            Scroll query
        """
        return {
            "query": query,
            "size": size,
            "sort": ["_doc"]  # Efficient scrolling
        }
    
    @staticmethod
    def format_bytes(size_bytes: int) -> str:
        """
        Format bytes to human readable string
        
        Args:
            size_bytes: Size in bytes
            
        Returns:
            Formatted string
        """
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.2f} PB"
    
    @staticmethod
    def parse_timestamp(timestamp_str: str) -> Optional[datetime]:
        """
        Parse OpenSearch timestamp string
        
        Args:
            timestamp_str: Timestamp string
            
        Returns:
            datetime object or None
        """
        try:
            # Try ISO format first
            return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        except ValueError:
            try:
                # Try epoch milliseconds
                timestamp_ms = int(timestamp_str)
                return datetime.fromtimestamp(timestamp_ms / 1000.0)
            except (ValueError, TypeError):
                return None


class RetryHandler:
    """Handler for retry operations"""
    
    @staticmethod
    def should_retry(error: Exception) -> bool:
        """
        Check if operation should be retried
        
        Args:
            error: Exception to check
            
        Returns:
            True if should retry
        """
        error_str = str(error).lower()
        
        # Retry on these errors
        retryable_errors = [
            "connection",
            "timeout",
            "temporarily_unavailable",
            "cluster_block",
            "circuit_breaking",
            "429",  # Too many requests
            "503",  # Service unavailable
        ]
        
        return any(retry_word in error_str for retry_word in retryable_errors)
    
    @staticmethod
    def retry_with_backoff(
        operation_func,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        **kwargs
    ):
        """
        Execute operation with retry and backoff
        
        Args:
            operation_func: Function to execute
            max_attempts: Maximum number of attempts
            base_delay: Base delay in seconds
            **kwargs: Arguments for operation_func
            
        Returns:
            Operation result
            
        Raises:
            Last exception if all attempts fail
        """
        last_error = None
        
        for attempt in range(max_attempts):
            try:
                return operation_func(**kwargs)
            except Exception as e:
                last_error = e
                
                if not RetryHandler.should_retry(e) or attempt == max_attempts - 1:
                    raise
                
                delay = OpenSearchUtils.calculate_backoff_delay(attempt, base_delay)
                logger.warning(
                    f"Retryable error in attempt {attempt + 1}/{max_attempts}, "
                    f"retrying in {delay:.2f}s: {e}"
                )
                
                time.sleep(delay)
        
        raise last_error