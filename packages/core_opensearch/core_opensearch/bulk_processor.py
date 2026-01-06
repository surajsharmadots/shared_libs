"""
Bulk operations processor for OpenSearch with retry logic
"""
import asyncio
import logging
import time
from typing import Any, Dict, List, Optional, Tuple, Union
from dataclasses import dataclass, field

from opensearchpy.helpers import async_bulk, bulk

from .constants import DEFAULT_BULK_SIZE, BULK_RETRY_ATTEMPTS, BULK_RETRY_DELAY
from .exceptions import BulkOperationError
from .utils import OpenSearchUtils, RetryHandler
from .types import BulkOperationResult

logger = logging.getLogger(__name__)


@dataclass
class BulkBatch:
    """Represents a batch of bulk operations"""
    actions: List[Dict[str, Any]]
    index_name: str
    operation_type: str  # "index", "update", "delete"
    retry_count: int = 0
    errors: List[Dict[str, Any]] = field(default_factory=list)


class BulkProcessor:
    """
    Advanced bulk operations processor with:
    - Batch processing
    - Retry logic with exponential backoff
    - Error handling and reporting
    - Progress tracking
    - Memory management
    """
    
    def __init__(
        self,
        client: Any,  # OpenSearch client (async or sync)
        is_async: bool = True,
        batch_size: int = DEFAULT_BULK_SIZE,
        max_retries: int = BULK_RETRY_ATTEMPTS,
        retry_delay: float = BULK_RETRY_DELAY,
        refresh_after_batch: bool = False
    ):
        """
        Initialize bulk processor
        
        Args:
            client: OpenSearch client instance
            is_async: Whether client is async
            batch_size: Documents per batch
            max_retries: Maximum retry attempts
            retry_delay: Initial retry delay in seconds
            refresh_after_batch: Refresh index after each batch
        """
        self.client = client
        self.is_async = is_async
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.refresh_after_batch = refresh_after_batch
        
        self.stats = {
            "total_batches": 0,
            "total_documents": 0,
            "successful_batches": 0,
            "failed_batches": 0,
            "total_retries": 0,
            "total_errors": 0,
        }
    
    async def process_bulk_index(
        self,
        index_name: str,
        documents: List[Dict[str, Any]],
        document_id_field: Optional[str] = None
    ) -> BulkOperationResult:
        """
        Process bulk index operations
        
        Args:
            index_name: Target index
            documents: List of documents to index
            document_id_field: Field to use as document ID
            
        Returns:
            Bulk operation result
        """
        if not documents:
            return BulkOperationResult()
        
        self.stats["total_documents"] += len(documents)
        
        # Split into batches
        batches = []
        for batch_docs in OpenSearchUtils.chunk_documents(documents, self.batch_size):
            actions = []
            
            for doc in batch_docs:
                action = {
                    "_index": index_name,
                    "_source": OpenSearchUtils.normalize_document(doc)
                }
                
                # Set document ID if specified
                if document_id_field and document_id_field in doc:
                    action["_id"] = doc[document_id_field]
                elif "_id" in doc:
                    action["_id"] = doc.pop("_id")
                
                actions.append({"index": action})
            
            batches.append(BulkBatch(
                actions=actions,
                index_name=index_name,
                operation_type="index"
            ))
        
        # Process batches
        result = await self._process_batches(batches)
        result.total = len(documents)
        
        return result
    
    async def process_bulk_update(
        self,
        index_name: str,
        updates: List[Dict[str, Any]],
        document_id_field: str = "_id"
    ) -> BulkOperationResult:
        """
        Process bulk update operations
        
        Args:
            index_name: Target index
            updates: List of update operations (must contain ID field)
            document_id_field: Field containing document ID
            
        Returns:
            Bulk operation result
        """
        if not updates:
            return BulkOperationResult()
        
        batches = []
        for batch_updates in OpenSearchUtils.chunk_documents(updates, self.batch_size):
            actions = []
            
            for update_doc in batch_updates:
                if document_id_field not in update_doc:
                    raise ValueError(f"Document missing {document_id_field} field")
                
                doc_id = update_doc[document_id_field]
                update_source = {k: v for k, v in update_doc.items() if k != document_id_field}
                
                actions.append({
                    "_op_type": "update",
                    "_index": index_name,
                    "_id": doc_id,
                    "doc": update_source
                })
            
            batches.append(BulkBatch(
                actions=actions,
                index_name=index_name,
                operation_type="update"
            ))
        
        return await self._process_batches(batches)
    
    async def process_bulk_delete(
        self,
        index_name: str,
        document_ids: List[str]
    ) -> BulkOperationResult:
        """
        Process bulk delete operations
        
        Args:
            index_name: Target index
            document_ids: List of document IDs to delete
            
        Returns:
            Bulk operation result
        """
        if not document_ids:
            return BulkOperationResult()
        
        batches = []
        for batch_ids in OpenSearchUtils.chunk_documents(document_ids, self.batch_size):
            actions = []
            
            for doc_id in batch_ids:
                actions.append({
                    "_op_type": "delete",
                    "_index": index_name,
                    "_id": doc_id
                })
            
            batches.append(BulkBatch(
                actions=actions,
                index_name=index_name,
                operation_type="delete"
            ))
        
        return await self._process_batches(batches)
    
    async def _process_batches(
        self,
        batches: List[BulkBatch]
    ) -> BulkOperationResult:
        """
        Process list of batches
        
        Args:
            batches: List of batches to process
            
        Returns:
            Bulk operation result
        """
        result = BulkOperationResult()
        total_batches = len(batches)
        
        logger.info(f"Processing {total_batches} batches with {self.batch_size} documents each")
        
        for i, batch in enumerate(batches, 1):
            self.stats["total_batches"] += 1
            
            logger.debug(f"Processing batch {i}/{total_batches} for index {batch.index_name}")
            
            batch_result = await self._process_batch_with_retry(batch)
            
            # Update overall result
            result.successful += batch_result.successful
            result.failed += batch_result.failed
            result.errors.extend(batch_result.errors)
            result.took += batch_result.took
            
            # Update stats
            if batch_result.has_errors:
                self.stats["failed_batches"] += 1
                self.stats["total_errors"] += batch_result.failed
            else:
                self.stats["successful_batches"] += 1
            
            # Refresh if configured
            if self.refresh_after_batch and not batch_result.has_errors:
                await self._refresh_index(batch.index_name)
        
        result.has_errors = result.failed > 0
        
        logger.info(
            f"Bulk processing completed: "
            f"{result.successful} successful, "
            f"{result.failed} failed, "
            f"took {result.took:.2f}s"
        )
        
        return result
    
    async def _process_batch_with_retry(
        self,
        batch: BulkBatch
    ) -> BulkOperationResult:
        """
        Process a single batch with retry logic
        
        Args:
            batch: Batch to process
            
        Returns:
            Batch processing result
        """
        batch_result = BulkOperationResult(total=len(batch.actions))
        
        for attempt in range(self.max_retries):
            try:
                if self.is_async:
                    success, errors = await async_bulk(
                        client=self.client,
                        actions=batch.actions,
                        refresh=False,
                        raise_on_error=False,
                        stats_only=False
                    )
                else:
                    success, errors = bulk(
                        client=self.client,
                        actions=batch.actions,
                        refresh=False,
                        raise_on_error=False,
                        stats_only=False
                    )
                
                batch_result.successful = success
                batch_result.failed = len(errors) if errors else 0
                batch_result.errors = errors if errors else []
                batch_result.has_errors = batch_result.failed > 0
                
                # Update retry count
                if attempt > 0:
                    self.stats["total_retries"] += 1
                    batch.retry_count = attempt
                
                # Log results
                if batch_result.has_errors:
                    logger.warning(
                        f"Batch completed with {batch_result.failed} errors "
                        f"(attempt {attempt + 1}/{self.max_retries})"
                    )
                    
                    # Retry only specific errors
                    if self._should_retry_batch(errors) and attempt < self.max_retries - 1:
                        # Prepare batch for retry (remove successful actions)
                        batch = self._prepare_batch_for_retry(batch, errors)
                        if not batch.actions:
                            break  # All succeeded after retry filtering
                        
                        # Wait before retry
                        wait_time = self.retry_delay * (2 ** attempt)
                        logger.info(f"Retrying batch in {wait_time:.2f}s")
                        
                        if self.is_async:
                            await asyncio.sleep(wait_time)
                        else:
                            time.sleep(wait_time)
                        
                        continue
                else:
                    logger.debug(f"Batch completed successfully (attempt {attempt + 1})")
                
                break  # Success or no retry needed
                
            except Exception as e:
                logger.error(f"Batch processing failed on attempt {attempt + 1}: {e}")
                
                if attempt == self.max_retries - 1:
                    # Mark all as failed on final attempt
                    batch_result.failed = len(batch.actions)
                    batch_result.has_errors = True
                    batch_result.errors.append({"batch_error": str(e)})
                else:
                    # Wait and retry
                    wait_time = self.retry_delay * (2 ** attempt)
                    if self.is_async:
                        await asyncio.sleep(wait_time)
                    else:
                        time.sleep(wait_time)
        
        return batch_result
    
    def _should_retry_batch(self, errors: List[Dict[str, Any]]) -> bool:
        """
        Check if batch should be retried based on errors
        
        Args:
            errors: List of errors from bulk operation
            
        Returns:
            True if batch should be retried
        """
        if not errors:
            return False
        
        # Retry on these error types
        retryable_errors = [
            "version_conflict",
            "document_missing",
            "cluster_block",
            "circuit_breaking",
            "429",  # Too many requests
            "503",  # Service unavailable
            "timeout",
            "connection",
        ]
        
        for error in errors:
            error_type = error.get("error", {}).get("type", "")
            error_reason = str(error.get("error", {})).lower()
            
            for retryable in retryable_errors:
                if retryable in error_type.lower() or retryable in error_reason:
                    return True
        
        return False
    
    def _prepare_batch_for_retry(
        self,
        batch: BulkBatch,
        errors: List[Dict[str, Any]]
    ) -> BulkBatch:
        """
        Prepare batch for retry by removing successful actions
        
        Args:
            batch: Original batch
            errors: List of errors
            
        Returns:
            New batch with only failed actions
        """
        failed_actions = []
        error_indices = set()
        
        # Extract indices of failed actions
        for error in errors:
            if "index" in error:
                index_info = error["index"]
                if "_id" in index_info:
                    # Find the action with this ID
                    for i, action in enumerate(batch.actions):
                        action_data = list(action.values())[0]  # Get dict inside
                        if action_data.get("_id") == index_info["_id"]:
                            error_indices.add(i)
                            break
        
        # Create new batch with only failed actions
        for i, action in enumerate(batch.actions):
            if i in error_indices:
                failed_actions.append(action)
        
        return BulkBatch(
            actions=failed_actions,
            index_name=batch.index_name,
            operation_type=batch.operation_type,
            retry_count=batch.retry_count + 1
        )
    
    async def _refresh_index(self, index_name: str):
        """Refresh index"""
        try:
            if self.is_async:
                await self.client.indices.refresh(index=index_name)
            else:
                self.client.indices.refresh(index=index_name)
        except Exception as e:
            logger.warning(f"Failed to refresh index {index_name}: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get processor statistics
        
        Returns:
            Processor statistics
        """
        return {
            **self.stats,
            "batch_size": self.batch_size,
            "max_retries": self.max_retries,
            "is_async": self.is_async,
        }
    
    def reset_stats(self):
        """Reset processor statistics"""
        self.stats = {
            "total_batches": 0,
            "total_documents": 0,
            "successful_batches": 0,
            "failed_batches": 0,
            "total_retries": 0,
            "total_errors": 0,
        }