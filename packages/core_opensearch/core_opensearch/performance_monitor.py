"""
Performance monitoring for OpenSearch operations
"""
import threading
import time
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from collections import defaultdict
import statistics


@dataclass
class OpenSearchMetrics:
    """Metrics for OpenSearch operations"""
    search_count: int = 0
    index_count: int = 0
    update_count: int = 0
    delete_count: int = 0
    bulk_count: int = 0
    
    search_time: float = 0.0
    index_time: float = 0.0
    update_time: float = 0.0
    delete_time: float = 0.0
    bulk_time: float = 0.0
    
    search_errors: int = 0
    index_errors: int = 0
    update_errors: int = 0
    delete_errors: int = 0
    bulk_errors: int = 0
    
    avg_search_time: float = 0.0
    avg_index_time: float = 0.0
    avg_bulk_time: float = 0.0
    
    last_search_time: Optional[datetime] = None
    last_index_time: Optional[datetime] = None
    last_bulk_time: Optional[datetime] = None
    
    search_times: List[float] = field(default_factory=list)
    index_times: List[float] = field(default_factory=list)
    bulk_times: List[float] = field(default_factory=list)
    
    def record_search(self, exec_time: float, success: bool = True):
        """Record search operation"""
        self.search_count += 1
        self.search_time += exec_time
        self.search_times.append(exec_time)
        self.last_search_time = datetime.now()
        
        if not success:
            self.search_errors += 1
        
        # Keep only recent times for percentiles
        if len(self.search_times) > 1000:
            self.search_times = self.search_times[-1000:]
        
        self.avg_search_time = self.search_time / self.search_count
    
    def record_index(self, exec_time: float, success: bool = True):
        """Record index operation"""
        self.index_count += 1
        self.index_time += exec_time
        self.index_times.append(exec_time)
        self.last_index_time = datetime.now()
        
        if not success:
            self.index_errors += 1
        
        if len(self.index_times) > 1000:
            self.index_times = self.index_times[-1000:]
        
        self.avg_index_time = self.index_time / self.index_count
    
    def record_bulk(self, exec_time: float, doc_count: int, success_count: int, error_count: int):
        """Record bulk operation"""
        self.bulk_count += 1
        self.bulk_time += exec_time
        self.bulk_times.append(exec_time)
        self.last_bulk_time = datetime.now()
        
        if error_count > 0:
            self.bulk_errors += 1
        
        if len(self.bulk_times) > 1000:
            self.bulk_times = self.bulk_times[-1000:]
        
        self.avg_bulk_time = self.bulk_time / self.bulk_count if self.bulk_count > 0 else 0
    
    @property
    def search_p95(self) -> float:
        """95th percentile search time"""
        if not self.search_times:
            return 0.0
        return float(statistics.quantiles(self.search_times, n=20)[18])
    
    @property
    def index_p95(self) -> float:
        """95th percentile index time"""
        if not self.index_times:
            return 0.0
        return float(statistics.quantiles(self.index_times, n=20)[18])
    
    @property
    def bulk_p95(self) -> float:
        """95th percentile bulk time"""
        if not self.bulk_times:
            return 0.0
        return float(statistics.quantiles(self.bulk_times, n=20)[18])
    
    @property
    def search_success_rate(self) -> float:
        """Search success rate"""
        if self.search_count == 0:
            return 100.0
        return ((self.search_count - self.search_errors) / self.search_count) * 100
    
    @property
    def index_success_rate(self) -> float:
        """Index success rate"""
        if self.index_count == 0:
            return 100.0
        return ((self.index_count - self.index_errors) / self.index_count) * 100
    
    @property
    def bulk_success_rate(self) -> float:
        """Bulk success rate"""
        if self.bulk_count == 0:
            return 100.0
        return ((self.bulk_count - self.bulk_errors) / self.bulk_count) * 100


class OpenSearchStats:
    """
    Track OpenSearch performance statistics with thread safety
    """
    
    def __init__(self, retention_hours: int = 24):
        """
        Initialize statistics tracker
        
        Args:
            retention_hours: Number of hours to retain detailed metrics
        """
        self._lock = threading.RLock()
        self._stats: Dict[str, OpenSearchMetrics] = defaultdict(OpenSearchMetrics)
        self._start_time = time.time()
        self._retention_hours = retention_hours
        self._cleanup_interval = timedelta(hours=1)
        self._last_cleanup = datetime.now()
        
        # Cluster metrics
        self.cluster_health_checks = 0
        self.last_cluster_health: Optional[Dict[str, Any]] = None
        self.cluster_status_history: List[Dict[str, Any]] = []
    
    def record_operation(
        self,
        operation_type: str,
        index_name: str,
        exec_time: float,
        success: bool = True,
        doc_count: int = 1,
        success_count: int = 0,
        error_count: int = 0
    ):
        """
        Record operation metrics
        
        Args:
            operation_type: Type of operation (search, index, update, delete, bulk)
            index_name: Target index name
            exec_time: Execution time in seconds
            success: Whether operation was successful
            doc_count: Number of documents affected
            success_count: For bulk operations, number of successful docs
            error_count: For bulk operations, number of failed docs
        """
        with self._lock:
            # Auto-cleanup old data
            self._auto_cleanup()
            
            stats = self._stats[index_name]
            
            if operation_type == "search":
                stats.record_search(exec_time, success)
            elif operation_type == "index":
                stats.record_index(exec_time, success)
            elif operation_type == "bulk":
                stats.record_bulk(exec_time, doc_count, success_count, error_count)
            elif operation_type == "update":
                stats.update_count += 1
                stats.update_time += exec_time
                if not success:
                    stats.update_errors += 1
            elif operation_type == "delete":
                stats.delete_count += 1
                stats.delete_time += exec_time
                if not success:
                    stats.delete_errors += 1
    
    def record_cluster_health(self, health_data: Dict[str, Any]):
        """Record cluster health check"""
        with self._lock:
            self.cluster_health_checks += 1
            self.last_cluster_health = health_data
            
            # Keep history (last 100 checks)
            self.cluster_status_history.append({
                "timestamp": datetime.now().isoformat(),
                "status": health_data.get("status"),
                "nodes": health_data.get("number_of_nodes"),
                "data_nodes": health_data.get("number_of_data_nodes"),
                "active_shards": health_data.get("active_shards"),
                "unassigned_shards": health_data.get("unassigned_shards"),
            })
            
            if len(self.cluster_status_history) > 100:
                self.cluster_status_history = self.cluster_status_history[-100:]
    
    def _auto_cleanup(self):
        """Automatically cleanup old metrics data"""
        now = datetime.now()
        if now - self._last_cleanup < self._cleanup_interval:
            return
        
        with self._lock:
            cutoff_time = now - timedelta(hours=self._retention_hours)
            
            # Remove old entries
            keys_to_remove = []
            for key, metrics in self._stats.items():
                last_activity = max(
                    metrics.last_search_time or datetime.min,
                    metrics.last_index_time or datetime.min,
                    metrics.last_bulk_time or datetime.min
                )
                
                if last_activity < cutoff_time:
                    keys_to_remove.append(key)
            
            for key in keys_to_remove:
                del self._stats[key]
            
            self._last_cleanup = now
    
    def get_index_stats(self, index_name: str) -> Dict[str, Any]:
        """
        Get statistics for specific index
        
        Args:
            index_name: Index name
            
        Returns:
            Index statistics
        """
        with self._lock:
            stats = self._stats.get(index_name, OpenSearchMetrics())
            
            return {
                "search": {
                    "count": stats.search_count,
                    "total_time": stats.search_time,
                    "avg_time": stats.avg_search_time,
                    "p95_time": stats.search_p95,
                    "errors": stats.search_errors,
                    "success_rate": stats.search_success_rate,
                    "last_executed": (
                        stats.last_search_time.isoformat() 
                        if stats.last_search_time else None
                    ),
                },
                "index": {
                    "count": stats.index_count,
                    "total_time": stats.index_time,
                    "avg_time": stats.avg_index_time,
                    "p95_time": stats.index_p95,
                    "errors": stats.index_errors,
                    "success_rate": stats.index_success_rate,
                    "last_executed": (
                        stats.last_index_time.isoformat() 
                        if stats.last_index_time else None
                    ),
                },
                "bulk": {
                    "count": stats.bulk_count,
                    "total_time": stats.bulk_time,
                    "avg_time": stats.avg_bulk_time,
                    "p95_time": stats.bulk_p95,
                    "errors": stats.bulk_errors,
                    "success_rate": stats.bulk_success_rate,
                    "last_executed": (
                        stats.last_bulk_time.isoformat() 
                        if stats.last_bulk_time else None
                    ),
                },
                "update": {
                    "count": stats.update_count,
                    "total_time": stats.update_time,
                    "errors": stats.update_errors,
                },
                "delete": {
                    "count": stats.delete_count,
                    "total_time": stats.delete_time,
                    "errors": stats.delete_errors,
                }
            }
    
    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """
        Get statistics for all indices
        
        Returns:
            Dictionary of index statistics
        """
        with self._lock:
            return {index: self.get_index_stats(index) for index in self._stats.keys()}
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """
        Get overall performance summary
        
        Returns:
            Performance summary
        """
        with self._lock:
            all_stats = self.get_all_stats()
            
            total_operations = 0
            total_search_time = 0.0
            total_index_time = 0.0
            total_bulk_time = 0.0
            total_errors = 0
            
            for index_stats in all_stats.values():
                total_operations += (
                    index_stats["search"]["count"] +
                    index_stats["index"]["count"] +
                    index_stats["bulk"]["count"]
                )
                
                total_search_time += index_stats["search"]["total_time"]
                total_index_time += index_stats["index"]["total_time"]
                total_bulk_time += index_stats["bulk"]["total_time"]
                
                total_errors += (
                    index_stats["search"]["errors"] +
                    index_stats["index"]["errors"] +
                    index_stats["bulk"]["errors"]
                )
            
            avg_search_time = (
                total_search_time / len(all_stats) 
                if all_stats else 0
            )
            
            error_rate = (
                (total_errors / total_operations * 100) 
                if total_operations > 0 else 0
            )
            
            return {
                "total_indices": len(all_stats),
                "total_operations": total_operations,
                "total_search_time": total_search_time,
                "total_index_time": total_index_time,
                "total_bulk_time": total_bulk_time,
                "avg_search_time": avg_search_time,
                "total_errors": total_errors,
                "error_rate_percent": error_rate,
                "cluster_health_checks": self.cluster_health_checks,
                "uptime_hours": (time.time() - self._start_time) / 3600,
            }
    
    def get_slow_queries(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get slowest queries across all indices
        
        Args:
            limit: Maximum number of queries to return
            
        Returns:
            List of slow queries
        """
        with self._lock:
            slow_queries = []
            
            for index_name, metrics in self._stats.items():
                if metrics.search_times:
                    max_time = max(metrics.search_times)
                    if max_time > 0:
                        slow_queries.append({
                            "index": index_name,
                            "max_time": max_time,
                            "avg_time": metrics.avg_search_time,
                            "p95_time": metrics.search_p95,
                            "count": metrics.search_count,
                            "success_rate": metrics.search_success_rate,
                        })
            
            slow_queries.sort(key=lambda x: x["max_time"], reverse=True)
            return slow_queries[:limit]
    
    def reset(self):
        """Reset all statistics"""
        with self._lock:
            self._stats.clear()
            self._start_time = time.time()
            self._last_cleanup = datetime.now()
            self.cluster_health_checks = 0
            self.last_cluster_health = None
            self.cluster_status_history.clear()
    
    def get_cluster_status_history(self, limit: int = 20) -> List[Dict[str, Any]]:
        """
        Get cluster status history
        
        Args:
            limit: Maximum number of history entries
            
        Returns:
            Cluster status history
        """
        with self._lock:
            return self.cluster_status_history[-limit:] if self.cluster_status_history else []