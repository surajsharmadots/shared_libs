# packages/core_postgres_db/core_postgres_db/performance_monitor.py
"""
Performance monitoring for database queries - Sonarqube compliant
"""
import threading
import time
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from collections import defaultdict
import statistics


@dataclass
class QueryMetrics:
    """Metrics for a single query type"""
    count: int = 0
    total_time: float = 0.0
    min_time: float = float('inf')
    max_time: float = 0.0
    total_rows: int = 0
    errors: List[str] = field(default_factory=list)
    success_count: int = 0
    error_count: int = 0
    last_executed: Optional[datetime] = None
    execution_times: List[float] = field(default_factory=list)
    
    @property
    def avg_time(self) -> float:
        """Calculate average execution time"""
        return self.total_time / self.count if self.count > 0 else 0.0
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate"""
        total = self.success_count + self.error_count
        return (self.success_count / total * 100) if total > 0 else 100.0
    
    @property
    def avg_rows(self) -> float:
        """Calculate average rows affected"""
        return self.total_rows / self.count if self.count > 0 else 0.0
    
    @property
    def median_time(self) -> float:
        """Calculate median execution time"""
        if not self.execution_times:
            return 0.0
        return float(statistics.median(self.execution_times))
    
    @property
    def p95_time(self) -> float:
        """Calculate 95th percentile execution time"""
        if not self.execution_times:
            return 0.0
        return float(statistics.quantiles(self.execution_times, n=20)[18])  # 95th percentile


class QueryStats:
    """Track query performance statistics with thread safety"""
    
    def __init__(self, retention_hours: int = 24):
        """
        Initialize query statistics tracker
        
        Args:
            retention_hours: Number of hours to retain detailed metrics
        """
        self._lock = threading.RLock()
        self._stats: Dict[str, QueryMetrics] = defaultdict(QueryMetrics)
        self._start_time = time.time()
        self._retention_hours = retention_hours
        self._cleanup_interval = timedelta(hours=1)
        self._last_cleanup = datetime.now()
    
    def record_read(
        self,
        key: str,
        exec_time: float,
        rows: int,
        status: str = "success",
        error: Optional[str] = None
    ):
        """Record read query metrics"""
        self._record_metrics(key, exec_time, rows, status, error, "read")
    
    def record_write(
        self,
        key: str,
        exec_time: float,
        rows: int,
        status: str = "success",
        error: Optional[str] = None
    ):
        """Record write query metrics"""
        self._record_metrics(key, exec_time, rows, status, error, "write")
    
    def record_raw_sql(
        self,
        key: str,
        exec_time: float,
        rows: int,
        query_type: str,
        query_snippet: str,
        error: Optional[str] = None
    ):
        """Record raw SQL query metrics"""
        full_key = f"raw_sql:{query_type}:{query_snippet[:50]}"
        status = "error" if error else "success"
        self._record_metrics(full_key, exec_time, rows, status, error, query_type)
    
    def _record_metrics(
        self,
        key: str,
        exec_time: float,
        rows: int,
        status: str,
        error: Optional[str],
        operation: str
    ):
        """Internal method to record metrics"""
        with self._lock:
            # Auto-cleanup old data
            self._auto_cleanup()
            
            stats = self._stats[key]
            stats.count += 1
            stats.total_time += exec_time
            stats.min_time = min(stats.min_time, exec_time)
            stats.max_time = max(stats.max_time, exec_time)
            stats.total_rows += rows
            stats.last_executed = datetime.now()
            stats.execution_times.append(exec_time)
            
            if status == "success":
                stats.success_count += 1
            else:
                stats.error_count += 1
                if error:
                    stats.errors.append(f"{operation}: {error}")
            
            # Keep only recent execution times for percentile calculations
            if len(stats.execution_times) > 1000:
                stats.execution_times = stats.execution_times[-1000:]
    
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
                if metrics.last_executed and metrics.last_executed < cutoff_time:
                    keys_to_remove.append(key)
            
            for key in keys_to_remove:
                del self._stats[key]
            
            self._last_cleanup = now
    
    def snapshot(self) -> Dict[str, Dict[str, Any]]:
        """Get current statistics snapshot"""
        with self._lock:
            snapshot = {}
            for key, metrics in self._stats.items():
                snapshot[key] = {
                    "count": metrics.count,
                    "total_time": metrics.total_time,
                    "avg_time": metrics.avg_time,
                    "median_time": metrics.median_time,
                    "p95_time": metrics.p95_time,
                    "min_time": metrics.min_time if metrics.min_time != float('inf') else 0,
                    "max_time": metrics.max_time,
                    "total_rows": metrics.total_rows,
                    "avg_rows": metrics.avg_rows,
                    "success_count": metrics.success_count,
                    "error_count": metrics.error_count,
                    "success_rate": metrics.success_rate,
                    "last_executed": (
                        metrics.last_executed.isoformat() 
                        if metrics.last_executed else None
                    ),
                    "uptime": time.time() - self._start_time,
                    "recent_errors": metrics.errors[-10:] if metrics.errors else []
                }
            return snapshot
    
    def reset(self):
        """Reset all statistics"""
        with self._lock:
            self._stats.clear()
            self._start_time = time.time()
            self._last_cleanup = datetime.now()
    
    def get_top_slow_queries(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get top slowest queries by average time"""
        snapshot = self.snapshot()
        slow_queries = []
        
        for key, stats in snapshot.items():
            if stats["count"] > 0:
                slow_queries.append({
                    "query": key,
                    "avg_time": stats["avg_time"],
                    "median_time": stats["median_time"],
                    "p95_time": stats["p95_time"],
                    "count": stats["count"],
                    "total_time": stats["total_time"],
                    "success_rate": stats["success_rate"]
                })
        
        slow_queries.sort(key=lambda x: x["p95_time"], reverse=True)
        return slow_queries[:limit]
    
    def get_top_error_queries(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get queries with highest error rates"""
        snapshot = self.snapshot()
        error_queries = []
        
        for key, stats in snapshot.items():
            if stats["error_count"] > 0:
                error_queries.append({
                    "query": key,
                    "error_count": stats["error_count"],
                    "success_rate": stats["success_rate"],
                    "total_count": stats["count"],
                    "recent_errors": stats["recent_errors"]
                })
        
        error_queries.sort(key=lambda x: x["error_count"], reverse=True)
        return error_queries[:limit]
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get overall performance summary"""
        snapshot = self.snapshot()
        
        total_queries = 0
        total_time = 0.0
        total_rows = 0
        total_errors = 0
        
        for stats in snapshot.values():
            total_queries += stats["count"]
            total_time += stats["total_time"]
            total_rows += stats["total_rows"]
            total_errors += stats["error_count"]
        
        avg_time_per_query = total_time / total_queries if total_queries > 0 else 0
        error_rate = (total_errors / total_queries * 100) if total_queries > 0 else 0
        
        return {
            "total_queries": total_queries,
            "total_execution_time": total_time,
            "avg_time_per_query": avg_time_per_query,
            "total_rows_affected": total_rows,
            "total_errors": total_errors,
            "error_rate_percent": error_rate,
            "unique_query_types": len(snapshot),
            "uptime_hours": (time.time() - self._start_time) / 3600
        }