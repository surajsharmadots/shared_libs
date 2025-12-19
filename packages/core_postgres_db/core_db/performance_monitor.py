"""
Performance monitoring for database queries
"""
import threading
import time
from typing import Dict, Any, Optional, List
from datetime import datetime
from dataclasses import dataclass, field
from collections import defaultdict

@dataclass
class QueryMetrics:
    """Metrics for a single query type"""
    count: int = 0
    total_time: float = 0.0
    min_time: float = float('inf')
    max_time: float = 0.0
    total_rows: int = 0
    errors: List[str] = field(default_factory=list)
    last_executed: Optional[datetime] = None

class QueryStats:
    """Track query performance statistics"""
    
    def __init__(self):
        self._lock = threading.Lock()
        self._stats: Dict[str, QueryMetrics] = defaultdict(QueryMetrics)
        self._start_time = time.time()
    
    def record_read(
        self,
        key: str,
        exec_time: float,
        rows: int,
        status: str = "success",
        error: Optional[str] = None
    ):
        """Record read query metrics"""
        with self._lock:
            stats = self._stats[key]
            stats.count += 1
            stats.total_time += exec_time
            stats.min_time = min(stats.min_time, exec_time)
            stats.max_time = max(stats.max_time, exec_time)
            stats.total_rows += rows
            stats.last_executed = datetime.now()
            
            if status == "error" and error:
                stats.errors.append(error)
    
    def record_write(
        self,
        key: str,
        exec_time: float,
        rows: int,
        status: str = "success",
        error: Optional[str] = None
    ):
        """Record write query metrics"""
        self.record_read(key, exec_time, rows, status, error)
    
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
        full_key = f"{key}:{query_type}:{query_snippet}"
        status = "error" if error else "success"
        self.record_read(full_key, exec_time, rows, status, error)
    
    def snapshot(self) -> Dict[str, Dict[str, Any]]:
        """Get current statistics snapshot"""
        with self._lock:
            snapshot = {}
            for key, metrics in self._stats.items():
                avg_time = metrics.total_time / metrics.count if metrics.count > 0 else 0
                snapshot[key] = {
                    "count": metrics.count,
                    "total_time": metrics.total_time,
                    "avg_time": avg_time,
                    "min_time": metrics.min_time if metrics.min_time != float('inf') else 0,
                    "max_time": metrics.max_time,
                    "total_rows": metrics.total_rows,
                    "avg_rows": metrics.total_rows / metrics.count if metrics.count > 0 else 0,
                    "error_count": len(metrics.errors),
                    "last_executed": metrics.last_executed.isoformat() if metrics.last_executed else None,
                    "uptime": time.time() - self._start_time
                }
            return snapshot
    
    def reset(self):
        """Reset all statistics"""
        with self._lock:
            self._stats.clear()
            self._start_time = time.time()
    
    def get_top_slow_queries(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get top slowest queries"""
        snapshot = self.snapshot()
        slow_queries = []
        
        for key, stats in snapshot.items():
            if stats["count"] > 0:
                slow_queries.append({
                    "query": key,
                    "avg_time": stats["avg_time"],
                    "count": stats["count"],
                    "total_time": stats["total_time"]
                })
        
        slow_queries.sort(key=lambda x: x["avg_time"], reverse=True)
        return slow_queries[:limit]