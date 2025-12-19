"""
Tests for performance monitoring
"""
import time
import pytest
from unittest.mock import Mock

from core_postgresql_db.performance_monitor import QueryStats, QueryMetrics

class TestPerformanceMonitoring:
    """Test performance monitoring"""
    
    def test_query_metrics_initialization(self):
        """Test QueryMetrics initialization"""
        metrics = QueryMetrics()
        
        assert metrics.count == 0
        assert metrics.total_time == 0.0
        assert metrics.min_time == float('inf')
        assert metrics.max_time == 0.0
        assert metrics.total_rows == 0
        assert metrics.errors == []
        assert metrics.last_executed is None
    
    def test_query_stats_initialization(self):
        """Test QueryStats initialization"""
        stats = QueryStats()
        
        snapshot = stats.snapshot()
        assert snapshot == {}
        assert stats._start_time > 0
    
    def test_record_read_success(self):
        """Test recording successful read"""
        stats = QueryStats()
        
        stats.record_read("test_query", 0.5, 10)
        
        snapshot = stats.snapshot()
        assert "test_query" in snapshot
        assert snapshot["test_query"]["count"] == 1
        assert snapshot["test_query"]["total_time"] == 0.5
        assert snapshot["test_query"]["avg_time"] == 0.5
        assert snapshot["test_query"]["total_rows"] == 10
        assert snapshot["test_query"]["error_count"] == 0
        assert snapshot["test_query"]["last_executed"] is not None
    
    def test_record_read_error(self):
        """Test recording read with error"""
        stats = QueryStats()
        
        stats.record_read("test_query", 0.5, 0, "error", "Connection failed")
        
        snapshot = stats.snapshot()
        assert snapshot["test_query"]["error_count"] == 1
    
    def test_record_write_success(self):
        """Test recording successful write"""
        stats = QueryStats()
        
        stats.record_write("insert_query", 0.2, 1)
        
        snapshot = stats.snapshot()
        assert "insert_query" in snapshot
        assert snapshot["insert_query"]["count"] == 1
        assert snapshot["insert_query"]["total_rows"] == 1
    
    def test_record_raw_sql(self):
        """Test recording raw SQL"""
        stats = QueryStats()
        
        stats.record_raw_sql(
            "raw_sql",
            0.3,
            5,
            "read",
            "SELECT * FROM users",
            None
        )
        
        snapshot = stats.snapshot()
        # Key includes query type and snippet
        expected_key = "raw_sql:read:SELECT * FROM users"
        assert expected_key in snapshot
        assert snapshot[expected_key]["total_rows"] == 5
    
    def test_multiple_recordings(self):
        """Test multiple query recordings"""
        stats = QueryStats()
        
        # Record multiple queries
        stats.record_read("query1", 0.1, 5)
        stats.record_read("query1", 0.2, 3)
        stats.record_read("query2", 0.3, 10)
        
        snapshot = stats.snapshot()
        
        assert len(snapshot) == 2
        assert snapshot["query1"]["count"] == 2
        assert snapshot["query1"]["total_time"] == 0.3
        assert snapshot["query1"]["avg_time"] == 0.15
        assert snapshot["query1"]["total_rows"] == 8
        assert snapshot["query1"]["min_time"] == 0.1
        assert snapshot["query1"]["max_time"] == 0.2
        
        assert snapshot["query2"]["count"] == 1
        assert snapshot["query2"]["total_rows"] == 10
    
    def test_reset_stats(self):
        """Test resetting statistics"""
        stats = QueryStats()
        
        stats.record_read("test_query", 0.5, 10)
        
        snapshot_before = stats.snapshot()
        assert len(snapshot_before) == 1
        
        stats.reset()
        
        snapshot_after = stats.snapshot()
        assert snapshot_after == {}
    
    def test_get_top_slow_queries(self):
        """Test getting top slow queries"""
        stats = QueryStats()
        
        # Record queries with different execution times
        stats.record_read("fast_query", 0.1, 5)
        stats.record_read("medium_query", 0.5, 10)
        stats.record_read("slow_query", 1.0, 2)
        stats.record_read("very_slow_query", 2.0, 1)
        
        top_slow = stats.get_top_slow_queries(limit=2)
        
        assert len(top_slow) == 2
        # Should be sorted by avg_time descending
        assert top_slow[0]["query"] == "very_slow_query"
        assert top_slow[0]["avg_time"] == 2.0
        assert top_slow[1]["query"] == "slow_query"
        assert top_slow[1]["avg_time"] == 1.0
    
    def test_thread_safety(self):
        """Test thread safety of QueryStats"""
        import threading
        
        stats = QueryStats()
        
        def record_queries(thread_id: int):
            for i in range(100):
                stats.record_read(f"query_{thread_id}", 0.01, 1)
        
        # Create multiple threads
        threads = []
        for i in range(10):
            thread = threading.Thread(target=record_queries, args=(i,))
            threads.append(thread)
        
        # Start threads
        for thread in threads:
            thread.start()
        
        # Wait for completion
        for thread in threads:
            thread.join()
        
        # Check results
        snapshot = stats.snapshot()
        total_count = sum(data["count"] for data in snapshot.values())
        assert total_count == 1000  # 10 threads * 100 queries each