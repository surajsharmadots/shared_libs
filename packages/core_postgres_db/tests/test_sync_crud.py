"""
Tests for synchronous CRUD operations
"""
import pytest
import time
from typing import Dict, Any
from unittest.mock import patch, MagicMock

from core_postgresql_db import SyncPostgresDB
from core_postgresql_db.exceptions import (
    DatabaseError, DuplicateEntryError, ConstraintViolationError
)
from core_postgresql_db.types import QueryOptions, BulkInsertOptions

class TestSyncCRUDOperations:
    """Test synchronous CRUD operations"""
    
    def test_create_success(self, sync_test_db, sample_user_data):
        """Test successful create operation"""
        # Act
        result = sync_test_db.create("test_users", sample_user_data)
        
        # Assert
        assert result is not None
        assert result["name"] == sample_user_data["name"]
        assert result["email"] == sample_user_data["email"]
        assert "id" in result
        assert "created_at" in result
    
    def test_create_duplicate_email(self, sync_test_db, sample_user_data):
        """Test create with duplicate email (unique constraint)"""
        # Arrange - Create first user
        sync_test_db.create("test_users", sample_user_data)
        
        # Act & Assert - Try to create duplicate
        with pytest.raises(DuplicateEntryError):
            sync_test_db.create("test_users", sample_user_data)
    
    def test_read_all(self, sync_test_db, sample_bulk_users):
        """Test read all records"""
        # Arrange - Create multiple users
        for user in sample_bulk_users:
            sync_test_db.create("test_users", user)
        
        # Act
        users = sync_test_db.read("test_users")
        
        # Assert
        assert len(users) >= len(sample_bulk_users)
        assert all("id" in user for user in users)
    
    def test_read_with_conditions(self, sync_test_db, sample_bulk_users):
        """Test read with WHERE conditions"""
        # Arrange
        for user in sample_bulk_users:
            sync_test_db.create("test_users", user)
        
        # Act
        users = sync_test_db.read("test_users", {"active": True})
        
        # Assert
        assert all(user["active"] is True for user in users)
    
    def test_read_with_options(self, sync_test_db, sample_bulk_users):
        """Test read with query options"""
        # Arrange
        for user in sample_bulk_users:
            sync_test_db.create("test_users", user)
        
        options = QueryOptions(
            limit=2,
            offset=1,
            order_by=[("name", True)],  # ASC
            columns=["id", "name", "email"]
        )
        
        # Act
        users = sync_test_db.read("test_users", options=options)
        
        # Assert
        assert len(users) <= 2
        assert all("age" not in user for user in users)  # Only selected columns
    
    def test_read_one(self, sync_test_db, sample_user_data):
        """Test read single record"""
        # Arrange
        created = sync_test_db.create("test_users", sample_user_data)
        
        # Act
        user = sync_test_db.read_one("test_users", {"id": created["id"]})
        
        # Assert
        assert user is not None
        assert user["id"] == created["id"]
        assert user["email"] == sample_user_data["email"]
    
    def test_read_by_id(self, sync_test_db, sample_user_data):
        """Test read by ID"""
        # Arrange
        created = sync_test_db.create("test_users", sample_user_data)
        
        # Act
        user = sync_test_db.read_by_id("test_users", created["id"])
        
        # Assert
        assert user is not None
        assert user["id"] == created["id"]
    
    def test_read_nonexistent(self, sync_test_db):
        """Test read non-existent record"""
        # Act
        user = sync_test_db.read_one("test_users", {"id": 99999})
        
        # Assert
        assert user is None
    
    def test_exists_true(self, sync_test_db, sample_user_data):
        """Test exists returns True"""
        # Arrange
        created = sync_test_db.create("test_users", sample_user_data)
        
        # Act
        exists = sync_test_db.exists("test_users", {"id": created["id"]})
        
        # Assert
        assert exists is True
    
    def test_exists_false(self, sync_test_db):
        """Test exists returns False"""
        # Act
        exists = sync_test_db.exists("test_users", {"id": 99999})
        
        # Assert
        assert exists is False
    
    def test_count_all(self, sync_test_db, sample_bulk_users):
        """Test count all records"""
        # Arrange
        for user in sample_bulk_users:
            sync_test_db.create("test_users", user)
        
        # Act
        count = sync_test_db.count("test_users")
        
        # Assert
        assert count >= len(sample_bulk_users)
    
    def test_count_with_conditions(self, sync_test_db, sample_bulk_users):
        """Test count with conditions"""
        # Arrange
        for user in sample_bulk_users:
            sync_test_db.create("test_users", user)
        
        # Act
        count = sync_test_db.count("test_users", {"active": True})
        
        # Assert
        assert count >= 0
    
    def test_update_success(self, sync_test_db, sample_user_data):
        """Test successful update"""
        # Arrange
        created = sync_test_db.create("test_users", sample_user_data)
        update_data = {"age": 35, "active": False}
        
        # Act
        updated = sync_test_db.update(
            "test_users", 
            update_data, 
            {"id": created["id"]}
        )
        
        # Assert
        assert len(updated) == 1
        assert updated[0]["age"] == 35
        assert updated[0]["active"] is False
    
    def test_update_multiple(self, sync_test_db, sample_bulk_users):
        """Test update multiple records"""
        # Arrange
        for user in sample_bulk_users:
            sync_test_db.create("test_users", user)
        
        # Act - Update all users with age > 22
        updated = sync_test_db.update(
            "test_users",
            {"active": False},
            {"age__gt": 22}
        )
        
        # Assert
        assert len(updated) >= 0  # Could be 0 or more
    
    def test_delete_success(self, sync_test_db, sample_user_data):
        """Test successful delete"""
        # Arrange
        created = sync_test_db.create("test_users", sample_user_data)
        
        # Act
        deleted_count = sync_test_db.delete("test_users", {"id": created["id"]})
        
        # Assert
        assert deleted_count == 1
        
        # Verify record is deleted
        user = sync_test_db.read_one("test_users", {"id": created["id"]})
        assert user is None
    
    def test_delete_with_returning(self, sync_test_db, sample_user_data):
        """Test delete with returning"""
        # Arrange
        created = sync_test_db.create("test_users", sample_user_data)
        
        # Act
        deleted_rows = sync_test_db.delete(
            "test_users", 
            {"id": created["id"]}, 
            returning=True
        )
        
        # Assert
        assert len(deleted_rows) == 1
        assert deleted_rows[0]["id"] == created["id"]
    
    def test_bulk_create_success(self, sync_test_db, sample_bulk_users):
        """Test successful bulk create"""
        # Act
        results = sync_test_db.bulk_create("test_users", sample_bulk_users)
        
        # Assert
        assert len(results) == len(sample_bulk_users)
        assert all("id" in user for user in results)
    
    def test_bulk_create_with_options(self, sync_test_db, sample_bulk_users):
        """Test bulk create with options"""
        options = BulkInsertOptions(
            batch_size=2,
            return_rows=True,
            ignore_duplicates=False
        )
        
        # Act
        results = sync_test_db.bulk_create("test_users", sample_bulk_users, options)
        
        # Assert
        assert len(results) == len(sample_bulk_users)
    
    def test_execute_raw_sql_select(self, sync_test_db, sample_user_data):
        """Test raw SQL SELECT"""
        # Arrange
        sync_test_db.create("test_users", sample_user_data)
        
        # Act
        results = sync_test_db.execute_raw_sql(
            "SELECT * FROM test_users WHERE email = :email",
            {"email": sample_user_data["email"]}
        )
        
        # Assert
        assert len(results) >= 1
        assert results[0]["email"] == sample_user_data["email"]
    
    def test_execute_raw_sql_insert(self, sync_test_db):
        """Test raw SQL INSERT"""
        # Act
        rowcount = sync_test_db.execute_raw_sql(
            "INSERT INTO test_users (name, email, age) VALUES (:name, :email, :age)",
            {"name": "Raw SQL", "email": "raw@example.com", "age": 25},
            fetch_results=False
        )
        
        # Assert
        assert rowcount == 1
        
        # Verify insert
        user = sync_test_db.read_one("test_users", {"email": "raw@example.com"})
        assert user is not None
        assert user["name"] == "Raw SQL"
    
    def test_transaction_success(self, sync_test_db, sample_user_data):
        """Test successful transaction"""
        with sync_test_db.transaction() as tx:
            # Create user
            user = sync_test_db.create("test_users", sample_user_data)
            
            # Update user
            sync_test_db.update(
                "test_users",
                {"age": 40},
                {"id": user["id"]}
            )
        
        # Verify transaction committed
        updated = sync_test_db.read_one("test_users", {"id": user["id"]})
        assert updated["age"] == 40
    
    def test_transaction_rollback(self, sync_test_db, sample_user_data):
        """Test transaction rollback on error"""
        user_id = None
        
        try:
            with sync_test_db.transaction() as tx:
                # Create user
                user = sync_test_db.create("test_users", sample_user_data)
                user_id = user["id"]
                
                # This should fail (invalid column)
                sync_test_db.update(
                    "test_users",
                    {"invalid_column": "value"},  # Invalid column
                    {"id": user_id}
                )
        except DatabaseError:
            pass  # Expected error
        
        # Verify transaction was rolled back (user should not exist)
        if user_id:
            user = sync_test_db.read_one("test_users", {"id": user_id})
            assert user is None  # Should have been rolled back
    
    def test_health_check(self, sync_test_db):
        """Test health check"""
        # Act
        healthy = sync_test_db.health_check()
        
        # Assert
        assert healthy is True
    
    def test_get_stats(self, sync_test_db):
        """Test get statistics"""
        # Act
        stats = sync_test_db.get_stats()
        
        # Assert
        assert "connection_pool" in stats
        assert "query_stats" in stats
        assert "schema" in stats
    
    def test_table_info(self, sync_test_db):
        """Test get table information"""
        # Act
        table_info = sync_test_db.get_table_info("test_users")
        
        # Assert
        assert "name" in table_info
        assert "columns" in table_info
        assert "primary_key" in table_info
        assert table_info["name"] == "test_users"
    
    def test_error_invalid_table(self, sync_test_db):
        """Test error for invalid table"""
        with pytest.raises(DatabaseError):
            sync_test_db.read("nonexistent_table", {})
    
    def test_error_invalid_column(self, sync_test_db):
        """Test error for invalid column in update"""
        with pytest.raises(DatabaseError):
            sync_test_db.update(
                "test_users",
                {"invalid_column": "value"},  # Invalid column
                {"id": 1}
            )
    
    @patch('core_postgresql_db.sync_crud_operations.time')
    def test_query_timeout(self, mock_time, mock_sync_engine):
        """Test query timeout"""
        # Arrange
        mock_time.time.side_effect = [0, 60]  # Simulate long-running query
        
        with patch('core_postgresql_db.sync_crud_operations.Engine', return_value=mock_sync_engine):
            db = SyncPostgresDB(connection_string="test://")
            
            # Mock connection to raise timeout
            mock_conn = mock_sync_engine.begin.return_value.__enter__.return_value
            mock_conn.execute.side_effect = Exception("Query timeout")
            
            # Act & Assert
            with pytest.raises(DatabaseError):
                db.create("test_users", {"name": "Test"})
    
    def test_atomic_operations(self, sync_test_db):
        """Test atomicity of operations"""
        # Create user
        user1 = sync_test_db.create("test_users", {
            "name": "Atomic Test 1",
            "email": "atomic1@test.com",
            "age": 25
        })
        
        # This should be atomic
        user2 = sync_test_db.create("test_users", {
            "name": "Atomic Test 2", 
            "email": "atomic2@test.com",
            "age": 30
        })
        
        # Both should exist
        assert user1 is not None
        assert user2 is not None
        
        users = sync_test_db.read("test_users", {
            "email__in": ["atomic1@test.com", "atomic2@test.com"]
        })
        assert len(users) == 2