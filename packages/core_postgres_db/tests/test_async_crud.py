"""
Tests for asynchronous CRUD operations
"""
import pytest
import asyncio
from typing import Dict, Any

from core_postgresql_db import AsyncPostgresDB
from core_postgresql_db.exceptions import (
    DatabaseError, DuplicateEntryError, ConstraintViolationError
)
from core_postgresql_db.types import QueryOptions, BulkInsertOptions

pytestmark = pytest.mark.asyncio

class TestAsyncCRUDOperations:
    """Test asynchronous CRUD operations"""
    
    async def test_acreate_success(self, async_test_db, sample_user_data):
        """Test successful async create"""
        # Act
        result = await async_test_db.acreate("test_users", sample_user_data)
        
        # Assert
        assert result is not None
        assert result["name"] == sample_user_data["name"]
        assert result["email"] == sample_user_data["email"]
        assert "id" in result
    
    async def test_acreate_duplicate_email(self, async_test_db, sample_user_data):
        """Test async create with duplicate email"""
        # Arrange
        await async_test_db.acreate("test_users", sample_user_data)
        
        # Act & Assert
        with pytest.raises(DuplicateEntryError):
            await async_test_db.acreate("test_users", sample_user_data)
    
    async def test_aread_all(self, async_test_db, sample_bulk_users):
        """Test async read all records"""
        # Arrange
        for user in sample_bulk_users:
            await async_test_db.acreate("test_users", user)
        
        # Act
        users = await async_test_db.aread("test_users")
        
        # Assert
        assert len(users) >= len(sample_bulk_users)
    
    async def test_aread_with_conditions(self, async_test_db, sample_bulk_users):
        """Test async read with conditions"""
        # Arrange
        for user in sample_bulk_users:
            await async_test_db.acreate("test_users", user)
        
        # Act
        users = await async_test_db.aread("test_users", {"active": True})
        
        # Assert
        assert all(user["active"] is True for user in users)
    
    async def test_aread_with_options(self, async_test_db, sample_bulk_users):
        """Test async read with query options"""
        # Arrange
        for user in sample_bulk_users:
            await async_test_db.acreate("test_users", user)
        
        options = QueryOptions(
            limit=2,
            offset=1,
            order_by=[("name", True)]
        )
        
        # Act
        users = await async_test_db.aread("test_users", options=options)
        
        # Assert
        assert len(users) <= 2
    
    async def test_aread_one(self, async_test_db, sample_user_data):
        """Test async read single record"""
        # Arrange
        created = await async_test_db.acreate("test_users", sample_user_data)
        
        # Act
        user = await async_test_db.aread_one("test_users", {"id": created["id"]})
        
        # Assert
        assert user is not None
        assert user["id"] == created["id"]
    
    async def test_aread_by_id(self, async_test_db, sample_user_data):
        """Test async read by ID"""
        # Arrange
        created = await async_test_db.acreate("test_users", sample_user_data)
        
        # Act
        user = await async_test_db.aread_by_id("test_users", created["id"])
        
        # Assert
        assert user is not None
        assert user["id"] == created["id"]
    
    async def test_aread_nonexistent(self, async_test_db):
        """Test async read non-existent"""
        # Act
        user = await async_test_db.aread_one("test_users", {"id": 99999})
        
        # Assert
        assert user is None
    
    async def test_aexists_true(self, async_test_db, sample_user_data):
        """Test async exists returns True"""
        # Arrange
        created = await async_test_db.acreate("test_users", sample_user_data)
        
        # Act
        exists = await async_test_db.aexists("test_users", {"id": created["id"]})
        
        # Assert
        assert exists is True
    
    async def test_aexists_false(self, async_test_db):
        """Test async exists returns False"""
        # Act
        exists = await async_test_db.aexists("test_users", {"id": 99999})
        
        # Assert
        assert exists is False
    
    async def test_acount_all(self, async_test_db, sample_bulk_users):
        """Test async count all"""
        # Arrange
        for user in sample_bulk_users:
            await async_test_db.acreate("test_users", user)
        
        # Act
        count = await async_test_db.acount("test_users")
        
        # Assert
        assert count >= len(sample_bulk_users)
    
    async def test_acount_with_conditions(self, async_test_db, sample_bulk_users):
        """Test async count with conditions"""
        # Arrange
        for user in sample_bulk_users:
            await async_test_db.acreate("test_users", user)
        
        # Act
        count = await async_test_db.acount("test_users", {"active": True})
        
        # Assert
        assert count >= 0
    
    async def test_aupdate_success(self, async_test_db, sample_user_data):
        """Test successful async update"""
        # Arrange
        created = await async_test_db.acreate("test_users", sample_user_data)
        update_data = {"age": 35, "active": False}
        
        # Act
        updated = await async_test_db.aupdate(
            "test_users",
            update_data,
            {"id": created["id"]}
        )
        
        # Assert
        assert len(updated) == 1
        assert updated[0]["age"] == 35
    
    async def test_aupdate_multiple(self, async_test_db, sample_bulk_users):
        """Test async update multiple"""
        # Arrange
        for user in sample_bulk_users:
            await async_test_db.acreate("test_users", user)
        
        # Act
        updated = await async_test_db.aupdate(
            "test_users",
            {"active": False},
            {"age__gt": 22}
        )
        
        # Assert
        assert len(updated) >= 0
    
    async def test_adelete_success(self, async_test_db, sample_user_data):
        """Test successful async delete"""
        # Arrange
        created = await async_test_db.acreate("test_users", sample_user_data)
        
        # Act
        deleted_count = await async_test_db.adelete(
            "test_users", 
            {"id": created["id"]}
        )
        
        # Assert
        assert deleted_count == 1
        
        # Verify deletion
        user = await async_test_db.aread_one("test_users", {"id": created["id"]})
        assert user is None
    
    async def test_adelete_with_returning(self, async_test_db, sample_user_data):
        """Test async delete with returning"""
        # Arrange
        created = await async_test_db.acreate("test_users", sample_user_data)
        
        # Act
        deleted_rows = await async_test_db.adelete(
            "test_users",
            {"id": created["id"]},
            returning=True
        )
        
        # Assert
        assert len(deleted_rows) == 1
        assert deleted_rows[0]["id"] == created["id"]
    
    async def test_abulk_create_success(self, async_test_db, sample_bulk_users):
        """Test successful async bulk create"""
        # Act
        results = await async_test_db.abulk_create("test_users", sample_bulk_users)
        
        # Assert
        assert len(results) == len(sample_bulk_users)
        assert all("id" in user for user in results)
    
    async def test_abulk_create_with_options(self, async_test_db, sample_bulk_users):
        """Test async bulk create with options"""
        options = BulkInsertOptions(batch_size=2, return_rows=True)
        
        # Act
        results = await async_test_db.abulk_create(
            "test_users", 
            sample_bulk_users, 
            options
        )
        
        # Assert
        assert len(results) == len(sample_bulk_users)
    
    async def test_aexecute_raw_sql_select(self, async_test_db, sample_user_data):
        """Test async raw SQL SELECT"""
        # Arrange
        await async_test_db.acreate("test_users", sample_user_data)
        
        # Act
        results = await async_test_db.aexecute_raw_sql(
            "SELECT * FROM test_users WHERE email = :email",
            {"email": sample_user_data["email"]}
        )
        
        # Assert
        assert len(results) >= 1
        assert results[0]["email"] == sample_user_data["email"]
    
    async def test_aexecute_raw_sql_insert(self, async_test_db):
        """Test async raw SQL INSERT"""
        # Act
        rowcount = await async_test_db.aexecute_raw_sql(
            "INSERT INTO test_users (name, email, age) VALUES (:name, :email, :age)",
            {"name": "Async Raw", "email": "async_raw@example.com", "age": 25},
            fetch_results=False
        )
        
        # Assert
        assert rowcount == 1
        
        # Verify insert
        user = await async_test_db.aread_one(
            "test_users", 
            {"email": "async_raw@example.com"}
        )
        assert user is not None
    
    async def test_atransaction_success(self, async_test_db, sample_user_data):
        """Test successful async transaction"""
        async with async_test_db.atransaction():
            # Create user
            user = await async_test_db.acreate("test_users", sample_user_data)
            
            # Update user
            await async_test_db.aupdate(
                "test_users",
                {"age": 40},
                {"id": user["id"]}
            )
        
        # Verify transaction committed
        updated = await async_test_db.aread_one("test_users", {"id": user["id"]})
        assert updated["age"] == 40
    
    async def test_atransaction_rollback(self, async_test_db, sample_user_data):
        """Test async transaction rollback"""
        user_id = None
        
        try:
            async with async_test_db.atransaction():
                # Create user
                user = await async_test_db.acreate("test_users", sample_user_data)
                user_id = user["id"]
                
                # This should fail
                await async_test_db.aupdate(
                    "test_users",
                    {"invalid_column": "value"},  # Invalid column
                    {"id": user_id}
                )
        except DatabaseError:
            pass  # Expected
        
        # Verify rollback
        if user_id:
            user = await async_test_db.aread_one("test_users", {"id": user_id})
            assert user is None  # Should have been rolled back
    
    async def test_ahealth_check(self, async_test_db):
        """Test async health check"""
        # Act
        healthy = await async_test_db.ahealth_check()
        
        # Assert
        assert healthy is True
    
    async def test_aget_stats(self, async_test_db):
        """Test async get stats"""
        # Act
        stats = await async_test_db.aget_stats()
        
        # Assert
        assert "connection_pool" in stats
        assert "query_stats" in stats
    
    async def test_error_invalid_table_async(self, async_test_db):
        """Test async error for invalid table"""
        with pytest.raises(DatabaseError):
            await async_test_db.aread("nonexistent_table", {})
    
    async def test_concurrent_operations(self, async_test_db, sample_bulk_users):
        """Test concurrent async operations"""
        # Create tasks for concurrent operations
        tasks = []
        for user in sample_bulk_users:
            task = async_test_db.acreate("test_users", user)
            tasks.append(task)
        
        # Execute concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # All should succeed
        assert all(not isinstance(r, Exception) for r in results)
        assert len([r for r in results if r is not None]) == len(sample_bulk_users)
    
    async def test_atomic_async_operations(self, async_test_db):
        """Test atomicity of async operations"""
        # Create two users concurrently
        task1 = async_test_db.acreate("test_users", {
            "name": "Async Atomic 1",
            "email": "async_atomic1@test.com",
            "age": 25
        })
        
        task2 = async_test_db.acreate("test_users", {
            "name": "Async Atomic 2",
            "email": "async_atomic2@test.com",
            "age": 30
        })
        
        user1, user2 = await asyncio.gather(task1, task2)
        
        # Both should exist
        assert user1 is not None
        assert user2 is not None
        
        users = await async_test_db.aread("test_users", {
            "email__in": ["async_atomic1@test.com", "async_atomic2@test.com"]
        })
        assert len(users) == 2  