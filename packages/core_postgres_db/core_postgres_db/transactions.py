# packages/core_postgres_db/core_postgres_db/transactions.py
"""
Transaction management for atomic operations - Sonarqube compliant
"""
import logging
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncEngine
from contextlib import contextmanager

from sqlalchemy.engine import Engine

from .constants import READ_COMMITTED

logger = logging.getLogger(__name__)


class AsyncTransactionManager:
    """Async transaction manager with isolation level support"""
    
    def __init__(self, engine: AsyncEngine):
        self.engine = engine
    
    @asynccontextmanager
    async def begin(self, isolation_level: str = READ_COMMITTED):
        """
        Begin an async transaction with specified isolation level
        
        Args:
            isolation_level: SQL isolation level
        
        Yields:
            Connection object for transaction operations
        """
        async with self.engine.begin() as conn:
            try:
                # Set isolation level if specified
                if isolation_level and isolation_level != READ_COMMITTED:
                    await self._set_isolation_level(conn, isolation_level)
                
                logger.debug(f"Async transaction started with isolation: {isolation_level}")
                yield conn
                
            except Exception as e:
                logger.error(f"Async transaction rollback: {e}")
                await conn.rollback()
                raise
            else:
                await conn.commit()
                logger.debug("Async transaction committed successfully")
    
    async def _set_isolation_level(self, conn, isolation_level: str) -> None:
        """Set transaction isolation level"""
        valid_levels = ["READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"]
        
        if isolation_level.upper() not in valid_levels:
            raise ValueError(
                f"Invalid isolation level: {isolation_level}. "
                f"Valid options: {valid_levels}"
            )
        
        try:
            await conn.execute(f"SET TRANSACTION ISOLATION LEVEL {isolation_level}")
        except Exception as e:
            logger.warning(f"Failed to set isolation level: {e}")
            # Continue without isolation level setting
            
"""
Sync transaction manager (add to existing transactions.py)
"""

class TransactionManager:
    """Sync transaction manager with isolation level support"""
    
    def __init__(self, engine: Engine):
        self.engine = engine
    
    @contextmanager
    def begin(self, isolation_level: str = READ_COMMITTED):
        """
        Begin a transaction with specified isolation level
        
        Args:
            isolation_level: SQL isolation level
        
        Yields:
            Connection object for transaction operations
        """
        with self.engine.begin() as conn:
            try:
                # Set isolation level if specified
                if isolation_level and isolation_level != READ_COMMITTED:
                    self._set_isolation_level(conn, isolation_level)
                
                logger.debug(f"Transaction started with isolation: {isolation_level}")
                yield conn
                
            except Exception as e:
                logger.error(f"Transaction rollback: {e}")
                conn.rollback()
                raise
            else:
                conn.commit()
                logger.debug("Transaction committed successfully")
    
    def _set_isolation_level(self, conn, isolation_level: str) -> None:
        """Set transaction isolation level"""
        valid_levels = ["READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"]
        
        if isolation_level.upper() not in valid_levels:
            raise ValueError(
                f"Invalid isolation level: {isolation_level}. "
                f"Valid options: {valid_levels}"
            )
        
        try:
            conn.execute(f"SET TRANSACTION ISOLATION LEVEL {isolation_level}")
        except Exception as e:
            logger.warning(f"Failed to set isolation level: {e}")
            # Continue without isolation level setting