"""
Transaction management for atomic operations
"""
import logging
from contextlib import contextmanager, asynccontextmanager
from typing import Optional

from sqlalchemy.engine import Engine, Connection
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncConnection

logger = logging.getLogger(__name__)

class TransactionManager:
    """Synchronous transaction manager"""
    
    def __init__(self, engine: Engine):
        self.engine = engine
    
    @contextmanager
    def begin(self, isolation_level: str = "READ COMMITTED"):
        """
        Begin a transaction with specified isolation level
        
        Args:
            isolation_level: SQL isolation level
        """
        with self.engine.begin() as conn:
            try:
                # Set isolation level
                if isolation_level:
                    conn.execute(f"SET TRANSACTION ISOLATION LEVEL {isolation_level}")
                
                logger.debug(f"Transaction started with isolation: {isolation_level}")
                yield conn
                
            except Exception as e:
                logger.error(f"Transaction rollback: {e}")
                conn.rollback()
                raise
            else:
                conn.commit()
                logger.debug("Transaction committed successfully")

class AsyncTransactionManager:
    """Asynchronous transaction manager"""
    
    def __init__(self, engine: AsyncEngine):
        self.engine = engine
    
    @asynccontextmanager
    async def begin(self, isolation_level: str = "READ COMMITTED"):
        """
        Begin an async transaction with specified isolation level
        
        Args:
            isolation_level: SQL isolation level
        """
        async with self.engine.begin() as conn:
            try:
                # Set isolation level
                if isolation_level:
                    await conn.execute(f"SET TRANSACTION ISOLATION LEVEL {isolation_level}")
                
                logger.debug(f"Async transaction started with isolation: {isolation_level}")
                yield conn
                
            except Exception as e:
                logger.error(f"Async transaction rollback: {e}")
                await conn.rollback()
                raise
            else:
                await conn.commit()
                logger.debug("Async transaction committed successfully")