"""
Session management for database operations
"""
import logging
from typing import Optional
from contextlib import contextmanager, asynccontextmanager

from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

logger = logging.getLogger(__name__)

class SessionManager:
    """Synchronous session manager"""
    
    def __init__(self, engine):
        self.engine = engine
        self.SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=engine
        )
    
    @contextmanager
    def get_session(self):
        """Get database session with auto cleanup"""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Session rollback: {e}")
            raise
        finally:
            session.close()
    
    def get_session_obj(self) -> Session:
        """Get session object (caller must close)"""
        return self.SessionLocal()

class AsyncSessionManager:
    """Asynchronous session manager"""
    
    def __init__(self, engine):
        self.engine = engine
        self.AsyncSessionLocal = async_sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=engine,
            class_=AsyncSession
        )
    
    @asynccontextmanager
    async def get_session(self):
        """Get async database session with auto cleanup"""
        session = self.AsyncSessionLocal()
        try:
            yield session
            await session.commit()
        except Exception as e:
            await session.rollback()
            logger.error(f"Async session rollback: {e}")
            raise
        finally:
            await session.close()
    
    def get_session_obj(self) -> AsyncSession:
        """Get async session object (caller must close)"""
        return self.AsyncSessionLocal()