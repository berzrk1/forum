import logging
import ssl

from datetime import datetime
from typing import Annotated, AsyncGenerator

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import Mapped, declarative_base, mapped_column
from sqlalchemy.sql import func
from sqlalchemy.types import DateTime

from forum.config import settings

Base = declarative_base()

log = logging.getLogger(__name__)

_engine = None
_sessionlocal = None


def get_engine():
    """Create engine only when explicitly called, and reuse it in subsequent calls."""
    global _engine

    if _engine is None:
        # First time the engine gets called
        if settings.is_development:
            _engine = create_async_engine(str(settings.DATABASE_URI))
        else:
            ssl_ctx = ssl.create_default_context(
                ssl.Purpose.CLIENT_AUTH, cafile="/certs/global-bundle.pem"
            )
            _engine = create_async_engine(
                str(settings.DATABASE_URI), connect_args={"ssl": ssl_ctx}
            )
    return _engine


def get_sessionlocal():
    """Create session only when explicitly called, and reuse it in subsequent calls."""
    global _sessionlocal

    if _sessionlocal is None:
        _sessionlocal = async_sessionmaker(
            get_engine(), class_=AsyncSession, expire_on_commit=False
        )
    return _sessionlocal


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Get database session with auto-commit."""
    session = get_sessionlocal()()
    try:
        yield session
        await session.commit()
    except Exception:
        log.warning("Database rollback")
        await session.rollback()
        raise
    finally:
        await session.close()


DbSession = Annotated[AsyncSession, Depends(get_db)]


class TimestampMixin:
    """Timestamp Mixin for created_at and updated_at."""

    created_at: Mapped[datetime] = mapped_column(
        DateTime(True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(True), server_default=func.now(), onupdate=func.now()
    )
