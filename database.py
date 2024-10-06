from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

# PostgreSQL database URL with asyncpg
DATABASE_URL = "postgresql+asyncpg://postgres:redsox45@localhost:5432/hoop_scoop"

# Create an async engine for async operations with asyncpg
engine = create_async_engine(DATABASE_URL, echo=True)

# Async session for async operations
async_session = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
