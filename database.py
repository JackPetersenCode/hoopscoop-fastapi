from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Get the database URL from the environment variable
DATABASE_URL = os.getenv("DATABASE_URL")

# Create an async engine for async operations with asyncpg
engine = create_async_engine(DATABASE_URL, echo=True)

# Async session for async operations
async_session = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
