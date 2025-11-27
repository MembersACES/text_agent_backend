"""
Database configuration with Cloud SQL (PostgreSQL) support
Automatically switches between SQLite (local dev) and PostgreSQL (production)
"""
import os
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from dotenv import load_dotenv

load_dotenv()

# Determine database type from environment
DB_TYPE = os.getenv("DB_TYPE", "sqlite")
DATABASE_URL = os.getenv("DATABASE_URL")

if DB_TYPE == "postgresql":
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL must be set when DB_TYPE=postgresql")
    
    logging.info("🐘 Using PostgreSQL (Cloud SQL)")
    
    # Cloud SQL Configuration
    engine = create_engine(
        DATABASE_URL,
        pool_pre_ping=True,  # Verify connections before using
        pool_recycle=3600,   # Recycle connections after 1 hour
        echo=False,
    )
else:
    # Local SQLite for development
    logging.info("📁 Using SQLite (local development)")
    DB_NAME = os.getenv("SQLITE_DB_NAME", "tasks.db")
    DATABASE_URL = f"sqlite:///./{DB_NAME}"
    
    engine = create_engine(
        DATABASE_URL,
        connect_args={"check_same_thread": False}
    )

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def init_db():
    Base.metadata.create_all(bind=engine)
    logging.info("✅ Database tables initialized")
