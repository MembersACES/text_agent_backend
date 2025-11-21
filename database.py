"""
Database configuration with SQLite + optional GCS persistence
"""

import os
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from dotenv import load_dotenv

# Load .env FIRST — BEFORE importing storage
load_dotenv()

DB_NAME = os.getenv("SQLITE_DB_NAME", "tasks.db")
DISABLE_DB_SYNC = os.getenv("DISABLE_DB_SYNC", "false").lower() == "true"

# Lazy import placeholder (we load storage only if needed)
download_sqlite_db = None
upload_sqlite_db = None

# Only import storage if NOT disabled
if not DISABLE_DB_SYNC:
    try:
        from utils.storage import download_sqlite_db, upload_sqlite_db
        download_sqlite_db()
    except Exception as e:
        logging.error(f"Failed to initialize DB sync: {e}")

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
        # Only upload if enabled
        if upload_sqlite_db and not DISABLE_DB_SYNC:
            try:
                upload_sqlite_db()
            except Exception as e:
                logging.error(f"Error uploading DB: {e}")


def init_db():
    Base.metadata.create_all(bind=engine)

    if upload_sqlite_db and not DISABLE_DB_SYNC:
        try:
            upload_sqlite_db()
        except Exception as e:
            logging.error(f"Error uploading DB during init: {e}")
