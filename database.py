"""
Database configuration with Cloud SQL (PostgreSQL) support
Automatically switches between SQLite (local dev) and PostgreSQL (production)
"""
import os
import logging
from sqlalchemy import create_engine, inspect, text
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
    """
    Initialise database tables and apply simple, safe migrations needed for new fields.

    For now this includes:
    - Adding offers.pipeline_stage if it does not exist (used for the detailed offer pipeline).
    """
    Base.metadata.create_all(bind=engine)
    logging.info("✅ Database tables initialized")

    # Lightweight migration: ensure offers.pipeline_stage exists.
    try:
        inspector = inspect(engine)
        columns = [col["name"] for col in inspector.get_columns("offers")]
        if "pipeline_stage" not in columns:
            logging.info("Adding missing offers.pipeline_stage column")
            with engine.begin() as conn:
                conn.execute(
                    text("ALTER TABLE offers ADD COLUMN pipeline_stage VARCHAR(50)")
                )
            logging.info("✅ Added offers.pipeline_stage column")
    except Exception as e:
        # Don't block startup if this fails; log and continue.
        logging.warning("Could not ensure offers.pipeline_stage column: %s", e)

    # Offers: optional Base 2 / comparison fields
    # (annual_savings, current_cost, new_cost, and comparison offer metrics)
    try:
        insp = inspect(engine)
        if "offers" in (insp.get_table_names() or []):
            cols = [c["name"] for c in insp.get_columns("offers")]
            for col_name, col_type in [
                ("annual_savings", "REAL"),
                ("current_cost", "REAL"),
                ("new_cost", "REAL"),
                ("annual_usage_gj", "REAL"),
                ("energy_charge_pct", "REAL"),
                ("contracted_rate", "REAL"),
                ("offer_rate", "REAL"),
            ]:
                if col_name not in cols:
                    logging.info("Adding missing offers.%s column", col_name)
                    with engine.begin() as conn:
                        conn.execute(text(f"ALTER TABLE offers ADD COLUMN {col_name} {col_type}"))
                    logging.info("✅ Added offers.%s column", col_name)
    except Exception as e:
        logging.warning("Could not ensure offers savings columns: %s", e)

    # Strategy & WIP: ensure strategy_items has offer_id and activity_type if table exists.
    try:
        insp = inspect(engine)
        if "strategy_items" in (insp.get_table_names() or []):
            cols = [c["name"] for c in insp.get_columns("strategy_items")]
            if "offer_id" not in cols:
                logging.info("Adding missing strategy_items.offer_id column")
                with engine.begin() as conn:
                    conn.execute(text("ALTER TABLE strategy_items ADD COLUMN offer_id INTEGER"))
                logging.info("✅ Added strategy_items.offer_id column")
            if "activity_type" not in cols:
                logging.info("Adding missing strategy_items.activity_type column")
                with engine.begin() as conn:
                    conn.execute(text("ALTER TABLE strategy_items ADD COLUMN activity_type VARCHAR(50)"))
                logging.info("✅ Added strategy_items.activity_type column")
            if "offer_activity_id" not in cols:
                logging.info("Adding missing strategy_items.offer_activity_id column")
                with engine.begin() as conn:
                    conn.execute(text("ALTER TABLE strategy_items ADD COLUMN offer_activity_id INTEGER"))
                logging.info("✅ Added strategy_items.offer_activity_id column")
            if "excluded_from_wip" not in cols:
                logging.info("Adding missing strategy_items.excluded_from_wip column")
                with engine.begin() as conn:
                    conn.execute(text("ALTER TABLE strategy_items ADD COLUMN excluded_from_wip INTEGER DEFAULT 0"))
                logging.info("✅ Added strategy_items.excluded_from_wip column")
    except Exception as e:
        logging.warning("Could not ensure strategy_items columns: %s", e)

    # Testimonials: ensure testimonial_type, testimonial_solution_type_id, testimonial_savings exist (added after initial table).
    try:
        insp = inspect(engine)
        if "testimonials" in (insp.get_table_names() or []):
            cols = [c["name"] for c in insp.get_columns("testimonials")]
            for col_name, col_type in [
                ("testimonial_type", "VARCHAR(255)"),
                ("testimonial_solution_type_id", "VARCHAR(100)"),
                ("testimonial_savings", "VARCHAR(255)"),
            ]:
                if col_name not in cols:
                    logging.info("Adding missing testimonials.%s column", col_name)
                    with engine.begin() as conn:
                        conn.execute(text(f"ALTER TABLE testimonials ADD COLUMN {col_name} {col_type}"))
                    logging.info("✅ Added testimonials.%s column", col_name)
    except Exception as e:
        logging.warning("Could not ensure testimonials columns: %s", e)

    # Clients: advocate / referral fields (referred by another member or business name + active flag)
    try:
        insp = inspect(engine)
        if "clients" in (insp.get_table_names() or []):
            cols = [c["name"] for c in insp.get_columns("clients")]
            if "referred_by_client_id" not in cols:
                logging.info("Adding missing clients.referred_by_client_id column")
                with engine.begin() as conn:
                    conn.execute(text("ALTER TABLE clients ADD COLUMN referred_by_client_id INTEGER"))
                logging.info("✅ Added clients.referred_by_client_id column")
            if "referred_by_business_name" not in cols:
                logging.info("Adding missing clients.referred_by_business_name column")
                with engine.begin() as conn:
                    conn.execute(text("ALTER TABLE clients ADD COLUMN referred_by_business_name VARCHAR(255)"))
                logging.info("✅ Added clients.referred_by_business_name column")
            if "referred_by_active" not in cols:
                logging.info("Adding missing clients.referred_by_active column")
                with engine.begin() as conn:
                    conn.execute(text("ALTER TABLE clients ADD COLUMN referred_by_active INTEGER NOT NULL DEFAULT 1"))
                logging.info("✅ Added clients.referred_by_active column")
    except Exception as e:
        logging.warning("Could not ensure clients advocate columns: %s", e)

    # Clients: advocacy meeting details (stored on dashboard)
    try:
        insp = inspect(engine)
        if "clients" in (insp.get_table_names() or []):
            cols = [c["name"] for c in insp.get_columns("clients")]
            if "advocacy_meeting_date" not in cols:
                logging.info("Adding missing clients.advocacy_meeting_date column")
                with engine.begin() as conn:
                    conn.execute(text("ALTER TABLE clients ADD COLUMN advocacy_meeting_date DATE"))
                logging.info("✅ Added clients.advocacy_meeting_date column")
            if "advocacy_meeting_time" not in cols:
                logging.info("Adding missing clients.advocacy_meeting_time column")
                with engine.begin() as conn:
                    conn.execute(text("ALTER TABLE clients ADD COLUMN advocacy_meeting_time VARCHAR(20)"))
                logging.info("✅ Added clients.advocacy_meeting_time column")
            if "advocacy_meeting_completed" not in cols:
                logging.info("Adding missing clients.advocacy_meeting_completed column")
                with engine.begin() as conn:
                    conn.execute(text("ALTER TABLE clients ADD COLUMN advocacy_meeting_completed INTEGER NOT NULL DEFAULT 0"))
                logging.info("✅ Added clients.advocacy_meeting_completed column")
    except Exception as e:
        logging.warning("Could not ensure clients advocacy meeting columns: %s", e)

    # Pudu consumables: lifecycle tracking fields for replacement planning.
    try:
        insp = inspect(engine)
        if "pudu_consumables" in (insp.get_table_names() or []):
            cols = [c["name"] for c in insp.get_columns("pudu_consumables")]
            if "last_replaced_at" not in cols:
                logging.info("Adding missing pudu_consumables.last_replaced_at column")
                with engine.begin() as conn:
                    conn.execute(text("ALTER TABLE pudu_consumables ADD COLUMN last_replaced_at DATE"))
                logging.info("✅ Added pudu_consumables.last_replaced_at column")
            if "replacement_interval_days" not in cols:
                logging.info("Adding missing pudu_consumables.replacement_interval_days column")
                with engine.begin() as conn:
                    conn.execute(text("ALTER TABLE pudu_consumables ADD COLUMN replacement_interval_days INTEGER"))
                logging.info("✅ Added pudu_consumables.replacement_interval_days column")
    except Exception as e:
        logging.warning("Could not ensure pudu_consumables lifecycle columns: %s", e)
