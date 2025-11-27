from google.cloud import storage
import os
import logging

DISABLE_DB_SYNC = os.getenv("DISABLE_DB_SYNC", "false").lower() == "true"
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
DB_NAME = os.getenv("SQLITE_DB_NAME", "tasks.db")


def get_storage_client():
    return storage.Client()


def download_sqlite_db():
    """Download tasks.db from GCS → local filesystem"""

    if DISABLE_DB_SYNC:
        logging.info("DB sync disabled — skipping download")
        return False

    if not BUCKET_NAME:
        logging.error("GCS_BUCKET_NAME missing — cannot download DB")
        return False

    local_path = f"./{DB_NAME}"

    try:
        client = get_storage_client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(DB_NAME)

        if not blob.exists():
            logging.warning("SQLite DB not found in bucket — skipping download")
            return False

        blob.download_to_filename(local_path)
        logging.info("Downloaded SQLite DB from GCS")
        return True
    except Exception as e:
        logging.error(f"Failed to download SQLite DB: {e}")
        return False


def upload_sqlite_db():
    """Upload local tasks.db → GCS"""

    if DISABLE_DB_SYNC:
        logging.info("DB sync disabled — skipping upload")
        return False

    if not BUCKET_NAME:
        logging.error("GCS_BUCKET_NAME missing — cannot upload DB")
        return False

    local_path = f"./{DB_NAME}"

    try:
        client = get_storage_client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(DB_NAME)
        blob.upload_from_filename(local_path)
        logging.info("Uploaded SQLite DB to GCS")
    except Exception as e:
        logging.error(f"Failed to upload SQLite DB: {e}")
