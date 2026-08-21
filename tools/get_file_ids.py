import logging
from langchain_core.tools import tool

from tools.business_info import get_file_ids as _get_file_ids

logger = logging.getLogger(__name__)


@tool
def get_file_ids(business_name: str) -> dict:
    """
    Get the file IDs for a given business from Google Sheets (direct) with n8n fallback.

    Args:
        business_name (str): The official name of the business to search for.

    Returns:
        A dictionary containing the file IDs for the business's documents.
    """
    logger.info("Requesting file IDs for %r", business_name)
    return _get_file_ids(business_name)
