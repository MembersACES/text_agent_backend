import requests
import logging
from langchain_core.tools import tool

logger = logging.getLogger(__name__)

@tool
def get_file_ids(business_name: str) -> dict:
    """
    Get the file IDs for a given business from a Google Sheet via an n8n webhook.

    Args:
        business_name (str): The official name of the business to search for.

    Returns:
        A dictionary containing the file IDs for the business's documents.
    """
    webhook_url = "https://membersaces.app.n8n.cloud/webhook/return_fileIDs"
    payload = {"business_name": business_name}
    logger.info(f"Requesting file IDs for '{business_name}' from n8n webhook.")

    try:
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()  # Raise an exception for bad status codes
        
        if response.status_code == 200:
            file_ids_data = response.json()
            logger.info(f"Successfully retrieved file IDs: {file_ids_data}")
            return file_ids_data
        else:
            logger.warning(f"Could not find file IDs for '{business_name}'. Status: {response.status_code}")
            return {}

    except requests.exceptions.RequestException as e:
        logger.error(f"Error calling n8n webhook for file IDs: {e}", exc_info=True)
        return {}
    except ValueError: # Catches JSON decoding errors
        logger.error(f"Failed to decode JSON from file ID webhook response. Response text: {response.text}")
        return {} 