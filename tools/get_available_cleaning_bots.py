from langchain_core.tools import tool
import requests

@tool
def get_available_cleaning_bots() -> list:
    """Fetch the list of available cleaning bots and their associated businesses from the n8n webhook. Returns a list of dicts with robot and business details."""
    url = "https://membersaces.app.n8n.cloud/webhook/pudu_weekly_map"
    response = requests.get(url)
    response.raise_for_status()
    bots = response.json()
    return bots 