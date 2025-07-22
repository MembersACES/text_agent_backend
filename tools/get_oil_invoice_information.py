import requests

def get_oil_invoice_information(
    account_name: str = None
) -> dict:
    """
    Get the latest oil invoice information as JSON.
    Args:
        account_name (str, required): The Oil account name
    Returns:
        dict: The parsed JSON response from the n8n API, or an error dict.
    """
    if not account_name:
        return {"error": "Please provide an oil account name."}

    payload = {"account_name": account_name, "business_name": ""}

    response = requests.post(
        "https://membersaces.app.n8n.cloud/webhook/search-oil-info", json=payload
    )

    if response.status_code == 404:
        return {"error": "Sorry but couldn't find oil invoice information for that account name."}

    try:
        return response.json()
    except Exception as e:
        return {"error": f"Failed to parse JSON response: {str(e)}"} 