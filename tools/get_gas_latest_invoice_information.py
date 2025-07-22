import requests

def get_gas_latest_invoice_information(
    business_name: str = None, mrin: str = None
) -> dict:
    """
    Get the latest gas invoice information as JSON.
    Args:
        business_name (str, optional): The name of the business to search for
        mrin (str, optional): The MRIN (Meter Registration Identification Number)
    Returns:
        dict: The parsed JSON response from the n8n API, or an error dict.
    """
    if not business_name and not mrin:
        return {"error": "Please provide either a business name or MRIN"}

    payload = {}
    if business_name:
        payload["mrin"] = ""
        payload["business_name"] = business_name
    if mrin:
        payload["business_name"] = ""
        payload["mrin"] = mrin

    response = requests.post(
        "https://membersaces.app.n8n.cloud/webhook/search-gas-info", json=payload
    )

    if response.status_code == 404:
        return {"error": "Sorry but couldn't find gas invoice information for that business and MRIN"}

    try:
        return response.json()
    except Exception as e:
        return {"error": f"Failed to parse JSON response: {str(e)}"}
