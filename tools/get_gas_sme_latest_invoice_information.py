import requests

def get_gas_sme_latest_invoice_information(
    business_name: str = None, mrin: str = None
) -> dict:
    """
    Get the latest gas SME invoice information as JSON.
    Args:
        business_name (str, optional): The name of the business to search for
        mrin (str, optional): The MRIN of the business to search for
    Returns:
        dict: The parsed JSON response from the n8n API, or an error dict.
    """
    if not business_name and not mrin:
        return {"error": "Please provide either a business name or mrin"}

    payload = {}
    if business_name:
        payload["mrin"] = ""
        payload["business_name"] = business_name
    if mrin:
        payload["business_name"] = ""
        payload["mrin"] = mrin

    response = requests.post(
        "https://membersaces.app.n8n.cloud/webhook/search-gas-sme-info", json=payload
    )

    if response.status_code == 404:
        return {"error": "Sorry but couldn't find gas SME invoice information for that business and MRIN"}

    try:
        return response.json()
    except Exception as e:
        return {"error": f"Failed to parse JSON response: {str(e)}"} 