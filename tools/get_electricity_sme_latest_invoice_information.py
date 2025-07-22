import requests

def get_electricity_sme_latest_invoice_information(
    business_name: str = None, nmi: str = None
) -> dict:
    """
    Get the latest electricity SME invoice information as JSON.
    Args:
        business_name (str, optional): The name of the business to search for
        nmi (str, optional): The NMI of the business to search for
    Returns:
        dict: The parsed JSON response from the n8n API, or an error dict.
    """
    if not business_name and not nmi:
        return {"error": "Please provide either a business name or nmi"}

    payload = {}
    if business_name:
        payload["nmi"] = ""
        payload["business_name"] = business_name
    if nmi:
        payload["business_name"] = ""
        payload["nmi"] = nmi

    response = requests.post(
        "https://membersaces.app.n8n.cloud/webhook/search-electricity-sme-info", json=payload
    )

    if response.status_code == 404:
        return {"error": "Sorry but couldn't find electricity SME invoice information for that business and NMI"}

    try:
        data = response.json()
        # If the key is wrong, relabel it
        if "electricity_ci_invoice_details" in data:
            data["electricity_sme_invoice_details"] = data.pop("electricity_ci_invoice_details")
        return data
    except Exception as e:
        return {"error": f"Failed to parse JSON response: {str(e)}"} 