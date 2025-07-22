import requests

def get_electricity_ci_latest_invoice_information(
    business_name: str = None, nmi: str = None
) -> dict:
    """
    Get the latest electricity C&I invoice information as JSON.
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
        "https://membersaces.app.n8n.cloud/webhook/search-electricity-ci-info", json=payload
    )

    if response.status_code == 404:
        return {"error": "Sorry but couldn't find electricity C&I invoice information for that business and NMI"}

    try:
        return response.json()
    except Exception as e:
        return {"error": f"Failed to parse JSON response: {str(e)}"}
