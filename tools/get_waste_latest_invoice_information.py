import requests

def get_waste_latest_invoice_information(
    business_name: str = None, customer_number: str = None
) -> dict:
    """
    Get the latest waste invoice information as JSON.
    Args:
        business_name (str, optional): The name of the business to search for
        customer_number (str, optional): The Customer number or account number
    Returns:
        dict: The parsed JSON response from the n8n API, or an error dict.
    """
    if not business_name and not customer_number:
        return {"error": "Please provide either a business name or customer_number"}

    payload = {}
    if business_name:
        payload["customer_number"] = ""
        payload["business_name"] = business_name
    if customer_number:
        payload["business_name"] = ""
        payload["customer_number"] = customer_number

    response = requests.post(
        "https://membersaces.app.n8n.cloud/webhook/search-waste-info", json=payload
    )

    if response.status_code == 404:
        return {"error": "Sorry but couldn't find waste invoice information for that business name and customer number"}

    try:
        return response.json()
    except Exception as e:
        return {"error": f"Failed to parse JSON response: {str(e)}"}
