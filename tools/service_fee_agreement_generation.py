from langchain_core.tools import tool
import requests
import datetime


@tool
def service_fee_agreement_generation(
    business_name: str,
    trading_as: str,
    abn: str,
    postal_address: str,
    site_address: str,
    telephone: str,
    contact_name: str,
    position: str,
    client_folder_url: str,
    email: str,
) -> int:
    """Generates a Service Agreement for a specific business

    Args:
        business_name: Name of the business
        trading_as: Trading name of the business
        abn: Australian Business Number
        postal_address: Postal address of the business
        site_address: Physical site address of the business
        telephone: Contact telephone number
        email: Contact email address
        contact_name: Name of the primary contact
        position: Position/role of the contact person
        client_folder_url: URL of the client's Google Drive folder
    """

    current_date = datetime.datetime.now()
    current_month = current_date.strftime("%b")
    current_year = current_date.year

    payload = {
        "data": {
            "business_name": business_name,
            "trading_as": trading_as,
            "abn": abn,
            "postal_address": postal_address,
            "site_address": site_address,
            "telephone": telephone,
            "email": email,
            "contact_name": contact_name,
            "position": position,
            "client_folder_url": client_folder_url,
            "current_month": current_month,
            "current_year": current_year,
        },
        "template_id": "1LpJfOVV6z9QNmnkwgBt2DVzizolj89cHRckcB1H4lK0",
        "file_name": f"Service Agreement for {business_name}",
    }
    response = requests.post(
        "https://membersaces.app.n8n.cloud/webhook/document-generation-2", json=payload
    )

    if response.status_code == 404:
        return "Sorry but couldn't find that business name"

    data = response.json()
    document_link = data.get("document_link")

    return f'The Service Agreement (SFA) for "{business_name}" has been successfully generated. You can access it here: {document_link} and this is the link of the Google Drive Folder: {client_folder_url}. If you need further assistance or details, please let me know!'
