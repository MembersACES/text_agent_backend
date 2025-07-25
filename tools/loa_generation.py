from langchain_core.tools import tool
import requests
import datetime


@tool
def loa_generation(
    business_name: str,
    abn: str,
    trading_as: str,
    postal_address: str,
    site_address: str,
    telephone: str,
    email: str,
    contact_name: str,
    position: str,
    client_folder_url: str,
) -> str:
    """Generate Letter of Authority (LOA) document for a business.

    Args:
        business_name: Business name
        abn: Australian Business Number
        trading_as: Trading name
        postal_address: Business postal address
        site_address: Business physical address
        telephone: Phone number
        email: Email address
        contact_name: Primary contact name
        position: Contact person's role
        client_folder_url: Client's Google Drive folder URL
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
        "template_id": "170ZpEktA9fo1H0TkaJYFh7iyreYLOwAmrfP2e0Rk_lg",
        "file_name": f"Letter of Authority for {business_name}",
    }

    response = requests.post(
        "https://membersaces.app.n8n.cloud/webhook/document-generation-2", json=payload
    )

    data = response.json()
    document_link = data.get("document_link")

    return f'The Letter of Agreement (LOA) for "{business_name}" has been successfully generated. You can access it here: {document_link} and this is the link of the Google Drive Folder: {client_folder_url}. If you need further assistance or details, please let me know!'
