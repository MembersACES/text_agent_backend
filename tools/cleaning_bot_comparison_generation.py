from langchain_core.tools import tool
from tools.get_business_information import get_business_information
import requests
import requests
import json

@tool
def cleaning_bot_comparison_generation(
    business_name: str,
    current_monthly_cost: str,
    quantity_bot: str,
    surface_area: str,
    quantity_of_scrubber: str,
    scrubber_surface_area: str,
) -> str:
    """Create a cleaning bot comparison for a business. This tool handles everything automatically.
    
    Args:
        business_name: Name of the business (e.g., "Darebin RSL")
        current_monthly_cost: Current monthly cleaning cost (number only, no $)
        quantity_bot: Number of bots
        surface_area: Total surface area to be cleaned (number only, no %)
        quantity_of_scrubber: Number of scrubbers
        scrubber_surface_area: Surface area covered by scrubbers (number only, no %)
    """
    # First, get the business information
    business_info = get_business_information(business_name)
    
    # Check if we got an error string or actual data
    if isinstance(business_info, str):
        return f"Error getting business information: {business_info}"
    
    # Extract the record_ID from the response
    record_id = business_info.get('record_ID')
    
    if not record_id:
        raise Exception(f"Could not find record ID for business: {business_name}")
    
    # Now generate the comparison using the correct record ID
    payload = {
        "record_id": record_id,
        "current_monthly_cost": current_monthly_cost,
        "quantity_bot": quantity_bot,
        "surface_area": surface_area,
        "quantity_of_scrubber": quantity_of_scrubber,
        "scrubber_surface_area": scrubber_surface_area,
    }

    response = requests.post(
        "https://membersaces.app.n8n.cloud/webhook/generate-cleaning-bot-comparison",
        json=payload,
    )

    if response.status_code != 200:
        raise Exception(
            f"Failed to generate cleaning bot comparison. Status code: {response.status_code}"
        )

    data = response.json()
    pdf_document_link = data.get("pdf_document_link")
    spreadsheet_document_link = data.get("spreadsheet_document_link")

    return f"The Cleaning bot comparison document has been successfully generated. You can access the PDF comparison here: {pdf_document_link} and the spreadsheet here: {spreadsheet_document_link}"